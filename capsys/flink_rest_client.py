#!/usr/bin/env python3
"""Small local Flink REST client shim for CAPSys scripts.

This implements only the subset of the third-party API that the existing
scripts use, so the profiling flow can run without an extra pip dependency.
"""

from __future__ import annotations

import requests


class JobsAPI:
    def __init__(self, client: "FlinkRestClient"):
        self._client = client

    def overview(self):
        return self._client._get("/jobs/overview").get("jobs", [])

    def all(self):
        return self.overview()

    def get(self, job_id: str):
        return self._client._get(f"/jobs/{job_id}")

    def get_vertex_ids(self, job_id: str):
        job = self.get(job_id)
        return [vertex["id"] for vertex in job.get("vertices", [])]

    def stop(self, job_id: str, savepoint_path: str = ""):
        payload = {"drain": False}
        if savepoint_path:
            payload["targetDirectory"] = savepoint_path
        return self._client._post(f"/jobs/{job_id}/stop", json=payload)

    def create_savepoint(self, job_id: str, savepoint_path: str = ""):
        payload = {}
        if savepoint_path:
            payload["target-directory"] = savepoint_path
        return self._client._post(f"/jobs/{job_id}/savepoints", json=payload)


class JarsAPI:
    def __init__(self, client: "FlinkRestClient"):
        self._client = client

    def all(self):
        return self._client._get("/jars")

    def run(self, jar_id: str, arguments=None, savepoint_path: str = ""):
        payload = {}
        if arguments:
            payload["programArgsList"] = [str(arg) for arg in arguments]
        if savepoint_path:
            payload["savepointPath"] = savepoint_path
        response = self._client._post(f"/jars/{jar_id}/run", json=payload)
        return response.get("jobid", "")


class TaskManagersAPI:
    def __init__(self, client: "FlinkRestClient"):
        self._client = client

    def all(self):
        return self._client._get("/taskmanagers").get("taskmanagers", [])

    def get_logs(self, taskmanager_id: str):
        response = self._client._get(f"/taskmanagers/{taskmanager_id}/logs")
        if isinstance(response, dict):
            return response.get("logs", [])
        return response


class FlinkRestClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        self.jobs = JobsAPI(self)
        self.jars = JarsAPI(self)
        self.taskmanagers = TaskManagersAPI(self)

    @classmethod
    def get(cls, host: str, port: int):
        return cls(host, port)

    def overview(self):
        return self._get("/overview")

    def _get(self, path: str):
        response = requests.get(f"{self.base_url}{path}")
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, json=None):
        response = requests.post(f"{self.base_url}{path}", json=json)
        response.raise_for_status()
        return response.json()
