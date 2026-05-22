import json
import tempfile
import unittest
from pathlib import Path

from cli.experiment import (
    NodeInfo,
    _desired_nes_topology_links,
    _map_topology_workers_to_nes_ids,
    _rewrite_nes_query_sink_host,
)


class NesTopologyTests(unittest.TestCase):
    def test_rewrite_nes_query_sink_host_uses_runtime_sink_address(self) -> None:
        query = (
            'Query::from("bids").sink(TcpSinkDescriptor::create("10.10.0.10", 9000));'
        )

        rewritten = _rewrite_nes_query_sink_host(query, "10.30.0.10")

        self.assertIn('TcpSinkDescriptor::create("10.30.0.10", 9000)', rewritten)

    def test_desired_links_include_only_compute_to_compute_edges(self) -> None:
        topology = {
            "nodes": [
                {"id": "src", "node_type": "source"},
                {"id": "A", "node_type": "compute"},
                {"id": "B", "node_type": "compute"},
                {"id": "C", "node_type": "compute"},
                {"id": "snk", "node_type": "sink"},
            ],
            "edges": [
                {"source": "src", "target": "A"},
                {"source": "A", "target": "B"},
                {"source": "A", "target": "C"},
                {"source": "B", "target": "snk"},
            ],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            topology_path = Path(tmpdir) / "topology.json"
            topology_path.write_text(json.dumps(topology))
            links = _desired_nes_topology_links(
                str(topology_path),
                {"A", "B", "C"},
            )

        self.assertEqual(links, [("B", "A"), ("C", "A")])

    def test_desired_links_allow_multiple_parents_for_same_child(self) -> None:
        topology = {
            "nodes": [
                {"id": "A", "node_type": "compute"},
                {"id": "B", "node_type": "compute"},
                {"id": "C", "node_type": "compute"},
            ],
            "edges": [
                {"source": "A", "target": "C"},
                {"source": "B", "target": "C"},
            ],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            topology_path = Path(tmpdir) / "topology.json"
            topology_path.write_text(json.dumps(topology))
            links = _desired_nes_topology_links(str(topology_path), {"A", "B", "C"})

        self.assertEqual(links, [("C", "A"), ("C", "B")])

    def test_map_topology_workers_to_nes_ids_uses_reported_addresses(self) -> None:
        workers = [
            NodeInfo(
                id="A",
                host="203.0.113.10",
                user="ubuntu",
                node_type="compute",
                address="10.0.0.11",
            ),
            NodeInfo(
                id="B",
                host="203.0.113.11",
                user="ubuntu",
                node_type="compute",
                address="10.0.0.12",
            ),
        ]
        payload = {
            "nodes": [
                {
                    "workerId": 7,
                    "details": {"localWorkerHost": "10.0.0.11"},
                },
                {
                    "id": "9",
                    "network": {"host": "http://10.0.0.12:1337"},
                },
            ]
        }

        mapping = _map_topology_workers_to_nes_ids(payload, workers)

        self.assertEqual(mapping, {"A": 7, "B": 9})


if __name__ == "__main__":
    unittest.main()
