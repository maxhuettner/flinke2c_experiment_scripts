import unittest
from pathlib import Path

from cli.topology import load_topology


REPO_ROOT = Path(__file__).resolve().parents[1]
WAN_TOPOLOGY = REPO_ROOT / "config" / "topologies" / "wan.json"


class TopologyRegionTests(unittest.TestCase):
    def test_wan_topology_expands_same_region_all_to_all_edges(self) -> None:
        graph = load_topology(str(WAN_TOPOLOGY))

        self.assertTrue(graph.has_edge("N11", "N14"))
        self.assertTrue(graph.has_edge("N11", "snk"))
        self.assertTrue(graph.has_edge("N14", "N15"))

    def test_cloud_region_inference_distinguishes_on_prem(self) -> None:
        graph = load_topology(str(WAN_TOPOLOGY))

        self.assertEqual(graph.nodes["N1"]["data"].cloud_region("eu-central-1"), "eu-west-3")
        self.assertEqual(graph.nodes["N11"]["data"].cloud_region("eu-central-1"), "us-east-1")
        self.assertIsNone(graph.nodes["N2"]["data"].cloud_region("eu-central-1"))


if __name__ == "__main__":
    unittest.main()
