import unittest
from pathlib import Path

from cli.inventory import _build_wireguard_graph, _cloud_transit_route_commands
from cli.terraform import build_tfvars_by_region
from cli.topology import load_topology


REPO_ROOT = Path(__file__).resolve().parents[1]
WAN_TOPOLOGY = REPO_ROOT / "config" / "topologies" / "wan.json"


class TerraformInventoryRegionTests(unittest.TestCase):
    def test_tfvars_are_grouped_per_region(self) -> None:
        graph = load_topology(str(WAN_TOPOLOGY))

        tfvars_by_region = build_tfvars_by_region(graph, aws_region="eu-central-1")

        self.assertEqual(set(tfvars_by_region), {"eu-west-3", "us-east-1"})
        self.assertEqual(
            set(tfvars_by_region["eu-west-3"]["ec2_instances"]),
            {"N1", "N6", "N7", "N8", "N9", "N10"},
        )
        self.assertEqual(
            set(tfvars_by_region["us-east-1"]["ec2_instances"]),
            {"N11", "N12", "N13", "N14", "N15", "snk"},
        )
        self.assertFalse(tfvars_by_region["eu-west-3"]["ec2_instances"]["N10"].get("source_dest_check", True))
        self.assertFalse(tfvars_by_region["us-east-1"]["ec2_instances"]["N13"].get("source_dest_check", True))

    def test_wireguard_graph_respects_native_mesh_vs_explicit_edges(self) -> None:
        graph = load_topology(str(WAN_TOPOLOGY))

        wg_graph = _build_wireguard_graph(graph, default_cloud_region="eu-central-1")

        self.assertTrue(wg_graph.has_edge("N1", "N6"))
        self.assertTrue(wg_graph.has_edge("N6", "N8"))
        self.assertTrue(wg_graph.has_edge("N8", "N11"))
        self.assertFalse(wg_graph.has_edge("N11", "N14"))

    def test_route_only_cloud_nodes_get_routes_to_remote_region(self) -> None:
        graph = load_topology(str(WAN_TOPOLOGY))

        route_cmds = _cloud_transit_route_commands(
            graph,
            "N14",
            default_cloud_region="eu-central-1",
        )

        self.assertTrue(any("10.20.0.13/32" in cmd for cmd in route_cmds))


if __name__ == "__main__":
    unittest.main()
