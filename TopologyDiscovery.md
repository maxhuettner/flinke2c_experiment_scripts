# Topology discovery

`sim discover-edges` derives a topology's edge list from the live deployment and feeds into the existing JSON → graphml pipeline (`tools/topology_to_graphml.py`).

**Direct neighbors** (`--direct`). Each node sends a TTL=1 ICMP echo to every
other node's topology address. A reply means the destination answered before
any router had to decrement and drop the packet thus returning if the node is exactly one hop away on the internal network, independent of whether that hop is a WireGuard tunnel, a LAN segment, or native VPC routing. No reply means the pair is only
reachable through an intermediate node's forwarding and is excluded. The
result is an undirected edge set by construction.

## Output

A dry run prints the discovered edges and diffs them against
`load_topology()`'s expanded graph, which already accounts for
`network_type: all-to-all` groups so those aren't reported as spurious
differences. `--apply` writes the discovered pairs into the JSON's `edges`
array

## Commands

```bash
sim discover-edges -f config/topologies/X.json --direct
sim discover-edges -f config/topologies/X.json --direct --apply
python3 tools/topology_to_graphml.py config/topologies/X.json
```
