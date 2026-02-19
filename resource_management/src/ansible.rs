use crate::ProvisioningPlan;
use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_yaml;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fs,
    net::Ipv4Addr,
    path::Path,
};
use tracing::warn;
use x25519_dalek::{PublicKey, StaticSecret};

const DEFAULT_LISTEN_PORT: u16 = 51_820;
const DEFAULT_INTERFACE: &str = "wg0";
const DEFAULT_KEY_SALT: &str = "network-sim-wireguard";
const DEFAULT_PREFIX: u8 = 32;
const DEFAULT_NETWORK_BASE: Ipv4Addr = Ipv4Addr::new(10, 10, 10, 0);

/// Summary of the experiment topology so WireGuard peers and routes can be derived without
/// depending on `petgraph` or experiment-specific types.
#[derive(Debug, Clone, Default)]
pub struct TopologySummary {
    pub nodes: Vec<String>,
    pub edges: Vec<(String, String)>,
}

/// Information about reachable IPs for a Terraform-managed instance.
#[derive(Debug, Clone, Default)]
pub struct InstanceNetworkInfo {
    pub public_ip: Option<String>,
    pub private_ip: Option<String>,
}

/// Tunables for the generated WireGuard configuration.
#[derive(Debug, Clone)]
pub struct WireguardConfig {
    pub interface: String,
    pub listen_port: u16,
    pub key_salt: String,
    pub network_base: Ipv4Addr,
    pub address_prefix: u8,
}

impl Default for WireguardConfig {
    fn default() -> Self {
        Self {
            interface: DEFAULT_INTERFACE.to_string(),
            listen_port: DEFAULT_LISTEN_PORT,
            key_salt: DEFAULT_KEY_SALT.to_string(),
            network_base: DEFAULT_NETWORK_BASE,
            address_prefix: DEFAULT_PREFIX,
        }
    }
}

/// Render the inventory by merging Terraform outputs, static on-prem entries, and graph metadata.
pub fn write_inventory(
    plan: &ProvisioningPlan,
    topology: &TopologySummary,
    instances: &BTreeMap<String, InstanceNetworkInfo>,
    ansible_user: &str,
    ansible_key: Option<&str>,
    onprem_inventory_path: Option<&Path>,
    output_path: &Path,
    wireguard: &WireguardConfig,
) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).context("failed to create ansible inventory directory")?;
    }

    let mut inventory = if let Some(path) = onprem_inventory_path {
        load_onprem_inventory(path)?
    } else {
        InventoryDoc::default()
    };

    let mut host_entries: BTreeMap<String, HostEntry> = BTreeMap::new();
    for (host, vars) in inventory.all.children.onprem.hosts.clone() {
        host_entries.insert(host.clone(), HostEntry { vars, is_cloud: false });
    }

    for resource in &plan.resources_to_provision {
        let Some(instance) = instances.get(&resource.id) else {
            warn!("terraform output missing instance {}", resource.id);
            continue;
        };

        let host_ip = instance.public_ip.clone().or(instance.private_ip.clone());
        if host_ip.is_none() {
            warn!(
                "instance {} has no reachable IP, skipping ansible inventory entry",
                resource.id
            );
            continue;
        }

        host_entries.insert(
            resource.id.clone(),
            HostEntry {
                vars: HostVars {
                    ansible_host: host_ip,
                    ansible_user: Some(ansible_user.to_string()),
                    ansible_ssh_private_key_file: ansible_key.map(|key| key.to_string()),
                    node_class: Some("cloud".to_string()),
                    wireguard: None,
                    extra: BTreeMap::new(),
                },
                is_cloud: true,
            },
        );
    }

    configure_wireguard(&mut host_entries, topology, wireguard);

    let mut new_cloud_hosts = BTreeMap::new();
    let mut new_onprem_hosts = BTreeMap::new();

    for (host, entry) in host_entries {
        if entry.is_cloud {
            new_cloud_hosts.insert(host.clone(), entry.vars);
        } else {
            new_onprem_hosts.insert(host.clone(), entry.vars);
        }
    }

    inventory.all.children.cloud.hosts = new_cloud_hosts;
    inventory.all.children.onprem.hosts = new_onprem_hosts;

    let yaml = serde_yaml::to_string(&inventory).context("failed to serialize ansible inventory")?;
    fs::write(output_path, yaml).context("failed to write ansible inventory file")?;
    Ok(())
}

fn load_onprem_inventory(path: &Path) -> Result<InventoryDoc> {
    if path.exists() {
        let file = fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let doc: InventoryDoc = serde_yaml::from_reader(file).context("failed to parse on-prem inventory")?;
        Ok(doc)
    } else {
        Ok(InventoryDoc::default())
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct InventoryDoc {
    all: InventoryAll,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct InventoryAll {
    #[serde(default)]
    children: InventoryChildren,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct InventoryChildren {
    #[serde(default)]
    cloud: HostGroup,
    #[serde(default, skip_serializing_if = "host_group_is_empty")]
    onprem: HostGroup,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct HostGroup {
    #[serde(default)]
    hosts: BTreeMap<String, HostVars>,
}

fn host_group_is_empty(group: &HostGroup) -> bool {
    group.hosts.is_empty()
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct HostVars {
    #[serde(skip_serializing_if = "Option::is_none")]
    ansible_host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ansible_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ansible_ssh_private_key_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wireguard: Option<WireguardHostVars>,
    #[serde(flatten, default)]
    extra: BTreeMap<String, serde_yaml::Value>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct WireguardHostVars {
    #[serde(skip_serializing_if = "Option::is_none")]
    interface: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    listen_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peers: Option<Vec<WireguardPeer>>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct WireguardPeer {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    public_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    allowed_ips: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    persistent_keepalive: Option<u16>,
}

struct HostEntry {
    vars: HostVars,
    is_cloud: bool,
}

#[derive(Clone)]
struct WireguardKeyPair {
    private_key: String,
    public_key: String,
}

fn configure_wireguard(
    host_entries: &mut BTreeMap<String, HostEntry>,
    topology: &TopologySummary,
    wireguard: &WireguardConfig,
) {
    if host_entries.is_empty() {
        return;
    }

    let mut participant_ids: Vec<String> = topology
        .nodes
        .iter()
        .filter_map(|id| {
            if host_entries.contains_key(id) {
                Some(id.clone())
            } else {
                warn!(
                    "Skipping WireGuard config for graph node '{}' because it is not present in the inventory",
                    id
                );
                None
            }
        })
        .collect();

    if participant_ids.is_empty() {
        return;
    }

    participant_ids.sort();
    participant_ids.dedup();

    let address_map = assign_wireguard_addresses(&participant_ids, &wireguard.network_base, wireguard.address_prefix);
    let mut key_map = HashMap::new();
    for id in &participant_ids {
        key_map.insert(id.clone(), generate_wireguard_keypair(id, &wireguard.key_salt));
    }

    let adjacency = build_adjacency_map(topology, host_entries);
    let routing_tables = compute_next_hops(&participant_ids, &adjacency);

    for node_id in participant_ids {
        let peers = build_peer_list(
            routing_tables.get(&node_id),
            host_entries,
            &address_map,
            &key_map,
            wireguard.listen_port,
        );

        if let Some(entry) = host_entries.get_mut(&node_id) {
            let existing = entry.vars.wireguard.clone();
            let listen_port = existing
                .as_ref()
                .and_then(|wg| wg.listen_port)
                .unwrap_or(wireguard.listen_port);

            let endpoint = existing.as_ref().and_then(|wg| wg.endpoint.clone()).or_else(|| {
                entry
                    .vars
                    .ansible_host
                    .as_ref()
                    .map(|host| format!("{}:{}", host, listen_port))
            });

            let private_key = existing
                .as_ref()
                .and_then(|wg| wg.private_key.clone())
                .unwrap_or_else(|| key_map[&node_id].private_key.clone());

            entry.vars.wireguard = Some(WireguardHostVars {
                interface: Some(wireguard.interface.clone()),
                listen_port: Some(listen_port),
                private_key: Some(private_key),
                address: address_map.get(&node_id).cloned(),
                endpoint,
                peers: Some(peers),
            });

            entry.vars.node_class.get_or_insert_with(|| {
                if entry.is_cloud {
                    "cloud".to_string()
                } else {
                    "onprem".to_string()
                }
            });
        }
    }
}

fn build_adjacency_map(
    topology: &TopologySummary,
    host_entries: &BTreeMap<String, HostEntry>,
) -> HashMap<String, Vec<String>> {
    let mut adjacency: HashMap<String, BTreeSet<String>> = HashMap::new();

    for node_id in host_entries.keys() {
        adjacency.entry(node_id.clone()).or_default();
    }

    for (source_id, target_id) in &topology.edges {
        if !host_entries.contains_key(source_id) || !host_entries.contains_key(target_id) {
            continue;
        }

        adjacency
            .entry(source_id.clone())
            .or_default()
            .insert(target_id.clone());
        adjacency
            .entry(target_id.clone())
            .or_default()
            .insert(source_id.clone());
    }

    adjacency
        .into_iter()
        .map(|(node, neighbors)| (node, neighbors.into_iter().collect()))
        .collect()
}

fn compute_next_hops(
    participant_ids: &[String],
    adjacency: &HashMap<String, Vec<String>>,
) -> HashMap<String, HashMap<String, String>> {
    let mut tables = HashMap::new();

    for source in participant_ids {
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut hop_map: HashMap<String, String> = HashMap::new();

        visited.insert(source.clone());
        queue.push_back(source.clone());

        while let Some(current) = queue.pop_front() {
            if let Some(neighbors) = adjacency.get(&current) {
                for neighbor in neighbors {
                    if !visited.insert(neighbor.clone()) {
                        continue;
                    }

                    let first_hop = if current == *source {
                        neighbor.clone()
                    } else {
                        hop_map.get(&current).cloned().unwrap_or_else(|| neighbor.clone())
                    };

                    hop_map.insert(neighbor.clone(), first_hop.clone());
                    queue.push_back(neighbor.clone());
                }
            }
        }

        let mut routes = HashMap::new();
        for dest in participant_ids {
            if dest == source {
                continue;
            }

            if let Some(next_hop) = hop_map.get(dest) {
                routes.insert(dest.clone(), next_hop.clone());
            } else {
                warn!(
                    "node '{}' cannot reach '{}' in the topology; skipping WireGuard route",
                    source, dest
                );
            }
        }

        tables.insert(source.clone(), routes);
    }

    tables
}

fn build_peer_list(
    routes: Option<&HashMap<String, String>>,
    host_entries: &BTreeMap<String, HostEntry>,
    address_map: &HashMap<String, String>,
    key_map: &HashMap<String, WireguardKeyPair>,
    default_listen_port: u16,
) -> Vec<WireguardPeer> {
    let mut allowed_by_peer: BTreeMap<String, Vec<String>> = BTreeMap::new();

    if let Some(routes) = routes {
        for (dest, next_hop) in routes {
            let Some(address) = address_map.get(dest) else {
                continue;
            };
            allowed_by_peer
                .entry(next_hop.clone())
                .or_default()
                .push(address.clone());
        }
    }

    let mut peers = Vec::new();
    for (peer_id, mut allowed_ips) in allowed_by_peer {
        let Some(peer_entry) = host_entries.get(&peer_id) else {
            continue;
        };
        let Some(peer_keys) = key_map.get(&peer_id) else {
            continue;
        };

        allowed_ips.sort();
        allowed_ips.dedup();

        let endpoint = peer_entry
            .vars
            .wireguard
            .as_ref()
            .and_then(|wg| wg.endpoint.clone())
            .or_else(|| {
                peer_entry.vars.ansible_host.as_ref().map(|host| {
                    let port = peer_entry
                        .vars
                        .wireguard
                        .as_ref()
                        .and_then(|wg| wg.listen_port)
                        .unwrap_or(default_listen_port);
                    format!("{}:{}", host, port)
                })
            });

        peers.push(WireguardPeer {
            name: Some(peer_id.clone()),
            public_key: Some(peer_keys.public_key.clone()),
            endpoint,
            allowed_ips: Some(allowed_ips),
            persistent_keepalive: Some(25),
        });
    }

    peers
}

fn assign_wireguard_addresses(ids: &[String], base: &Ipv4Addr, prefix: u8) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let base = u32::from(*base);
    for (offset, node_id) in ids.iter().enumerate() {
        let ip = Ipv4Addr::from(base + (offset as u32) + 1);
        map.insert(node_id.clone(), format!("{}/{}", ip, prefix));
    }
    map
}

fn generate_wireguard_keypair(node_id: &str, salt: &str) -> WireguardKeyPair {
    let mut hasher = Sha256::new();
    hasher.update(salt.as_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut secret_bytes = [0u8; 32];
    secret_bytes.copy_from_slice(&digest[..32]);
    let secret = StaticSecret::from(secret_bytes);
    let public = PublicKey::from(&secret);
    WireguardKeyPair {
        private_key: STANDARD.encode(secret.to_bytes()),
        public_key: STANDARD.encode(public.to_bytes()),
    }
}
