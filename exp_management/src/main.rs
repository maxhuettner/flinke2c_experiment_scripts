use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use ::tracing::{info, level_filters::LevelFilter};
use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use dotenv::dotenv;
use petgraph::{Graph, visit::EdgeRef};
use resource_management::{
    ProvisioningPlan, ResourceSpec, ResourceType, TerraformDefaults,
    ansible::{InstanceNetworkInfo, TopologySummary, WireguardConfig},
    plan_from_resources, write_tfvars_json,
};
use russh::Sig;
use serde::Deserialize;
use tokio::{
    process::Command as TokioCommand,
    task::{self, JoinHandle},
    time::sleep,
};

use crate::{
    graph::{
        build_graph_from_file,
        topo_edge::TopoEdge,
        topo_node::{TopoNode, TopoNodeType},
    },
    ssh::{
        Client,
        comm_session::{CommandResponseChannels, SessionCommand},
    },
    tracing::init_tracing,
};

mod config;
mod graph;
mod ssh;
mod tracing;

#[derive(clap::ValueEnum, Clone)]
enum CommandType {
    Setup,
    Destroy,
    Experiment,
}

#[derive(clap::Parser)]
struct CommandLineArgs {
    #[clap(short, long, value_enum)]
    pub command_type: CommandType,

    #[clap(short, long = "config-file")]
    pub file: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let cmd_args = CommandLineArgs::parse();
    match cmd_args.command_type {
        CommandType::Setup => return setup(&cmd_args.file).await,
        CommandType::Destroy => return destroy(&cmd_args.file).await,
        CommandType::Experiment => {}
    }

    let log_level = env::var("LOG_LEVEL")
        .ok()
        .and_then(|val| val.parse::<LevelFilter>().ok())
        .unwrap_or(LevelFilter::INFO);

    init_tracing(log_level);

    let graph = build_graph_from_file("config/topologies/1.json").await;
    let node = graph
        .raw_nodes()
        .iter()
        .map(|node| node.weight.clone())
        .collect::<Vec<TopoNode>>();

    let user = env::var("SSH_USER").expect("ssh user required");
    let key_path = env::var("SSH_KEY_PATH").expect("ssh key path required");
    let pass_phrase = env::var("SSH_KEY_PASSPHRASE").ok();

    let node_2 = node.get(1).unwrap().clone();
    let user_2 = user.clone();
    let pass_phrase2 = pass_phrase.clone();
    let key_path2 = key_path.clone();
    let new_sess: JoinHandle<anyhow::Result<()>> = tokio::task::spawn(async move {
        let client = Client::connect(node_2.address.as_str(), &user_2, &key_path2, pass_phrase2.as_deref()).await?;
        let session = client.start_session().await?;

        let data = session
            .send_command_blocking(SessionCommand::Data("cat /etc/os-release".to_string()), None)
            .await;

        if let Ok(data) = data {
            info!(%data);
        }

        Ok(())
    });

    let client = Client::connect(
        node.first().unwrap().address.as_str(),
        &user,
        &key_path,
        pass_phrase.as_deref(),
    )
    .await?;

    let session = client.start_session().await?;

    let CommandResponseChannels { commands, responses } = session.command_response_channels();

    let cmd = "sleep 10 && echo Hello, world!".to_string();

    commands.send(SessionCommand::Data(cmd))?;

    if let Ok(data) = responses.recv().await {
        info!(%data);
    }

    let commands_clone = commands.clone();
    task::spawn(async move {
        sleep(Duration::from_secs(5)).await;
        commands_clone.send(SessionCommand::Signal(Sig::INT)).unwrap();
    });

    if let Ok(data) = responses.recv().await {
        info!(%data);
    }

    // let sftp_session = client.start_sftp_session().await?;
    // if let Err(error) = sftp_session.create_dir().await {
    //     warn!("Failed to create folder: {}", error);
    // }

    sleep(Duration::from_secs(1)).await;

    session.stop().await?;
    new_sess.await??;

    info!(
        "Loaded topology {:?} with {} nodes and {} edges",
        graph,
        graph.node_count(),
        graph.edge_count()
    );
    Ok(())
}

const TERRAFORM_DIR: &str = "resource_management/terraform";
const TERRAFORM_TFVARS: &str = "generated.auto.tfvars.json";
const ANSIBLE_DIR: &str = "exp_management/ansible";
const ANSIBLE_INVENTORY_PATH: &str = "exp_management/ansible/inventory/generated_hosts.yml";
const ANSIBLE_STATIC_ONPREM: &str = "exp_management/ansible/inventory/onprem.yml";
async fn setup(config_file: &str) -> Result<()> {
    let (plan, graph) = ensure_tfvars(config_file).await?;

    run_terraform(&["-chdir=resource_management/terraform", "init"]).await?;
    run_terraform(&[
        "-chdir=resource_management/terraform",
        "apply",
        "-auto-approve",
        "-var-file=generated.auto.tfvars.json",
    ])
    .await?;

    let terraform_outputs = fetch_terraform_outputs().await?;
    let ansible_user = env::var("ANSIBLE_CLOUD_SSH_USER")
        .or_else(|_| env::var("CLOUD_SSH_USER"))
        .unwrap_or_else(|_| "ubuntu".to_string());
    let ansible_key = resolve_private_key_path()?;

    let topology = TopologySummary {
        nodes: graph.raw_nodes().iter().map(|node| node.weight.id.clone()).collect(),
        edges: graph
            .edge_references()
            .map(|edge| (graph[edge.source()].id.clone(), graph[edge.target()].id.clone()))
            .collect(),
    };

    let instance_map: BTreeMap<String, InstanceNetworkInfo> = terraform_outputs
        .instances
        .value
        .iter()
        .map(|(id, meta)| {
            (
                id.clone(),
                InstanceNetworkInfo {
                    public_ip: meta.public_ip.clone(),
                    private_ip: meta.private_ip.clone(),
                },
            )
        })
        .collect();

    resource_management::ansible::write_inventory(
        &plan,
        &topology,
        &instance_map,
        &ansible_user,
        ansible_key.as_deref(),
        Some(Path::new(ANSIBLE_STATIC_ONPREM)),
        Path::new(ANSIBLE_INVENTORY_PATH),
        &WireguardConfig::default(),
    )?;
    run_ansible_playbook().await?;

    Ok(())
}

async fn destroy(config_file: &str) -> Result<()> {
    let _ = ensure_tfvars(config_file).await?;

    run_terraform(&["-chdir=resource_management/terraform", "init"]).await?;
    run_terraform(&[
        "-chdir=resource_management/terraform",
        "destroy",
        "-auto-approve",
        "-var-file=generated.auto.tfvars.json",
    ])
    .await?;

    Ok(())
}

async fn ensure_tfvars(config_file: &str) -> Result<(ProvisioningPlan, Graph<TopoNode, TopoEdge>)> {
    let graph = build_graph_from_file(config_file).await;
    let resources: Vec<ResourceSpec> = graph
        .raw_nodes()
        .iter()
        .map(|node| topo_node_to_resource(&node.weight))
        .collect();

    let mut defaults = TerraformDefaults::default();
    if defaults.ssh_public_key.is_none() {
        defaults.ssh_public_key = load_ssh_public_key()?;
    }
    let plan = plan_from_resources(resources, &defaults)?;
    let tfvars_path = Path::new(TERRAFORM_DIR).join(TERRAFORM_TFVARS);
    write_tfvars_json(&tfvars_path, &plan.terraform)?;

    Ok((plan, graph))
}

fn topo_node_to_resource(node: &TopoNode) -> ResourceSpec {
    let resource_type = match node.node_type {
        TopoNodeType::Source => ResourceType::Source,
        TopoNodeType::Sink => ResourceType::Sink,
        TopoNodeType::Compute => ResourceType::Compute,
    };

    let mut spec = ResourceSpec::new(node.id.clone(), resource_type).with_label(node.id.clone());
    spec.set_property("tag.address", node.address.clone());
    if let Some(speed) = node.speed {
        spec.set_property("tag.speed", speed.to_string());
    }

    spec
}

async fn run_terraform(args: &[&str]) -> Result<()> {
    let status = TokioCommand::new("terraform")
        .args(args)
        .status()
        .await
        .with_context(|| format!("failed to run terraform {:?}", args))?;

    if !status.success() {
        anyhow::bail!("terraform command {:?} failed with {}", args, status);
    }

    Ok(())
}

async fn run_ansible_playbook() -> Result<()> {
    let status = TokioCommand::new("ansible-playbook")
        .arg("playbooks/site.yml")
        .current_dir(ANSIBLE_DIR)
        .status()
        .await
        .with_context(|| "failed to run ansible-playbook")?;

    if !status.success() {
        anyhow::bail!("ansible-playbook failed with {}", status);
    }

    Ok(())
}

async fn fetch_terraform_outputs() -> Result<TerraformOutputs> {
    let output = TokioCommand::new("terraform")
        .args(["-chdir=resource_management/terraform", "output", "-json"])
        .output()
        .await
        .with_context(|| "failed to run terraform output -json")?;

    if !output.status.success() {
        anyhow::bail!("terraform output failed with {}", output.status);
    }

    let parsed: TerraformOutputs =
        serde_json::from_slice(&output.stdout).context("failed to parse terraform outputs")?;
    Ok(parsed)
}

#[derive(Debug, Deserialize)]
struct TerraformOutputs {
    instances: TerraformOutputValue<BTreeMap<String, TerraformInstanceMeta>>,
}

#[derive(Debug, Deserialize)]
struct TerraformOutputValue<T> {
    value: T,
}

#[derive(Debug, Deserialize)]
struct TerraformInstanceMeta {
    public_ip: Option<String>,
    private_ip: Option<String>,
    #[allow(dead_code)]
    id: Option<String>,
    #[allow(dead_code)]
    arn: Option<String>,
    #[allow(dead_code)]
    az: Option<String>,
}

fn resolve_private_key_path() -> Result<Option<String>> {
    if let Ok(explicit) = env::var("ANSIBLE_SSH_KEY_PATH") {
        return Ok(Some(expand_path_to_string(&explicit)?));
    }

    if let Ok(explicit) = env::var("SSH_KEY_PATH") {
        return Ok(Some(expand_path_to_string(&explicit)?));
    }

    Ok(default_private_key_path())
}

fn load_ssh_public_key() -> Result<Option<String>> {
    let Some((path, required)) = detect_public_key_path()? else {
        return Ok(None);
    };

    if !path.exists() {
        if required {
            bail!("SSH public key not found at {}", path.display());
        } else {
            return Ok(None);
        }
    }

    let contents =
        fs::read_to_string(&path).with_context(|| format!("failed to read SSH public key {}", path.display()))?;
    let key = contents.trim().to_string();
    if key.is_empty() {
        bail!("SSH public key at {} is empty", path.display());
    }

    Ok(Some(key))
}

fn detect_public_key_path() -> Result<Option<(PathBuf, bool)>> {
    if let Ok(path) = env::var("SSH_PUBLIC_KEY_PATH") {
        return Ok(Some((expand_path(&path)?, true)));
    }

    if let Ok(path) = env::var("ANSIBLE_SSH_PUBLIC_KEY_PATH") {
        return Ok(Some((expand_path(&path)?, true)));
    }

    for var in ["ANSIBLE_SSH_KEY_PATH", "SSH_KEY_PATH"] {
        if let Ok(base) = env::var(var) {
            if let Some(derived) = derive_public_key_path(&base) {
                return Ok(Some((expand_path(&derived)?, true)));
            }
        }
    }

    if let Some(default_path) = default_public_key_path() {
        return Ok(Some((default_path, false)));
    }

    Ok(None)
}

fn default_public_key_path() -> Option<PathBuf> {
    let mut path = resolve_home_dir()?;
    path.push(".ssh/id_ed25519.pub");
    Some(path)
}

fn default_private_key_path() -> Option<String> {
    let mut path = resolve_home_dir()?;
    path.push(".ssh/id_ed25519");
    if path.exists() {
        Some(path.to_string_lossy().into_owned())
    } else {
        None
    }
}

fn expand_path_to_string(input: &str) -> Result<String> {
    Ok(expand_path(input)?.to_string_lossy().into_owned())
}

fn expand_path(input: &str) -> Result<PathBuf> {
    if input == "~" {
        let home = resolve_home_dir().ok_or_else(|| anyhow!("home directory not set"))?;
        return Ok(home);
    }

    if let Some(stripped) = input.strip_prefix("~/") {
        let home = resolve_home_dir().ok_or_else(|| anyhow!("home directory not set"))?;
        return Ok(home.join(stripped));
    }

    Ok(PathBuf::from(input))
}

fn resolve_home_dir() -> Option<PathBuf> {
    if let Ok(home) = env::var("HOME") {
        return Some(PathBuf::from(home));
    }

    if let Ok(home) = env::var("USERPROFILE") {
        return Some(PathBuf::from(home));
    }

    None
}

fn derive_public_key_path(private_key: &str) -> Option<String> {
    if private_key.trim().is_empty() {
        return None;
    }

    if private_key.ends_with(".pub") {
        Some(private_key.to_string())
    } else {
        Some(format!("{}.pub", private_key))
    }
}
