use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::{collections::BTreeMap, fs, path::Path};

pub mod ansible;

/// High-level provisioning plan that the CLI can feed into Terraform and later
/// provisioning steps (e.g., Ansible and SSH orchestration).
#[derive(Debug, Clone)]
pub struct ProvisioningPlan {
    pub resources: Vec<ResourceSpec>,
    pub terraform: TerraformInputVars,
    pub resources_to_provision: Vec<ResourceSpec>,
}

/// Minimal description of a compute resource extracted from GraphML (or any
/// other experiment description) by the CLI.
#[derive(Debug, Clone)]
pub struct ResourceSpec {
    pub id: String,
    pub label: Option<String>,
    pub resource_type: ResourceType,
    pub properties: BTreeMap<String, String>,
}

impl ResourceSpec {
    pub fn new(id: impl Into<String>, resource_type: ResourceType) -> Self {
        Self {
            id: id.into(),
            label: None,
            resource_type,
            properties: BTreeMap::new(),
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.properties.insert(key.into(), value.into());
    }

    pub fn property(&self, key: &str) -> Option<&str> {
        get_case_insensitive(&self.properties, key)
    }

    pub fn should_create_cloud_resource(&self) -> bool {
        if let Some(value) = self.property("provision") {
            match value.to_ascii_lowercase().as_str() {
                "existing" | "static" | "skip" | "false" | "onprem" | "on-prem" => return false,
                "true" | "create" | "cloud" | "aws" | "ec2" | "new" => return true,
                _ => {}
            }
        }

        matches!(self.resource_type, ResourceType::Compute | ResourceType::AwsEc2)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceType {
    Source,
    Sink,
    Compute,
    AwsEc2,
    OnPrem,
    Unknown(String),
}

impl ResourceType {
    pub fn from_string(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "source" => Self::Source,
            "sink" => Self::Sink,
            "compute" => Self::Compute,
            "aws" | "aws_ec2" | "ec2" => Self::AwsEc2,
            "onprem" | "on-prem" => Self::OnPrem,
            other => Self::Unknown(other.to_string()),
        }
    }

    fn as_tag_value(&self) -> &str {
        match self {
            Self::Source => "source",
            Self::Sink => "sink",
            Self::Compute => "compute",
            Self::AwsEc2 => "aws_ec2",
            Self::OnPrem => "onprem",
            Self::Unknown(value) => value.as_str(),
        }
    }
}

/// Defaults that are injected into the generated Terraform variables file when
/// a node does not override them in the experiment description.
#[derive(Debug, Clone)]
pub struct TerraformDefaults {
    pub aws_region: String,
    pub availability_zone: Option<String>,
    pub vpc_cidr: String,
    pub public_subnet_cidr: String,
    pub default_tags: BTreeMap<String, String>,
    pub instance_tags: BTreeMap<String, String>,
    pub ssh_key_name: Option<String>,
    pub ssh_public_key: Option<String>,
    pub ssh_key_pair_name: Option<String>,
    pub ssh_ingress_cidrs: Vec<String>,
    pub wireguard_ingress_cidrs: Vec<String>,
    pub wireguard_udp_port: u16,
    pub default_user_data: Option<String>,
    pub default_ami: Option<String>,
    pub default_instance_type: String,
}

impl Default for TerraformDefaults {
    fn default() -> Self {
        Self {
            aws_region: "eu-central-1".to_string(),
            availability_zone: None,
            vpc_cidr: "10.42.0.0/16".to_string(),
            public_subnet_cidr: "10.42.0.0/20".to_string(),
            default_tags: BTreeMap::new(),
            instance_tags: BTreeMap::new(),
            ssh_key_name: None,
            ssh_public_key: None,
            ssh_key_pair_name: Some("network-sim-cli".to_string()),
            ssh_ingress_cidrs: vec!["0.0.0.0/0".to_string()],
            wireguard_ingress_cidrs: Vec::new(),
            wireguard_udp_port: 51_820,
            default_user_data: None,
            default_ami: None,
            default_instance_type: "t3.micro".to_string(),
        }
    }
}

/// Object that mirrors the Terraform variables exposed in
/// `resource_management/terraform/variables.tf`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TerraformInputVars {
    pub aws_region: String,
    pub vpc_cidr: String,
    pub public_subnet_cidr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub availability_zone: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub default_tags: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssh_key_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssh_public_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssh_key_pair_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ssh_ingress_cidrs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wireguard_ingress_cidrs: Vec<String>,
    pub wireguard_udp_port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_user_data: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ec2_instances: BTreeMap<String, TerraformInstanceConfig>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TerraformInstanceConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ami: Option<String>,
    pub instance_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subnet_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub associate_public_ip_address: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_group_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iam_instance_profile: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

/// Build a provisioning plan from a list of resources that the CLI already
/// parsed from the experiment description.
pub fn plan_from_resources(resources: Vec<ResourceSpec>, defaults: &TerraformDefaults) -> Result<ProvisioningPlan> {
    let terraform = build_terraform_inputs(&resources, defaults)?;
    let resources_to_provision = resources
        .iter()
        .filter(|node| node.should_create_cloud_resource())
        .cloned()
        .collect();

    Ok(ProvisioningPlan {
        resources,
        terraform,
        resources_to_provision,
    })
}

/// Serialize the Terraform inputs to JSON so they can be consumed by
/// `terraform apply -var-file`.
pub fn write_tfvars_json(path: impl AsRef<Path>, vars: &TerraformInputVars) -> Result<()> {
    let data = serde_json::to_string_pretty(vars)?;
    fs::write(&path, format!("{data}\n")).with_context(|| format!("failed to write {:?}", path.as_ref()))?;
    Ok(())
}

/// Build the Terraform variable structure from the provided resources.
pub fn build_terraform_inputs(resources: &[ResourceSpec], defaults: &TerraformDefaults) -> Result<TerraformInputVars> {
    let mut instances = BTreeMap::new();
    for node in resources {
        if !node.should_create_cloud_resource() {
            continue;
        }

        let instance = terraform_instance_from_resource(node, defaults)
            .with_context(|| format!("failed to convert node {} into Terraform input", node.id))?;
        instances.insert(node.id.clone(), instance);
    }

    Ok(TerraformInputVars {
        aws_region: defaults.aws_region.clone(),
        vpc_cidr: defaults.vpc_cidr.clone(),
        public_subnet_cidr: defaults.public_subnet_cidr.clone(),
        availability_zone: defaults.availability_zone.clone(),
        default_tags: defaults.default_tags.clone(),
        ssh_key_name: defaults.ssh_key_name.clone(),
        ssh_public_key: defaults.ssh_public_key.clone(),
        ssh_key_pair_name: defaults.ssh_key_pair_name.clone(),
        ssh_ingress_cidrs: defaults.ssh_ingress_cidrs.clone(),
        wireguard_ingress_cidrs: defaults.wireguard_ingress_cidrs.clone(),
        wireguard_udp_port: defaults.wireguard_udp_port,
        default_user_data: defaults.default_user_data.clone(),
        ec2_instances: instances,
    })
}

fn terraform_instance_from_resource(
    node: &ResourceSpec,
    defaults: &TerraformDefaults,
) -> Result<TerraformInstanceConfig> {
    let ami = node
        .property("ami")
        .or_else(|| node.property("ami_id"))
        .map(|value| value.to_string())
        .or_else(|| defaults.default_ami.clone());

    let instance_type = node
        .property("instance_type")
        .or_else(|| node.property("instanceType"))
        .map(|value| value.to_string())
        .unwrap_or_else(|| defaults.default_instance_type.clone());

    if instance_type.trim().is_empty() {
        bail!(
            "node {} is missing an instance type and no default is configured",
            node.id
        );
    }

    let associate_public_ip_address = node
        .property("associate_public_ip_address")
        .or_else(|| node.property("public_ip"))
        .or_else(|| node.property("publicIp"))
        .and_then(parse_bool);

    let security_group_ids = node
        .property("security_group_ids")
        .or_else(|| node.property("security_groups"))
        .map(parse_list);

    let mut tags = defaults.instance_tags.clone();
    tags.entry("Name".to_string())
        .or_insert_with(|| node.label.clone().unwrap_or_else(|| node.id.clone()));
    tags.insert("graph_node_id".to_string(), node.id.clone());
    tags.insert(
        "graph_node_type".to_string(),
        node.resource_type.as_tag_value().to_string(),
    );

    if let Some(label) = &node.label {
        tags.insert("graph_node_label".to_string(), label.clone());
    }

    for (key, value) in &node.properties {
        if let Some(tag_key) = key.strip_prefix("tag.") {
            tags.insert(tag_key.to_string(), value.clone());
        }
    }

    Ok(TerraformInstanceConfig {
        ami,
        instance_type,
        subnet_id: node.property("subnet_id").map(str::to_string),
        key_name: node.property("key_name").map(str::to_string),
        associate_public_ip_address,
        security_group_ids,
        user_data: node.property("user_data").map(str::to_string),
        iam_instance_profile: node
            .property("iam_instance_profile")
            .or_else(|| node.property("instance_profile"))
            .map(str::to_string),
        tags,
    })
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Some(true),
        "false" | "0" | "no" | "n" => Some(false),
        _ => None,
    }
}

fn parse_list(value: &str) -> Vec<String> {
    value
        .split(|ch| ch == ',' || ch == ';')
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(|item| item.to_string())
        .collect()
}

fn get_case_insensitive<'a>(properties: &'a BTreeMap<String, String>, needle: &str) -> Option<&'a str> {
    if let Some(value) = properties.get(needle) {
        return Some(value);
    }

    let needle_lower = needle.to_ascii_lowercase();
    properties.iter().find_map(|(key, value)| {
        if key.to_ascii_lowercase() == needle_lower {
            Some(value.as_str())
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_resources() -> Vec<ResourceSpec> {
        let mut compute_a = ResourceSpec::new("cloud-a", ResourceType::Compute)
            .with_label("router-a")
            .with_property("ami", "ami-111111")
            .with_property("instance_type", "t3.small");
        compute_a.set_property("tag.role", "router");

        let compute_b = ResourceSpec::new("cloud-b", ResourceType::AwsEc2).with_property("public_ip", "false");

        let onprem = ResourceSpec::new("on-prem", ResourceType::OnPrem).with_property("provision", "existing");

        vec![compute_a, compute_b, onprem]
    }

    #[test]
    fn filters_resources_correctly() {
        let defaults = TerraformDefaults::default();
        let resources = sample_resources();
        let plan = plan_from_resources(resources, &defaults).expect("plan is built");

        assert_eq!(plan.resources.len(), 3);
        assert_eq!(plan.resources_to_provision.len(), 2);
    }

    #[test]
    fn builds_terraform_instances_from_resources() {
        let resources = sample_resources();
        let defaults = TerraformDefaults {
            default_ami: Some("ami-default".to_string()),
            default_instance_type: "t3.micro".to_string(),
            ssh_ingress_cidrs: vec!["203.0.113.0/24".to_string()],
            wireguard_ingress_cidrs: vec!["198.51.100.0/24".to_string()],
            ..TerraformDefaults::default()
        };

        let vars = build_terraform_inputs(&resources, &defaults).unwrap();
        assert_eq!(vars.ec2_instances.len(), 2);
        assert_eq!(vars.ec2_instances["cloud-a"].instance_type, "t3.small".to_string());
        assert_eq!(vars.ec2_instances["cloud-b"].ami.as_deref(), Some("ami-default"));
    }
}
