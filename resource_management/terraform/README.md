# Terraform Bootstrap

This directory contains the minimal Terraform configuration required to turn the
GraphML experiment description into AWS infrastructure that the CLI can later
control. The CLI is expected to parse the GraphML, translate the nodes into the
`ec2_instances` variable, run Terraform to provision the instances, and finally
configure them (for example by installing WireGuard and the experiment payload).

## Layout

| File | Purpose |
| --- | --- |
| `versions.tf` | Pins Terraform and AWS provider versions. |
| `provider.tf` | Configures the AWS provider and default tags. |
| `variables.tf` | All tunable inputs that can be generated from GraphML. |
| `main.tf` | Networking, security groups, and the parametrised EC2 instances. |
| `outputs.tf` | Connection data that the CLI can consume after `apply`. |

## Variable contract

The CLI should convert each GraphML node into an entry in `ec2_instances`. The
map key should stay stable (e.g. GraphML node id) so Terraform can reconcile
updates and deletions automatically. Each entry supports:

```hcl
ec2_instances = {
  "node-a" = {
    ami           = "ami-0ecb62995f68bb549"
    instance_type = "t3.small"
    user_data     = file("${path.module}/user_data/node-a.sh")
    tags = {
      role = "router"
    }
  }
}
```

All other fields (`ami`, `subnet_id`, `key_name`,
`associate_public_ip_address`, `security_group_ids`, `iam_instance_profile`)
are optional per instance. A
default subnet, VPC, security group, and internet access are created for cases
where the GraphML file does not specify networking.

When `ami` is omitted the configuration automatically uses the latest Ubuntu
22.04 LTS (Jammy) AMI for the selected region (via the Canonical owner ID
`099720109477`). Override it per instance if you need a custom image.

To make the automation easier, the CLI can emit a tfvars file (HCL or JSON). A
simple HCL example looks like:

```hcl
aws_region             = "us-east-1"
ssh_key_name           = "network-sim"
wireguard_ingress_cidrs = ["203.0.113.0/24"]

ec2_instances = {
  router-1 = {
    ami           = "ami-0ecb62995f68bb549"
    instance_type = "t3.micro"
    user_data     = file("./user_data/router-1.sh")
    tags = {
      role = "router"
    }
  }

  client-a = {
    ami                         = "ami-01b9f1e7dc427266e"
    instance_type               = "t4g.small"
    associate_public_ip_address = true
  }
}
```

The CLI can then run Terraform with:

```
terraform -chdir=terraform init
terraform -chdir=terraform apply -var-file=generated.auto.tfvars
```

## WireGuard workflow

The shared security group already exposes port `var.wireguard_udp_port` to the
`wireguard_ingress_cidrs`. The CLI should still provision the WireGuard keys,
peer configuration, and interface setup. Passing a rendered script through the
`user_data` field is the simplest option, but you can also push the configs
after the instances boot and the CLI retrieves the IP addresses from the
`instances` output.

## On-prem integration

Only the AWS side is provisioned here. After `apply` succeeds the CLI can merge
the Terraform `instances` output with the pre-existing on-prem inventory and run
the configuration phase (Ansible, SSH automation, etc.) in a uniform way.

## CLI integration plan

The `resource_management` binary can orchestrate everything as follows:

1. Parse the GraphML file and convert every relevant node into an entry of
   `ec2_instances`. Persist the map plus global inputs (region, CIDRs, SSH/WG
   allow-lists) into `resource_management/terraform/generated.auto.tfvars`.
2. Shell out to Terraform with `terraform -chdir=resource_management/terraform
   init` (first run) and `terraform -chdir=resource_management/terraform apply`
   or `destroy` to create/tear down the AWS portion of the experiment.
3. Consume `terraform -chdir=resource_management/terraform output -json` so the
   CLI knows the instance IDs and connection info needed for the next steps.
4. Run the configuration phase (future Ansible or direct SSH) against the newly
   created EC2 hosts **and** the static on-prem hosts referenced in the GraphML
   file, ensuring they all end up with the required software and WireGuard mesh.

Once this flow is in place you only need to add the Ansible playbooks and the
SSH/experiment execution logic without revisiting the Terraform layout.
