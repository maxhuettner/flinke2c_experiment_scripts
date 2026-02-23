locals {
  instance_definitions = var.ec2_instances
}

data "aws_ami" "ubuntu_jammy" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

data "aws_ami" "ubuntu_jammy_arm64" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-arm64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

data "aws_ec2_instance_type" "selected" {
  for_each      = local.instance_definitions
  instance_type = each.value.instance_type
}

resource "aws_key_pair" "generated" {
  count      = var.ssh_public_key == "" ? 0 : 1
  key_name   = var.ssh_key_pair_name
  public_key = var.ssh_public_key
}

# Dedicated VPC for experiment infrastructure.
resource "aws_vpc" "experiments" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "network-sim-experiments"
  }
}

resource "aws_internet_gateway" "experiments" {
  vpc_id = aws_vpc.experiments.id

  tags = {
    Name = "network-sim-igw"
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.experiments.id
  cidr_block              = var.public_subnet_cidr
  map_public_ip_on_launch = true
  availability_zone       = var.availability_zone

  tags = {
    Name = "network-sim-public"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.experiments.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.experiments.id
  }

  tags = {
    Name = "network-sim-public"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Security group that exposes SSH and WireGuard.
resource "aws_security_group" "experiment" {
  name        = "network-sim-experiment"
  description = "Common rules shared by experiment instances"
  vpc_id      = aws_vpc.experiments.id
}

resource "aws_security_group_rule" "intra_cluster_ingress" {
  type                     = "ingress"
  security_group_id        = aws_security_group.experiment.id
  protocol                 = "-1"
  from_port                = 0
  to_port                  = 0
  source_security_group_id = aws_security_group.experiment.id
}

resource "aws_security_group_rule" "ssh_ingress" {
  for_each          = toset(var.ssh_ingress_cidrs)
  type              = "ingress"
  security_group_id = aws_security_group.experiment.id
  protocol          = "tcp"
  from_port         = 22
  to_port           = 22
  cidr_blocks       = [each.value]
}

resource "aws_security_group_rule" "wireguard_ingress" {
  for_each          = toset(var.wireguard_ingress_cidrs)
  type              = "ingress"
  security_group_id = aws_security_group.experiment.id
  protocol          = "udp"
  from_port         = var.wireguard_udp_port
  to_port           = var.wireguard_udp_port_max
  cidr_blocks       = [each.value]
}

resource "aws_security_group_rule" "egress_all" {
  type              = "egress"
  security_group_id = aws_security_group.experiment.id
  protocol          = "-1"
  from_port         = 0
  to_port           = 0
  cidr_blocks       = ["0.0.0.0/0"]
  ipv6_cidr_blocks  = ["::/0"]
}

resource "aws_instance" "nodes" {
  for_each = local.instance_definitions

  ami = coalesce(
    each.value.ami,
    contains(data.aws_ec2_instance_type.selected[each.key].supported_architectures, "arm64") ?
    data.aws_ami.ubuntu_jammy_arm64.id :
    data.aws_ami.ubuntu_jammy.id
  )
  instance_type = each.value.instance_type
  private_ip    = each.value.private_ip
  subnet_id     = each.value.subnet_id == null ? aws_subnet.public.id : each.value.subnet_id
  key_name = (
    each.value.key_name != null ? each.value.key_name :
    length(aws_key_pair.generated) > 0 ? aws_key_pair.generated[0].key_name : var.ssh_key_name
  )
  associate_public_ip_address = (
    each.value.associate_public_ip_address == null ? true : each.value.associate_public_ip_address
  )
  iam_instance_profile = (
    each.value.iam_instance_profile == null ? null : each.value.iam_instance_profile
  )
  user_data = each.value.user_data == null ? var.default_user_data : each.value.user_data

  vpc_security_group_ids = distinct(
    concat(
      [aws_security_group.experiment.id],
      each.value.security_group_ids == null ? [] : each.value.security_group_ids
    )
  )

  tags = merge(
    {
      Name = each.key
    },
    lookup(each.value, "tags", {})
  )

  root_block_device {
    volume_size           = var.root_volume_size_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }
}
