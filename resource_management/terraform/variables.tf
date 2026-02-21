variable "aws_region" {
  description = "AWS region where experiment infrastructure will be created."
  type        = string
  default     = "us-east-1"
}

variable "availability_zone" {
  description = "Optional availability zone override for the public subnet."
  type        = string
  default     = null
}

variable "default_tags" {
  description = "Tags that are added to every resource."
  type        = map(string)
  default     = {}
}

variable "vpc_cidr" {
  description = "CIDR block for the dedicated experiment VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "public_subnet_cidr" {
  description = "CIDR block for the default public subnet used by EC2 instances."
  type        = string
  default     = "10.42.0.0/20"
}

variable "ssh_key_name" {
  description = "Default EC2 key pair to associate with instances. Leave null to rely on instance-specific overrides."
  type        = string
  default     = null
}

variable "ssh_public_key" {
  description = "Raw public key material used to create a temporary EC2 key pair (takes precedence over ssh_key_name when set)."
  type        = string
  default     = ""
}

variable "ssh_key_pair_name" {
  description = "Key pair name to use when uploading ssh_public_key."
  type        = string
  default     = "network-sim-cli"
}

variable "ssh_ingress_cidrs" {
  description = "CIDR ranges that are allowed to SSH into the experiment instances."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "wireguard_ingress_cidrs" {
  description = "CIDR ranges that are allowed to reach the WireGuard UDP port."
  type        = list(string)
  default     = []
}

variable "wireguard_udp_port" {
  description = "WireGuard UDP port exposed on the experiment instances."
  type        = number
  default     = 51820
}

variable "default_user_data" {
  description = "User data script that is injected into every instance unless overridden."
  type        = string
  default     = ""
}

variable "root_volume_size_gb" {
  description = "Root EBS volume size in GiB for all EC2 instances."
  type        = number
  default     = 16
}

variable "ec2_instances" {
  description = <<EOT
Declarative description of the EC2 instances generated from the GraphML experiment configuration.
Keys should be unique node identifiers (e.g. graph node ids) so the CLI can reconcile changes.
EOT
  type = map(object({
    ami                         = optional(string)
    instance_type               = string
    private_ip                  = optional(string)
    subnet_id                   = optional(string)
    key_name                    = optional(string)
    associate_public_ip_address = optional(bool)
    security_group_ids          = optional(list(string))
    user_data                   = optional(string)
    iam_instance_profile        = optional(string)
    tags                        = optional(map(string))
  }))
  default = {}
}
