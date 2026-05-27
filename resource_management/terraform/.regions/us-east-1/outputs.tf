output "vpc_id" {
  description = "ID of the experiment VPC."
  value       = aws_vpc.experiments.id
}

output "public_subnet_id" {
  description = "ID of the default public subnet that instances use unless overridden."
  value       = aws_subnet.public.id
}

output "experiment_security_group_id" {
  description = "Security group that permits SSH and WireGuard traffic."
  value       = aws_security_group.experiment.id
}

output "instances" {
  description = "Connection metadata for every EC2 instance. Keys align with the GraphML node ids."
  value = {
    for name, instance in aws_instance.nodes :
    name => {
      id         = instance.id
      arn        = instance.arn
      public_ip  = instance.public_ip
      private_ip = instance.private_ip
      az         = instance.availability_zone
    }
  }
}

output "wireguard_udp_port" {
  description = "WireGuard port exposed through the shared security group."
  value       = var.wireguard_udp_port
}
