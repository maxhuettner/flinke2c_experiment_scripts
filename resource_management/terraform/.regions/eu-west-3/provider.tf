provider "aws" {
  region = var.aws_region
  profile = "terraform"

  default_tags {
    tags = var.default_tags
  }
}
