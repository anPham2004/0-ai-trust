provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "0-ai-trust"
      ManagedBy = "terraform"
      Lifetime  = "three-weeks"
    }
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_vpc" "g3" {
  cidr_block           = "10.30.0.0/24"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "g3-source-vpc" }
}

resource "aws_internet_gateway" "g3" {
  vpc_id = aws_vpc.g3.id
  tags   = { Name = "g3-source-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.g3.id
  cidr_block              = "10.30.0.0/26"
  map_public_ip_on_launch = true
  availability_zone       = "${var.region}a"
  tags                    = { Name = "g3-source-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.g3.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.g3.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "source" {
  name        = "g3-source-admin"
  description = "Restricted access to the source simulator"
  vpc_id      = aws_vpc.g3.id

  ingress {
    description = "SSH from administrator"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.admin_cidrs
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

data "aws_s3_bucket" "landing" { bucket = var.landing_bucket_name }

resource "aws_iam_role" "source" {
  name_prefix = "g3-source-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "source_landing" {
  role = aws_iam_role.source.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = data.aws_s3_bucket.landing.arn
        Condition = { StringLike = { "s3:prefix" = [
          "g3/bootstrap/*",
          "g3/source/*",
          "g3/0-ai-trust/bronze/landing/*"
        ] } }
      },
      {
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${data.aws_s3_bucket.landing.arn}/g3/bootstrap/*",
          "${data.aws_s3_bucket.landing.arn}/g3/source/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "${data.aws_s3_bucket.landing.arn}/g3/0-ai-trust/bronze/landing/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "${data.aws_s3_bucket.landing.arn}/g3/source/nab/batch/*"
      },
      {
        Effect   = "Allow"
        Action   = ["freetier:GetAccountPlanState"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.source.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "source" {
  role = aws_iam_role.source.name
}

resource "aws_key_pair" "source" {
  key_name   = "g3-assignment"
  public_key = file(pathexpand(var.public_key_path))
}

resource "aws_instance" "source" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.source.id]
  associate_public_ip_address = true
  key_name                    = aws_key_pair.source.key_name
  iam_instance_profile        = aws_iam_instance_profile.source.name
  monitoring                  = false

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    encrypted             = true
    delete_on_termination = true
  }

  user_data = <<-BASH
    #!/usr/bin/env bash
    set -euo pipefail
    apt-get update
    apt-get install -y ca-certificates curl unzip
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    systemctl enable --now docker
    mkdir -p /opt/0-ai-trust
    chmod 755 /opt/0-ai-trust
  BASH

  credit_specification { cpu_credits = "standard" }
  lifecycle {
    # Preserve the stateful source simulator. AWS reports public-IP association
    # drift while a public instance is stopped, and user_data is bootstrap-only.
    ignore_changes = [ami, associate_public_ip_address, user_data]
  }
  tags = { Name = "0-ai-trust-source-simulator" }
}

resource "aws_sns_topic" "credit_alerts" {
  name = "zero-ai-trust-credit-alerts"
}

resource "aws_sns_topic_subscription" "credit_email" {
  topic_arn = aws_sns_topic.credit_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_iam_role_policy" "credit_alert" {
  role = aws_iam_role.source.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sns:Publish"]
      Resource = aws_sns_topic.credit_alerts.arn
    }]
  })
}
