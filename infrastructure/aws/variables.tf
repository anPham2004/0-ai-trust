variable "region" {
  description = "Sydney keeps the simulation close to the Australian scenario."
  type        = string
  default     = "ap-southeast-2"
}

variable "admin_cidrs" {
  description = "Restricted public administrator CIDRs; every entry must be a host /32."
  type        = list(string)
  validation {
    condition = length(var.admin_cidrs) > 0 && alltrue([
      for cidr in var.admin_cidrs : can(cidrhost(cidr, 0)) && endswith(cidr, "/32")
    ])
    error_message = "Use one or more restricted /32 administrator CIDRs."
  }
}

variable "public_key_path" {
  description = "Local OpenSSH public key imported as the assignment EC2 key pair."
  type        = string
  default     = "~/.ssh/g3-assignment.pub"
}

variable "instance_type" {
  description = "4 GiB is the minimum practical size for Kafka, Connect, Postgres and the exporter."
  type        = string
  default     = "c7i-flex.large"
}

variable "landing_bucket_name" {
  description = "Existing S3 bucket already connected to Databricks through Unity Catalog."
  type        = string
  default     = "g3-assignment"
}

variable "alert_email" {
  description = "Email address that receives the remaining-credit alert."
  type        = string
}
