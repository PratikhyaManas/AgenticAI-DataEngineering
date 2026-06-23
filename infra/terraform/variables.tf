# ==============================================================
# Terraform Variables
# Agent: CI/CD & DevOps Agent
# ==============================================================

variable "environment" {
  type        = string
  description = "Deployment environment (dev | test | prod)"
  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be dev, test, or prod"
  }
}

variable "project_name" {
  type        = string
  description = "Project name prefix for all resources"
  default     = "lakehouse"
}

variable "location" {
  type        = string
  description = "Azure region for all resources"
  default     = "westeurope"
}

variable "cost_center" {
  type        = string
  description = "Cost center tag for billing"
  default     = "data-platform"
}

variable "vnet_address_space" {
  type        = string
  description = "VNet address space CIDR"
  default     = "10.100.0.0/16"
}

variable "databricks_public_subnet_cidr" {
  type        = string
  description = "Public subnet CIDR for Databricks"
  default     = "10.100.1.0/24"
}

variable "databricks_private_subnet_cidr" {
  type        = string
  description = "Private subnet CIDR for Databricks"
  default     = "10.100.2.0/24"
}

variable "log_retention_days" {
  type        = number
  description = "Log Analytics retention in days"
  default     = 90
  validation {
    condition     = var.log_retention_days >= 30
    error_message = "Log retention must be at least 30 days for compliance"
  }
}

variable "databricks_cluster_policy" {
  type = object({
    autotermination_minutes = number
    max_workers             = number
    allowed_node_types      = list(string)
  })
  default = {
    autotermination_minutes = 30
    max_workers             = 20
    allowed_node_types      = ["Standard_DS3_v2", "Standard_DS4_v2", "Standard_DS5_v2"]
  }
  description = "Cluster policy constraints"
}
