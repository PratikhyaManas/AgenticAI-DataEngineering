# ==============================================================
# Terraform — Root Module
# Agent: CI/CD & DevOps Agent
#
# Provisions the complete Azure Lakehouse infrastructure:
#   - Resource Groups
#   - ADLS Gen2 (multi-zone storage)
#   - Azure Databricks Workspace (Premium SKU)
#   - Azure Key Vault
#   - Azure Monitor / Log Analytics
#   - Event Hubs Namespace
#   - Azure Data Factory (for Self-Hosted IR)
#   - Networking (VNet injection for Databricks)
# ==============================================================

terraform {
  required_version = ">= 1.8.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.110"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.47"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "azurerm" {
    # Configured via backend-config in CI/CD pipeline
    # resource_group_name  = "rg-lakehouse-tfstate"
    # storage_account_name = "stlakehousetestate"
    # container_name       = "tfstate"
    # key                  = "<env>/terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
  }
}

provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
  # Authentication via Azure CLI / Service Principal in CI
}

# ---------------------------------------------------------------------------
# Data sources
# ---------------------------------------------------------------------------
data "azurerm_client_config" "current" {}

# ---------------------------------------------------------------------------
# Random suffix for globally unique resource names
# ---------------------------------------------------------------------------
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  suffix       = random_string.suffix.result
  name_prefix  = "${var.project_name}-${var.environment}"
  tags = {
    environment = var.environment
    project     = var.project_name
    managed_by  = "terraform"
    cost_center = var.cost_center
  }
}

# ---------------------------------------------------------------------------
# Resource Group
# ---------------------------------------------------------------------------
resource "azurerm_resource_group" "lakehouse" {
  name     = "rg-${local.name_prefix}-lakehouse"
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------------------
# Virtual Network (for Databricks VNet injection)
# ---------------------------------------------------------------------------
resource "azurerm_virtual_network" "lakehouse" {
  name                = "vnet-${local.name_prefix}"
  location            = azurerm_resource_group.lakehouse.location
  resource_group_name = azurerm_resource_group.lakehouse.name
  address_space       = [var.vnet_address_space]
  tags                = local.tags
}

resource "azurerm_subnet" "databricks_public" {
  name                 = "snet-databricks-public"
  resource_group_name  = azurerm_resource_group.lakehouse.name
  virtual_network_name = azurerm_virtual_network.lakehouse.name
  address_prefixes     = [var.databricks_public_subnet_cidr]

  delegation {
    name = "databricks-delegation"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_subnet" "databricks_private" {
  name                 = "snet-databricks-private"
  resource_group_name  = azurerm_resource_group.lakehouse.name
  virtual_network_name = azurerm_virtual_network.lakehouse.name
  address_prefixes     = [var.databricks_private_subnet_cidr]

  delegation {
    name = "databricks-delegation"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

# ---------------------------------------------------------------------------
# Network Security Groups (Databricks-required NSG rules)
# ---------------------------------------------------------------------------
resource "azurerm_network_security_group" "databricks" {
  name                = "nsg-databricks-${local.name_prefix}"
  location            = azurerm_resource_group.lakehouse.location
  resource_group_name = azurerm_resource_group.lakehouse.name
  tags                = local.tags
}

resource "azurerm_subnet_network_security_group_association" "public" {
  subnet_id                 = azurerm_subnet.databricks_public.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

resource "azurerm_subnet_network_security_group_association" "private" {
  subnet_id                 = azurerm_subnet.databricks_private.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

# ---------------------------------------------------------------------------
# ADLS Gen2 Storage Account
# ---------------------------------------------------------------------------
resource "azurerm_storage_account" "lakehouse" {
  name                     = "adls${local.name_prefix}${local.suffix}"
  resource_group_name      = azurerm_resource_group.lakehouse.name
  location                 = azurerm_resource_group.lakehouse.location
  account_tier             = "Standard"
  account_replication_type = var.environment == "prod" ? "GRS" : "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true        # Required for ADLS Gen2

  min_tls_version          = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false  # Disable shared key; use AAD-only auth

  blob_properties {
    delete_retention_policy {
      days = 30
    }
    versioning_enabled = true
  }

  tags = local.tags
}

# Containers (zones)
resource "azurerm_storage_container" "zones" {
  for_each             = toset(["landing", "raw", "curated", "gold", "quarantine", "sandbox"])
  name                 = each.value
  storage_account_name = azurerm_storage_account.lakehouse.name
}

# ---------------------------------------------------------------------------
# Azure Databricks Workspace (Premium — required for Unity Catalog)
# ---------------------------------------------------------------------------
resource "azurerm_databricks_workspace" "this" {
  name                = "dbw-${local.name_prefix}"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location
  sku                 = "premium"

  custom_parameters {
    no_public_ip                                         = true
    virtual_network_id                                   = azurerm_virtual_network.lakehouse.id
    public_subnet_name                                   = azurerm_subnet.databricks_public.name
    private_subnet_name                                  = azurerm_subnet.databricks_private.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
  }

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Azure Key Vault
# ---------------------------------------------------------------------------
resource "azurerm_key_vault" "lakehouse" {
  name                       = "kv-${local.name_prefix}-${local.suffix}"
  location                   = azurerm_resource_group.lakehouse.location
  resource_group_name        = azurerm_resource_group.lakehouse.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 90
  purge_protection_enabled   = true
  enable_rbac_authorization  = true    # Use RBAC instead of vault access policies

  network_acls {
    default_action             = "Deny"
    bypass                     = "AzureServices"
    virtual_network_subnet_ids = [
      azurerm_subnet.databricks_public.id,
      azurerm_subnet.databricks_private.id,
    ]
  }

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Log Analytics Workspace
# ---------------------------------------------------------------------------
resource "azurerm_log_analytics_workspace" "lakehouse" {
  name                = "law-${local.name_prefix}"
  location            = azurerm_resource_group.lakehouse.location
  resource_group_name = azurerm_resource_group.lakehouse.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days
  tags                = local.tags
}

# ---------------------------------------------------------------------------
# Application Insights (linked to Log Analytics)
# ---------------------------------------------------------------------------
resource "azurerm_application_insights" "lakehouse" {
  name                = "appi-${local.name_prefix}"
  location            = azurerm_resource_group.lakehouse.location
  resource_group_name = azurerm_resource_group.lakehouse.name
  workspace_id        = azurerm_log_analytics_workspace.lakehouse.id
  application_type    = "other"
  tags                = local.tags
}

# Diagnostic settings: Databricks → Log Analytics
resource "azurerm_monitor_diagnostic_setting" "databricks" {
  name               = "diag-databricks-${local.name_prefix}"
  target_resource_id = azurerm_databricks_workspace.this.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.lakehouse.id

  dynamic "enabled_log" {
    for_each = ["clusters", "jobs", "notebook", "sql", "genie", "globalInitScripts",
                "iamRole", "mlflowExperiment", "mlflowAcledArtifact",
                "databricksUI", "secrets", "dataAccess"]
    content {
      category = enabled_log.value
    }
  }
}

# ---------------------------------------------------------------------------
# Event Hubs Namespace
# ---------------------------------------------------------------------------
resource "azurerm_eventhub_namespace" "lakehouse" {
  name                = "evhns-${local.name_prefix}"
  location            = azurerm_resource_group.lakehouse.location
  resource_group_name = azurerm_resource_group.lakehouse.name
  sku                 = "Standard"
  capacity            = 2
  auto_inflate_enabled     = true
  maximum_throughput_units = 10
  tags = local.tags
}

resource "azurerm_eventhub" "sales_events" {
  name                = "sales-events"
  namespace_name      = azurerm_eventhub_namespace.lakehouse.name
  resource_group_name = azurerm_resource_group.lakehouse.name
  partition_count     = 8
  message_retention   = 7
}

# ---------------------------------------------------------------------------
# Azure Data Factory (for Self-Hosted IR)
# ---------------------------------------------------------------------------
resource "azurerm_data_factory" "lakehouse" {
  name                = "adf-${local.name_prefix}"
  location            = azurerm_resource_group.lakehouse.location
  resource_group_name = azurerm_resource_group.lakehouse.name
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_data_factory_integration_runtime_self_hosted" "onprem" {
  name            = "ir-onprem-sqlserver"
  data_factory_id = azurerm_data_factory.lakehouse.id
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
output "databricks_workspace_url" {
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
  description = "Databricks workspace URL"
}

output "storage_account_name" {
  value       = azurerm_storage_account.lakehouse.name
  description = "ADLS Gen2 storage account name"
}

output "key_vault_uri" {
  value       = azurerm_key_vault.lakehouse.vault_uri
  description = "Azure Key Vault URI"
}

output "log_analytics_workspace_id" {
  value       = azurerm_log_analytics_workspace.lakehouse.id
  description = "Log Analytics Workspace Resource ID"
}

output "app_insights_instrumentation_key" {
  value       = azurerm_application_insights.lakehouse.instrumentation_key
  sensitive   = true
  description = "Application Insights Instrumentation Key (stored in Key Vault)"
}

output "eventhub_namespace_name" {
  value       = azurerm_eventhub_namespace.lakehouse.name
  description = "Event Hubs Namespace name"
}
