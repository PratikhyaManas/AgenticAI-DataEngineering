"""
Azure Monitor Alerting Integration — Data Quality & Observability Agent
=======================================================================
Pushes DQ check results and pipeline metrics to Azure Monitor via
Application Insights custom events and metrics.

Authentication: Managed Identity (no secrets in code).
"""

import json
import logging
from datetime import datetime, timezone
from typing import List, Optional

logger = logging.getLogger(__name__)


def push_dq_metrics_to_app_insights(
    instrumentation_key: str,
    run_id: str,
    environment: str,
    layer: str,
    domain: str,
    entity: str,
    failed_checks: int,
    total_checks: int,
    failure_details: Optional[List[dict]] = None,
) -> None:
    """
    Send DQ run summary and individual check failures to Application Insights
    as custom events and custom metrics.

    In production, the instrumentation key is fetched from Key Vault.
    For Databricks, use the azure-monitor-opentelemetry-exporter library.
    """
    try:
        from applicationinsights import TelemetryClient
        tc = TelemetryClient(instrumentation_key)

        # Custom metric: overall failure rate
        failure_rate = failed_checks / total_checks if total_checks > 0 else 0.0
        tc.track_metric(
            "dq.failure_rate",
            failure_rate,
            properties={
                "run_id":      run_id,
                "environment": environment,
                "layer":       layer,
                "domain":      domain,
                "entity":      entity,
            }
        )
        tc.track_metric("dq.failed_checks",  failed_checks,  properties={"run_id": run_id})
        tc.track_metric("dq.total_checks",   total_checks,   properties={"run_id": run_id})

        # Custom event per DQ run
        tc.track_event(
            "DataQualityRun",
            properties={
                "run_id":        run_id,
                "environment":   environment,
                "layer":         layer,
                "domain":        domain,
                "entity":        entity,
                "status":        "FAILED" if failed_checks > 0 else "PASSED",
                "failed_checks": str(failed_checks),
                "total_checks":  str(total_checks),
                "run_ts":        datetime.now(timezone.utc).isoformat(),
            }
        )

        # Track individual failures as separate events for drill-down
        if failure_details:
            for failure in failure_details:
                tc.track_event("DataQualityCheckFailed", properties={
                    "run_id":      run_id,
                    "check_name":  failure.get("check_name"),
                    "check_type":  failure.get("check_type"),
                    "column":      failure.get("column_name"),
                    "actual":      str(failure.get("actual_value")),
                    "threshold":   str(failure.get("threshold")),
                    "layer":       layer,
                    "entity":      entity,
                })

        tc.flush()
        logger.info(f"[MONITOR] DQ metrics pushed to Application Insights. run_id={run_id}")

    except ImportError:
        logger.warning("[MONITOR] applicationinsights package not installed. Skipping telemetry push.")
    except Exception as exc:
        logger.error(f"[MONITOR] Failed to push metrics: {exc}")


def create_azure_monitor_alert_action_group_template() -> dict:
    """
    Returns an ARM template fragment defining an Azure Monitor Action Group
    with email + Teams webhook notifications for DQ failures.
    Deploy this via Terraform or Bicep in infra/.
    """
    return {
        "type": "Microsoft.Insights/actionGroups",
        "apiVersion": "2023-01-01",
        "name": "lakehouse-dq-alerts-ag",
        "location": "Global",
        "properties": {
            "groupShortName": "LH-DQ",
            "enabled": True,
            "emailReceivers": [
                {
                    "name": "DataEngTeam",
                    "emailAddress": "data-engineering@company.com",
                    "useCommonAlertSchema": True,
                }
            ],
            "webhookReceivers": [
                {
                    "name": "TeamsChannel",
                    "serviceUri": "<TEAMS_WEBHOOK_URL_FROM_KEY_VAULT>",
                    "useCommonAlertSchema": True,
                }
            ],
        }
    }


def create_azure_monitor_metric_alert_template(
    app_insights_resource_id: str,
    action_group_resource_id: str,
) -> dict:
    """
    Returns an ARM fragment for a metric alert that fires when
    dq.failure_rate > 0.05 for any 5-minute window.
    """
    return {
        "type": "Microsoft.Insights/metricAlerts",
        "apiVersion": "2018-03-01",
        "name": "dq-failure-rate-alert",
        "location": "Global",
        "properties": {
            "description": "Fires when DQ failure rate exceeds 5% in a 5-minute window",
            "severity": 2,
            "enabled": True,
            "scopes": [app_insights_resource_id],
            "evaluationFrequency": "PT5M",
            "windowSize": "PT5M",
            "criteria": {
                "odata.type": "Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria",
                "allOf": [
                    {
                        "name": "dq_failure_rate",
                        "metricName": "customMetrics/dq.failure_rate",
                        "operator": "GreaterThan",
                        "threshold": 0.05,
                        "timeAggregation": "Maximum",
                        "criterionType": "StaticThresholdCriterion",
                    }
                ]
            },
            "actions": [{"actionGroupId": action_group_resource_id}],
        }
    }


# ---------------------------------------------------------------------------
# Incident Playbook Helper
# ---------------------------------------------------------------------------

def create_devops_work_item_on_failure(
    organization_url: str,
    project: str,
    pat_secret_key: str,
    title: str,
    description: str,
    tags: str = "data-quality;incident",
    area_path: str = None,
) -> None:
    """
    Creates an Azure DevOps work item (Bug) automatically when DQ checks fail.
    PAT is retrieved from Azure Key Vault Databricks secret scope at runtime.

    organization_url: e.g., https://dev.azure.com/myorg
    project:          e.g., DataPlatform
    pat_secret_key:   key name in Databricks secret scope pointing to ADO PAT
    """
    try:
        from azure.devops.connection import Connection
        from msrest.authentication import BasicAuthentication

        pat = dbutils.secrets.get(scope="kv-lakehouse-scope", key=pat_secret_key)  # noqa: F821
        credentials = BasicAuthentication("", pat)
        connection  = Connection(base_url=organization_url, creds=credentials)
        wit_client  = connection.clients.get_work_item_tracking_client()

        patch_document = [
            {"op": "add", "path": "/fields/System.Title",       "value": title},
            {"op": "add", "path": "/fields/System.Description", "value": description},
            {"op": "add", "path": "/fields/System.Tags",        "value": tags},
            {"op": "add", "path": "/fields/Microsoft.VSTS.Common.Priority", "value": 1},
        ]
        if area_path:
            patch_document.append({"op": "add", "path": "/fields/System.AreaPath", "value": area_path})

        work_item = wit_client.create_work_item(patch_document, project, "Bug")
        logger.info(f"[ADO] Work item created: #{work_item.id} — {title}")

    except ImportError:
        logger.warning("[ADO] azure-devops SDK not installed. Skipping work item creation.")
    except Exception as exc:
        logger.error(f"[ADO] Failed to create work item: {exc}")
