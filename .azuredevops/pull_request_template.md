## What does this PR do?
<!-- One-sentence summary of the change -->


## Related work item / issue
<!-- Link to the Azure DevOps work item or GitHub issue -->
AB#

## Type of change
<!-- Put an x in the relevant box -->
- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that changes existing behaviour)
- [ ] Config / infra change (YAML configs, Terraform, Databricks bundle)
- [ ] Refactor / optimisation (no functional change)
- [ ] Test / CI change

## Changes made
<!-- List the specific files and what changed in each -->
-
-
-

## How to test
<!-- Step-by-step instructions to verify the change works -->
1. `make seed-local` to prepare local Delta tables
2. `make test` — all tests must pass
3. If API change: `make serve-api` and hit `http://localhost:8000/docs`

## Pre-merge checklist
<!-- All boxes must be checked before requesting review -->
- [ ] `make test` passes locally (unit + integration)
- [ ] `make lint` passes with zero errors
- [ ] `make security` shows no new HIGH or CRITICAL findings
- [ ] `make bundle-validate` passes (if `databricks.yml` was changed)
- [ ] New code has corresponding tests (coverage ≥ 70 %)
- [ ] No secrets, credentials, or PII committed (`.env` not staged)
- [ ] `configs/` YAML updated if a new source/target was added
- [ ] `requirements.txt` updated if a new Python package was added

## Schema / breaking changes
<!-- Fill in if Delta table schemas, API contracts, or config YAML changed -->
- Delta tables affected:
- API endpoints changed:
- Config keys added/removed:

## Rollback plan
<!-- How to revert if this breaks production -->
- Delta time-travel: `RESTORE TABLE … TO VERSION AS OF N`
- Databricks bundle: re-deploy previous bundle version from the last successful CD run
- API: roll back the previous Docker image / Azure Container App revision
