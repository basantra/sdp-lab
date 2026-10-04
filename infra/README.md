# infra/

Terraform for OCI resources used by this lab (compartment, network, OCI Data
Flow / OKE or compute for Spark, IAM policies, object storage buckets, etc).

Conventions:

- One `.tf` file per resource concern (`network.tf`, `compute.tf`, `iam.tf`, ...), not one giant `main.tf`.
- Every `terraform` invocation that changes state runs through `run_logged` from [`env/log.sh`](../env/log.sh) so it lands in `runs/<date>/commands.log`.
- Provider versions are pinned in `versions.tf` and the resulting `.terraform.lock.hcl` is committed. The provider version itself still must be read from the registry's current page and recorded in [`env/versions.lock`](../env/versions.lock), not typed from memory.
- `*.tfstate` and `.terraform/` are gitignored (see [`../.gitignore`](../.gitignore)). State is per-run, not shared — remote state backend is a later decision, not assumed here.

Nothing is provisioned yet. No `.tf` files exist until the first infra step in [`RUNBOOK.md`](../RUNBOOK.md).
