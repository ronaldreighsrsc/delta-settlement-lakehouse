"""TDD contract tests for Spec 004 T2: Terraform Medallion S3, IAM, Budget Guardrails, and security."""

from pathlib import Path
import re

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TF_DIR = PROJECT_ROOT / "infra" / "terraform"
ENV_DIR = TF_DIR / "environments"
GITIGNORE = PROJECT_ROOT / ".gitignore"


def _read_all_tf() -> str:
    return "\n".join(
        p.read_text(encoding="utf-8") for p in sorted(TF_DIR.glob("*.tf"))
    )


def test_terraform_directory_and_environment_templates_exist():
    required_files = [
        TF_DIR / "versions.tf",
        TF_DIR / "providers.tf",
        TF_DIR / "variables.tf",
        TF_DIR / "main.tf",
        TF_DIR / "outputs.tf",
        ENV_DIR / "local-docker.tfvars",
        ENV_DIR / "localstack.tfvars",
        ENV_DIR / "aws-free-tier.tfvars.example",
    ]
    missing = [str(p.relative_to(PROJECT_ROOT)) for p in required_files if not p.exists()]
    assert not missing, f"Missing Terraform IaC files: {missing}"


def test_terraform_declares_four_medallion_buckets_and_hardening():
    tf_text = _read_all_tf()

    for zone in ("bronze", "silver", "dlq", "gold"):
        assert zone in tf_text, f"Medallion zone '{zone}' missing from Terraform configuration"

    assert 'resource "aws_s3_bucket"' in tf_text
    assert 'resource "aws_s3_bucket_public_access_block"' in tf_text
    for flag in (
        "block_public_acls",
        "block_public_policy",
        "ignore_public_acls",
        "restrict_public_buckets",
    ):
        assert re.search(rf"{flag}\s*=\s*true", tf_text), f"{flag} must be set to true"

    assert 'resource "aws_s3_bucket_versioning"' in tf_text
    assert 'status = "Enabled"' in tf_text or 'status      = "Enabled"' in tf_text
    assert 'resource "aws_s3_bucket_lifecycle_configuration"' in tf_text
    assert "noncurrent_version_expiration" in tf_text


def test_terraform_declares_scoped_iam_and_toggleable_budget_guardrail():
    tf_text = _read_all_tf()

    assert 'resource "aws_iam_role"' in tf_text
    assert 'resource "aws_iam_policy"' in tf_text
    assert 'resource "aws_iam_role_policy_attachment"' in tf_text
    assert "s3:GetObject" in tf_text
    assert "s3:PutObject" in tf_text
    assert "s3:DeleteObject" in tf_text
    assert "s3:ListBucket" in tf_text
    assert 'Resource = "*"' not in tf_text and '"Resource": "*"' not in tf_text

    assert 'resource "aws_budgets_budget"' in tf_text
    assert "var.enable_budget_guard" in tf_text
    assert "MONTHLY" in tf_text
    assert "USD" in tf_text


def test_terraform_tfvars_and_gitignore_prevent_secret_or_state_leaks():
    local_docker = (ENV_DIR / "local-docker.tfvars").read_text(encoding="utf-8")
    localstack = (ENV_DIR / "localstack.tfvars").read_text(encoding="utf-8")
    aws_example = (ENV_DIR / "aws-free-tier.tfvars.example").read_text(encoding="utf-8")

    assert re.search(r"use_local_endpoint\s*=\s*true", local_docker)
    assert re.search(r"enable_budget_guard\s*=\s*false", local_docker)
    assert re.search(r"use_local_endpoint\s*=\s*true", localstack)
    assert re.search(r"enable_budget_guard\s*=\s*false", localstack)

    assert re.search(r"use_local_endpoint\s*=\s*false", aws_example)
    assert re.search(r"enable_budget_guard\s*=\s*true", aws_example)

    for content in (local_docker, localstack, aws_example, _read_all_tf()):
        assert "AKIA" not in content, "Never hardcode real AWS access key prefixes in repo"

    gitignore_text = GITIGNORE.read_text(encoding="utf-8")
    assert ".terraform/" in gitignore_text
    assert "*.tfstate" in gitignore_text
    assert "*.tfvars" in gitignore_text
