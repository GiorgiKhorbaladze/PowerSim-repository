from typing import Any
from powersim.version import CONTRACT_VERSION
from .errors import ContractMigrationError


def _major(version: str) -> int:
    try:
        return int(version.split(".", 1)[0])
    except (AttributeError, ValueError) as exc:
        raise ContractMigrationError(f"invalid contract version: {version!r}") from exc


def migrate_contract(data: dict[str, Any], from_version: str, to_version: str = CONTRACT_VERSION):
    """Migrate known contracts. Legacy schemas are delegated to the loss-preserving adapter."""
    if to_version != CONTRACT_VERSION or _major(to_version) != 1:
        raise ContractMigrationError(f"unsupported target contract version: {to_version}")
    if from_version in {"1.0", "1.1", "1.2", "1.3", "1.4", "1.5"} and "time_index" in data:
        from powersim.validation.legacy_adapter import adapt_legacy_input
        return adapt_legacy_input(data)
    if _major(from_version) != 1:
        raise ContractMigrationError(f"unsupported source contract major version: {from_version}")
    if from_version != CONTRACT_VERSION:
        raise ContractMigrationError(f"no safe migration registered from {from_version} to {to_version}")
    return data.copy()

