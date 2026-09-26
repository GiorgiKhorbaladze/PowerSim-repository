import math

from .results import ComponentQAMetadata
from .vre import PassiveInjectionComponent


class ExchangeComponent(PassiveInjectionComponent):
    """Unidirectional import injection (future exchange directions use new ports)."""
    kind = "import"

    def profile_key(self):
        key = self.asset.get("pmax_profile")
        return key if isinstance(key, str) else None

    def profile_field_name(self):
        return "pmax_profile"

    def installed_capacity(self):
        value = self.asset.get("pmax_profile", self.asset.get("pmax", 0))
        return float(value) if not isinstance(value, str) else float(self.asset.get("pmax", 0) or 0)

    def available_mw(self, context, index):
        from .vre import maintenance_factor, temperature_factor
        key = self.profile_key()
        series = context.profiles.get(key) if key else None
        use_profile = isinstance(series, list) and index < len(series)
        if use_profile:
            try:
                candidate = float(series[index])
                use_profile = math.isfinite(candidate)
            except (TypeError, ValueError, OverflowError):
                use_profile = False
        base = candidate if use_profile else float(self.asset.get("pmax", 0) or 0)
        hour = int(context.offset_hours + round(index * context.duration_hours))
        return base * maintenance_factor(self.asset, hour) * temperature_factor(self.asset, context.profiles, index)

    def qa_spec(self):
        return ComponentQAMetadata(self.kind, ("import_bound", "finite_nonnegative"))
