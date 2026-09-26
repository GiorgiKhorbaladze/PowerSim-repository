from .results import ComponentQAMetadata
from .vre import PassiveInjectionComponent


class ExchangeComponent(PassiveInjectionComponent):
    """Unidirectional import injection (future exchange directions use new ports)."""
    kind = "import"

    def profile_key(self):
        key = self.asset.get("pmax_profile")
        return key if isinstance(key, str) else None

    def installed_capacity(self):
        value = self.asset.get("pmax_profile", self.asset.get("pmax", 0))
        return float(value) if not isinstance(value, str) else float(self.asset.get("pmax", 0) or 0)

    def available_mw(self, context, index):
        from .vre import maintenance_factor, temperature_factor
        key = self.profile_key()
        base = float(context.profiles[key][index]) if key else self.installed_capacity()
        hour = int(context.offset_hours + round(index * context.duration_hours))
        return base * maintenance_factor(self.asset, hour) * temperature_factor(self.asset, context.profiles, index)

    def qa_spec(self):
        return ComponentQAMetadata(self.kind, ("import_bound", "finite_nonnegative"))

