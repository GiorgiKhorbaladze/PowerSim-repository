from .vre import PassiveInjectionComponent


class RunOfRiverComponent(PassiveInjectionComponent):
    """Legacy simplified CF availability; intentionally no water balance."""
    kind = "hydro_ror"

    def profile_value(self, context, index):
        key = self.profile_key()
        return float(context.profiles[key][index]) if key else float(self.asset.get("cf", 0.65))

