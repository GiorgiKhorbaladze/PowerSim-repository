from .vre import PassiveInjectionComponent


class RunOfRiverComponent(PassiveInjectionComponent):
    """Legacy simplified CF availability; intentionally no water balance."""

    kind = "hydro_ror"

    def default_profile_value(self) -> float:
        return float(self.asset.get("cf", 0.65))
