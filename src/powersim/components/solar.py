from .wind import WindComponent


class SolarComponent(WindComponent):
    kind = "solar"

    def available_mw(self, context, index):
        from .vre import PassiveInjectionComponent
        cf = max(0.0, self.profile_value(context, index))
        clipped = min(1.0, cf * float(self.asset.get("dc_ac_ratio", 1) or 1))
        base = self.installed_capacity() * clipped * float(self.asset.get("inverter_efficiency", 1) or 1)
        base *= float(self.asset.get("_degradation_factor", 1) or 1)
        # Apply only the generic maintenance/temperature factors, not wind extras.
        from .vre import maintenance_factor, temperature_factor
        hour = int(context.offset_hours + round(index * context.duration_hours))
        return base * maintenance_factor(self.asset, hour) * temperature_factor(self.asset, context.profiles, index)

