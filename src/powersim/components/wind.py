from .vre import MONTH_END_HOURS, PassiveInjectionComponent
from powersim.contracts import IssueSeverity, ValidationIssue


class WindComponent(PassiveInjectionComponent):
    kind = "wind"

    def validate(self, context):
        issues = super().validate(context)
        try:
            rate = float(self.asset.get("degradation_rate_per_year", 0) or 0)
        except (TypeError, ValueError):
            rate = float("nan")
        if not 0 <= rate < 1:
            issue = ValidationIssue(code="invalid_degradation", severity=IssueSeverity.ERROR,
                                    path=f"assets.{self.asset_id}.degradation_rate_per_year",
                                    message="degradation_rate_per_year must be numeric and in [0, 1)")
            issues.append(issue)
            context.validation_issues.append(issue)
        return issues

    def installed_capacity(self):
        return float(self.asset.get("pmax_installed", self.asset.get("pmax", 0)) or 0)

    def available_mw(self, context, index):
        cf = max(0.0, self.profile_value(context, index))
        clipped = min(1.0, cf * float(self.asset.get("dc_ac_ratio", 1) or 1))
        base = self.installed_capacity() * clipped * float(self.asset.get("inverter_efficiency", 1) or 1)
        base *= float(self.asset.get("_degradation_factor", 1) or 1)
        base *= max(0.0, 1.0 - float(self.asset.get("wake_loss_frac", 0) or 0))
        hour = int(context.offset_hours + round(index * context.duration_hours))
        monthly = self.asset.get("monthly_availability_factor")
        if isinstance(monthly, list) and len(monthly) == 12:
            month = next((i for i, end in enumerate(MONTH_END_HOURS) if hour < end), 11)
            if monthly[month] is not None: base *= max(0.0, float(monthly[month]))
        if self.asset.get("air_density_correction"):
            series = context.profiles.get(self.asset.get("temp_profile_key"))
            if isinstance(series, list) and index < len(series) and 273.15 + float(series[index]) > 1e-3:
                base *= (273.15 + float(self.asset.get("density_ref_temp_c", 15) or 15)) / (273.15 + float(series[index]))
        from .vre import maintenance_factor, temperature_factor
        return base * maintenance_factor(self.asset, hour) * temperature_factor(self.asset, context.profiles, index)
