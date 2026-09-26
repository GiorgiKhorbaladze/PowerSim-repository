from powersim.contracts import IssueSeverity

from .context import make_validation_issue
from .vre import MONTH_END_HOURS, PassiveInjectionComponent, maintenance_factor, temperature_factor


class WindComponent(PassiveInjectionComponent):
    kind = "wind"

    def validate(self, context):
        issues = super().validate(context)
        try:
            rate = float(self.asset.get("degradation_rate_per_year", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            rate = float("nan")
        if not 0 <= rate < 1:
            issue = make_validation_issue(
                self.asset_id,
                "invalid_degradation",
                "degradation_rate_per_year must be numeric and in [0, 1)",
                field_name="degradation_rate_per_year",
                severity=IssueSeverity.ERROR,
            )
            issues.append(issue)
            context.validation_issues.append(issue)
        return issues

    def installed_capacity(self):
        return float(self.asset.get("pmax_installed", self.asset.get("pmax", 0)) or 0)

    def available_mw(self, context, index):
        cf = max(0.0, self.profile_value(context, index))
        clipped = min(1.0, cf * float(self.asset.get("dc_ac_ratio", 1) or 1))
        base = (
            self.installed_capacity()
            * clipped
            * float(self.asset.get("inverter_efficiency", 1) or 1)
        )
        base *= float(self.asset.get("_degradation_factor", 1) or 1)
        base *= max(0.0, 1.0 - float(self.asset.get("wake_loss_frac", 0) or 0))

        hour = int(context.offset_hours + round(index * context.duration_hours))
        monthly = self.asset.get("monthly_availability_factor")
        if isinstance(monthly, list) and len(monthly) == 12:
            # Match the legacy month selection, including the legacy fallback
            # to January if an hour falls outside the nominal one-year table.
            month = 0
            for candidate, end in enumerate(MONTH_END_HOURS):
                if hour < end:
                    month = candidate
                    break
            value = monthly[month]
            if value is not None:
                base *= max(0.0, float(value))

        if self.asset.get("air_density_correction"):
            series = context.profiles.get(self.asset.get("temp_profile_key"))
            if isinstance(series, list) and index < len(series):
                ambient = float(series[index])
                if 273.15 + ambient > 1e-3:
                    reference = float(self.asset.get("density_ref_temp_c", 15) or 15)
                    base *= (273.15 + reference) / (273.15 + ambient)

        return (
            base
            * maintenance_factor(self.asset, hour)
            * temperature_factor(self.asset, context.profiles, index)
        )
