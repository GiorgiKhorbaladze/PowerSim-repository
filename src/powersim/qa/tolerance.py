"""Single tolerance policy for independent QA."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TolerancePolicy:
    internal_absolute: float = 1e-6
    internal_relative: float = 1e-7
    persisted_absolute: float = 1e-3
    persisted_relative: float = 1e-6

    def limit(self, scale: float, *, persisted: bool = False) -> float:
        absolute = self.persisted_absolute if persisted else self.internal_absolute
        relative = self.persisted_relative if persisted else self.internal_relative
        return absolute + relative * abs(scale)


DEFAULT_TOLERANCES = TolerancePolicy()
