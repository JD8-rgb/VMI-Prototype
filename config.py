"""
config.py
---------
Single PlantConfig dataclass for all plant-specific business constants.

The algorithm modules (alerts.py, plan_orders.py, read_schedule.py)
historically declared these as module-level globals, which made
multi-customer / multi-plant support impossible without forking the
codebase or monkey-patching.

Now: every algorithm function accepts an optional `cfg: PlantConfig`
parameter that defaults to DEFAULT_CONFIG. Existing callers see no
change. The enterprise team can swap in per-customer configs without
touching algorithm code:

    from config import PlantConfig
    customer_a_cfg = PlantConfig(
        lead_time_hours=72,
        delivery_slots=(7, 9, 15),
        ...
    )
    alerts.check_lead_time(state, "Product U", cfg=customer_a_cfg)

Backward compatibility: the algorithm modules also re-export the old
module-level constants (LEAD_TIME_HOURS, etc.), wired to
DEFAULT_CONFIG fields, so any code still doing `from alerts import
LEAD_TIME_HOURS` continues to work. Those re-exports are deprecated
but kept indefinitely — removing them is its own pass.
"""

from dataclasses import dataclass, field
from typing import Tuple


@dataclass(frozen=True)
class PlantConfig:
    # ── Alerts ────────────────────────────────────────────────────────────
    safety_stock_lbs:           float = 10_000
    """Combined product level below this = safety-stock alert."""

    lead_time_hours:            float = 48
    """Truck must be planned at least this far in advance, AND lead-time
    alert horizon for inbound coverage check."""

    late_truck_hours:           float = 3
    """Alert if a scheduled truck is this many hours past its arrival."""

    projection_window_hours:    float = 168
    """How far run_projection looks ahead (default = 7 days)."""

    plant_state_mismatch_hours: float = 3
    """Alert if plant running/idle state contradicts the schedule for
    longer than this (e.g. running when scheduled idle)."""

    # ── Parser ────────────────────────────────────────────────────────────
    plant_max_hours: float = 118
    """Parser sanity cap. Any single window or total weekly run that
    exceeds this is treated as quoted-history leakage and forced low."""

    # ── Planner ───────────────────────────────────────────────────────────
    delivery_slots: Tuple[int, ...] = (6, 8, 14)
    """Allowed delivery arrival hours (24-hour clock)."""

    # ── Calendar ──────────────────────────────────────────────────────────
    plant_holidays: Tuple[str, ...] = ()
    """ISO-format date strings ("YYYY-MM-DD") on which the plant does
    not run, regardless of what the run-schedule windows say.
    is_running_at returns False on these dates and the planner's slot
    enumerator skips them. Empty tuple = no holidays (matches the demo
    behavior exactly).

    Stored as strings rather than `date` objects so the dataclass
    remains JSON-serializable through the customer config file."""

    # ── Order numbering ───────────────────────────────────────────────────
    sap_order_format: str = "SAP{n:05d}"
    """Python format string for new SAP order numbers. Must contain a
    single `{n}` placeholder that takes an integer. The default
    "SAP{n:05d}" produces "SAP90001"-style strings. Customers using a
    different ERP scheme (e.g. 10-digit numerics "{n:010d}", or a
    different prefix "ORD-{n:06d}") override this field."""

    sap_order_seed: int = 90_001
    """Starting integer when no prior SAP numbers exist on the system.
    Once `sap_history` is populated the seed is irrelevant — the next
    number is always max(existing) + 1."""

    # ── Reorder target curve ──────────────────────────────────────────────
    # Linear interpolation between (low_run_hours, low_lbs) and
    # (high_run_hours, high_lbs). Below low → low_lbs flat. Above high →
    # high_lbs flat.
    target_low_run_hours:  float = 28
    target_high_run_hours: float = 118
    target_low_lbs:        float = 15_000
    target_high_lbs:       float = 27_000

    def target_for_week(self, week_run_hours: float) -> float:
        """Linear-interpolated reorder target given scheduled weekly run hours."""
        if week_run_hours <= self.target_low_run_hours:
            return self.target_low_lbs
        if week_run_hours >= self.target_high_run_hours:
            return self.target_high_lbs
        span_hours = self.target_high_run_hours - self.target_low_run_hours
        span_lbs   = self.target_high_lbs       - self.target_low_lbs
        fraction   = (week_run_hours - self.target_low_run_hours) / span_hours
        return self.target_low_lbs + fraction * span_lbs

    def __post_init__(self) -> None:
        # ── sap_order_format must contain a {n} placeholder ──────────────
        # Without it, every truck would receive the same SAP string and
        # collide on sap_history dedup. Validate at construction so a
        # misconfigured customer file fails fast rather than silently
        # shipping duplicate order numbers.
        try:
            sample = self.sap_order_format.format(n=1)
        except (IndexError, KeyError, ValueError) as e:
            raise ValueError(
                f"PlantConfig.sap_order_format {self.sap_order_format!r} "
                f"is not a valid Python format string: {e}"
            ) from None
        if sample == self.sap_order_format.format(n=2):
            raise ValueError(
                f"PlantConfig.sap_order_format {self.sap_order_format!r} "
                f"does not include a {{n}} placeholder — every order would "
                f"receive the same string. Use e.g. 'SAP{{n:05d}}'."
            )

        # ── target curve must be non-degenerate ──────────────────────────
        if self.target_low_run_hours >= self.target_high_run_hours:
            raise ValueError(
                f"PlantConfig: target_low_run_hours "
                f"({self.target_low_run_hours}) must be < "
                f"target_high_run_hours ({self.target_high_run_hours})."
            )
        # The reorder-target curve must be non-decreasing in lbs as run
        # hours increase. A heavier-run week needing LESS inventory is
        # nonsense and would produce inverted interpolation.
        if self.target_low_lbs > self.target_high_lbs:
            raise ValueError(
                f"PlantConfig: target_low_lbs ({self.target_low_lbs}) "
                f"must be <= target_high_lbs ({self.target_high_lbs}). "
                f"A higher-utilization week cannot need LESS reorder target."
            )

        # ── Strictly-positive numeric fields ─────────────────────────────
        # These are all hour / lbs counts where zero or negative would
        # silently break alerts (zero lead_time disables the lead-time
        # check; negative safety_stock fires false positives every tick;
        # zero projection_window means run_projection sees no future).
        for name in ("safety_stock_lbs", "lead_time_hours",
                     "late_truck_hours", "projection_window_hours",
                     "plant_state_mismatch_hours", "plant_max_hours"):
            value = getattr(self, name)
            if value <= 0:
                raise ValueError(
                    f"PlantConfig.{name} must be > 0, got {value!r}."
                )

        # ── delivery_slots must be non-empty and within 0..23 ────────────
        if not self.delivery_slots:
            raise ValueError(
                "PlantConfig.delivery_slots must be non-empty — an empty "
                "slot list means the planner can never propose a truck."
            )
        for h in self.delivery_slots:
            if not (0 <= h <= 23):
                raise ValueError(
                    f"PlantConfig.delivery_slots hour {h!r} out of range "
                    f"0..23 (24-hour clock)."
                )

        # ── sap_order_seed must be non-negative ──────────────────────────
        # Negative seeds work mechanically but produce SAP strings like
        # "SAP-0001" which collide with the suffix-digit regex used by
        # _next_sap to find the next number.
        if self.sap_order_seed < 0:
            raise ValueError(
                f"PlantConfig.sap_order_seed must be >= 0, "
                f"got {self.sap_order_seed!r}."
            )


# The default config matches the historic module-globals exactly, so
# existing callers (which don't pass `cfg=`) see no behavior change.
DEFAULT_CONFIG = PlantConfig()
