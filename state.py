"""
state.py
--------
Typed dataclasses for the VMI prototype's domain.

Today the algorithm modules read raw `data["tanks"]["U-T1"]["current_level_lbs"]`
dicts loaded from `data.json`. That works but couples every algorithm to
the file's exact key names and shape — a schema migration requires
hunting every `info["heel_lbs"]` access pattern, and mypy can't catch
typos.

This module declares the domain types and provides round-trip
conversion to/from the existing JSON dict shape (no field renames, no
restructuring), so the algorithms can migrate one function at a time.
The enterprise team can also use these types directly when wrapping
the prototype's logic in a service layer.

Migration plan (NOT done in this commit):

  1. New code uses dataclasses where convenient.
  2. Existing algorithm functions keep accepting `data` dicts.
  3. As we touch each algorithm in subsequent passes, switch its
     signature to accept the dataclass and have callers convert at the
     edges via `PlantState.from_dict` / `.to_dict`.
  4. Once every algorithm is migrated, drop the dict-access code paths.

Today the helpers exist, are tested for round-trip preservation, and
are available — nothing in the demo path uses them yet.
"""

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Literal, Optional


# ── Leaf types ────────────────────────────────────────────────────────────────

TankStatus = Literal["draw", "standby", "receiving"]


@dataclass
class TankState:
    """One physical tank. Keyed in PlantState.tanks by tank name."""
    product: str
    current_level_lbs: float
    max_capacity_lbs: float
    heel_lbs: float
    status: TankStatus

    def to_dict(self) -> Dict[str, Any]:
        return {
            "product":           self.product,
            "current_level_lbs": self.current_level_lbs,
            "max_capacity_lbs":  self.max_capacity_lbs,
            "heel_lbs":          self.heel_lbs,
            "status":            self.status,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TankState":
        return cls(
            product           = d["product"],
            current_level_lbs = d["current_level_lbs"],
            max_capacity_lbs  = d["max_capacity_lbs"],
            heel_lbs          = d["heel_lbs"],
            status            = d["status"],
        )


@dataclass
class Truck:
    """One scheduled truck. sap_order may be None for planned-but-not-committed."""
    sap_order:        Optional[str]
    product:          str
    quantity_lbs:     float
    arrival_run_hour: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sap_order":        self.sap_order,
            "product":          self.product,
            "quantity_lbs":     self.quantity_lbs,
            "arrival_run_hour": self.arrival_run_hour,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Truck":
        return cls(
            sap_order        = d.get("sap_order"),
            product          = d["product"],
            quantity_lbs     = d["quantity_lbs"],
            arrival_run_hour = d["arrival_run_hour"],
        )


@dataclass
class RunWindow:
    """One scheduled plant-running window in run-hours."""
    start_hour: float
    end_hour:   float
    label:      str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "start_hour": self.start_hour,
            "end_hour":   self.end_hour,
            "label":      self.label,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RunWindow":
        return cls(
            start_hour = d["start_hour"],
            end_hour   = d["end_hour"],
            label      = d.get("label", ""),
        )


@dataclass
class ProductRate:
    lbs_per_hour: float

    def to_dict(self) -> Dict[str, Any]:
        return {"lbs_per_hour": self.lbs_per_hour}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ProductRate":
        return cls(lbs_per_hour=d["lbs_per_hour"])


# ── Top-level container ───────────────────────────────────────────────────────


@dataclass
class PlantState:
    """The entire mutable state of one plant. Round-trips with data.json."""

    # Required fields
    simulation_epoch:  str           # ISO datetime string, naive (today's contract)
    current_run_hour:  float
    tanks:             Dict[str, TankState]
    consumption_rates: Dict[str, ProductRate]
    truck_quantities:  Dict[str, float]
    scheduled_trucks:  List[Truck]    = field(default_factory=list)
    run_schedule:      List[RunWindow] = field(default_factory=list)

    # Optional / dedup / log fields — preserved as-is on round-trip.
    schedule_received_for_week:   Optional[str]       = None
    schedule_email_id:            Optional[str]       = None
    schedule_parse_issue:         Optional[str]       = None
    schedule_unreadable_alert_id: Optional[str]       = None
    schedule_alerted_ids:         List[str]           = field(default_factory=list)
    alerted_hashes:               List[str]           = field(default_factory=list)
    alert_log:                    List[Dict[str, Any]] = field(default_factory=list)
    sap_history:                  List[str]           = field(default_factory=list)
    plant_state_override:         Optional[Dict[str, Any]] = None

    # Operator-set runtime overrides (set via the Streamlit "VMI Controls"
    # panel). All persist week-to-week until the operator hits Reset.
    #
    # target_overrides : {"low": float, "high": float} or None.
    #     When set, replaces cfg.target_low_lbs / cfg.target_high_lbs
    #     in the per-week target curve. Bounded by cfg.tunable_*.
    # vmi_automation_enabled : True (default) or False.
    #     When False, the planner's auto-truck-ordering is suppressed
    #     and check_vmi_off fires a weekly Friday RED alert until the
    #     operator turns it back on.
    target_overrides:        Optional[Dict[str, float]] = None
    vmi_automation_enabled:  bool                       = True

    # Catch-all for any future fields added to data.json that this module
    # doesn't know about — preserved verbatim through the round-trip so a
    # newer-schema file isn't lossily mangled when an older code version
    # touches it.
    _extra: Dict[str, Any] = field(default_factory=dict)

    # ── Round-trip ──

    _KNOWN_KEYS = frozenset([
        "simulation_epoch", "current_run_hour",
        "tanks", "consumption_rates", "truck_quantities",
        "scheduled_trucks", "run_schedule",
        "schedule_received_for_week", "schedule_email_id",
        "schedule_parse_issue", "schedule_unreadable_alert_id",
        "schedule_alerted_ids", "alerted_hashes", "alert_log",
        "sap_history", "plant_state_override",
        "target_overrides", "vmi_automation_enabled",
    ])

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PlantState":
        return cls(
            simulation_epoch  = d["simulation_epoch"],
            current_run_hour  = d["current_run_hour"],
            tanks             = {k: TankState.from_dict(v)
                                 for k, v in d.get("tanks", {}).items()},
            consumption_rates = {k: ProductRate.from_dict(v)
                                 for k, v in d.get("consumption_rates", {}).items()},
            truck_quantities  = dict(d.get("truck_quantities", {})),
            scheduled_trucks  = [Truck.from_dict(t)
                                 for t in d.get("scheduled_trucks", [])],
            run_schedule      = [RunWindow.from_dict(w)
                                 for w in d.get("run_schedule", [])],
            schedule_received_for_week   = d.get("schedule_received_for_week"),
            schedule_email_id            = d.get("schedule_email_id"),
            schedule_parse_issue         = d.get("schedule_parse_issue"),
            schedule_unreadable_alert_id = d.get("schedule_unreadable_alert_id"),
            schedule_alerted_ids         = list(d.get("schedule_alerted_ids", [])),
            alerted_hashes               = list(d.get("alerted_hashes", [])),
            alert_log                    = list(d.get("alert_log", [])),
            sap_history                  = list(d.get("sap_history", [])),
            plant_state_override         = d.get("plant_state_override"),
            target_overrides             = d.get("target_overrides"),
            vmi_automation_enabled       = d.get("vmi_automation_enabled", True),
            _extra = {k: v for k, v in d.items() if k not in cls._KNOWN_KEYS},
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "simulation_epoch":  self.simulation_epoch,
            "current_run_hour":  self.current_run_hour,
            "tanks":             {k: v.to_dict() for k, v in self.tanks.items()},
            "consumption_rates": {k: v.to_dict() for k, v in self.consumption_rates.items()},
            "truck_quantities":  dict(self.truck_quantities),
            "scheduled_trucks":  [t.to_dict() for t in self.scheduled_trucks],
            "run_schedule":      [w.to_dict() for w in self.run_schedule],
            "schedule_received_for_week":   self.schedule_received_for_week,
            "schedule_email_id":            self.schedule_email_id,
            "schedule_parse_issue":         self.schedule_parse_issue,
            "schedule_unreadable_alert_id": self.schedule_unreadable_alert_id,
            "schedule_alerted_ids":         list(self.schedule_alerted_ids),
            "alerted_hashes":               list(self.alerted_hashes),
            "alert_log":                    list(self.alert_log),
            "sap_history":                  list(self.sap_history),
            "plant_state_override":         self.plant_state_override,
            "target_overrides":             self.target_overrides,
            "vmi_automation_enabled":       self.vmi_automation_enabled,
        }
        # Preserve any unknown future fields verbatim
        for k, v in self._extra.items():
            out.setdefault(k, v)
        return out

    # ── Convenience helpers (already useful for new code today) ──

    def tanks_for(self, product: str) -> List[TankState]:
        """All tanks holding the given product, in declaration order."""
        return [t for t in self.tanks.values() if t.product == product]

    def combined_level_lbs(self, product: str) -> float:
        """Total lbs across every tank for a product (no heel clamp)."""
        return sum(t.current_level_lbs for t in self.tanks_for(product))

    def combined_usable_lbs(self, product: str) -> float:
        """Total drawable lbs across every tank, clamped at 0 per tank."""
        return sum(max(0.0, t.current_level_lbs - t.heel_lbs)
                   for t in self.tanks_for(product))
