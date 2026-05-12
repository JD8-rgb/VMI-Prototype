"""
Advance the simulation clock by N run-hours, processing:
- Truck deliveries at their arrival_run_hour
- Run-schedule windows (consumption only happens inside scheduled windows)

Consumption and delivery delegate to alerts.simulate_consume /
alerts.simulate_delivery_no_alert so CLI behavior matches the Streamlit
app, projection chart, and planner. Previously this script had its own
copies that consumed from the persisted draw/status field, which made
CLI runs disagree with the live simulation after the draw-status fix.

CLI usage
---------
    python advance_time.py 8                       # advance 8 hours, save
    python advance_time.py "2026-04-22 17:00"      # advance to a target time
    python advance_time.py --customer X 8          # read-only sim against
                                                   # customers/X.json (no save)

Known limit (see MIGRATION_GUIDE.md): advance_time builds its own
event queue from run_schedule directly and does NOT consult cfg.plant_holidays.
For customers with holidays defined, consumption is currently still
computed across the holiday day. Streamlit / get_all_alerts / planner all
honor holidays correctly; only this CLI path is calendar-blind. Fixing
this is part of the multi-tenant runtime work — see MIGRATION_GUIDE § 7.
"""

import argparse
import json
import logging
import os
import sys
from time_utils import parse_time_input, format_run_hour
from alerts import simulate_consume, simulate_delivery_no_alert
from alerts import refresh_draw_status as _refresh_draw_status
import email_hooks


def _load_default_data():
    path = "data.json" if os.path.exists("data.json") else "defaults.json"
    with open(path) as f:
        return json.load(f)


def _parse_args(argv=None):
    p = argparse.ArgumentParser(prog="advance_time.py", description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("target",
                    help="Hours to advance (8) or target datetime (\"2026-04-22 17:00\")")
    p.add_argument("--customer", default=None,
                    help="Run against customers/<id>.json in read-only mode (no save)")
    return p.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.customer is not None:
        from customers import load_customer
        _, data = load_customer(args.customer)
        print(f"(Customer: {args.customer} — read-only, no save)")
        save_back = False
    else:
        data = _load_default_data()
        save_back = True

    tanks = data["tanks"]
    rates = data["consumption_rates"]
    trucks = data["scheduled_trucks"]
    run_schedule = data["run_schedule"]
    start_hour = data["current_run_hour"]

    # Parse the argument: if it's a small number (< current_run_hour), treat as
    # a duration to add. Otherwise, treat as a target run-hour or datetime.
    try:
        parsed = parse_time_input(data, args.target)
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    if parsed < start_hour:
        hours = parsed
        end_hour = start_hour + hours
    else:
        end_hour = parsed
        hours = end_hour - start_hour

    if hours <= 0:
        print(f"Error: target {format_run_hour(data, end_hour)} is not in the future.")
        return 1

    print(f"Advancing {hours} hours")
    print(f"  from: {format_run_hour(data, start_hour)}")
    print(f"  to:   {format_run_hour(data, end_hour)}\n")

    def consume_segment(seg_hours):
        """Drain each product's per-hour rate × seg_hours via shared logic."""
        if seg_hours <= 0:
            return
        for product, rate_info in rates.items():
            demand = rate_info["lbs_per_hour"] * seg_hours
            before = {n: i["current_level_lbs"] for n, i in tanks.items()
                      if i["product"] == product}
            print(f"  {product}: {demand:,.1f} lbs demand")
            simulate_consume(tanks, product, demand)
            for name, prev in before.items():
                now = tanks[name]["current_level_lbs"]
                if abs(now - prev) > 0.01:
                    print(f"    {name}: {prev:,.1f} -> {now:,.1f}  "
                          f"(consumed {prev - now:,.1f} lbs, status={tanks[name]['status']})")

    def _snapshot(run_hour):
        """Record per-tank levels at this tick into level_history.
        Powers the VMI Health Dashboard chart and any future bias-
        detection logic."""
        from level_history import record_level_snapshot as _rls
        _rls(data, run_hour)

    def deliver_truck(truck):
        """Pour truck into the lowest-level tank via shared logic.

        Returns True if the truck was fully delivered (caller marks the
        SAP delivered). Returns False if some lbs couldn't fit — the
        delivery is REFUSED (operator must intervene), and the caller
        retains the truck in scheduled_trucks for the operator to
        cancel / split / reschedule. Audit entry recorded either way.
        """
        product = truck["product"]
        quantity = truck["quantity_lbs"]
        sap = truck["sap_order"]
        print(f"  >> DELIVERY: {sap} | {product} | {quantity:,} lbs")
        before = {n: i["current_level_lbs"] for n, i in tanks.items()
                  if i["product"] == product}
        residual = simulate_delivery_no_alert(tanks, truck)
        for name, prev in before.items():
            now = tanks[name]["current_level_lbs"]
            if abs(now - prev) > 0.01:
                print(f"     {name}: +{now - prev:,.1f} lbs -> {now:,.1f}  "
                      f"(status={tanks[name]['status']})")
        if residual > 0:
            # Overfill: every tank for this product is full and the
            # cascade dropped `residual` lbs on the floor. Roll back
            # the pour so the simulation faithfully reflects "truck
            # refused at the gate", and retain the truck in
            # scheduled_trucks so the operator sees it and decides
            # (cancel / split / reschedule).
            for name, prev in before.items():
                tanks[name]["current_level_lbs"] = prev
            _refresh_draw_status(tanks, product)
            print(f"  !! REFUSED: {sap} | {residual:,.0f} lbs over "
                  f"available tank space. Truck retained in schedule.")
            try:
                from audit_log import record as _audit_record, A_OVERFLOW_REFUSED
                _audit_record(data, A_OVERFLOW_REFUSED, details={
                    "sap": sap,
                    "product": product,
                    "quantity_lbs": float(quantity),
                    "residual_lbs": float(residual),
                })
            except Exception:
                pass
            return False
        return True

    # --- Build event queue: trucks + run-window edges, in [start_hour, end_hour] ---
    events = []

    for truck in trucks:
        ah = truck["arrival_run_hour"]
        if start_hour < ah <= end_hour:
            events.append((ah, "delivery", truck))

    for window in run_schedule:
        if window["end_hour"] > start_hour and window["start_hour"] < end_hour:
            ws = max(window["start_hour"], start_hour)
            we = min(window["end_hour"], end_hour)
            if ws > start_hour:
                events.append((ws, "run_start", None))
            if we < end_hour:
                events.append((we, "run_end", None))

    def event_priority(e):
        order = {"run_end": 0, "run_start": 1, "delivery": 2}
        return (e[0], order[e[1]])

    events.sort(key=event_priority)

    # Determine initial burning state
    burning = False
    for window in run_schedule:
        if window["start_hour"] <= start_hour < window["end_hour"]:
            burning = True
            break

    clock = start_hour
    delivered_sap_orders = []

    print(f"(Plant is {'RUNNING' if burning else 'idle'} at start)")

    # Snapshot at the very start so level_history has an anchor point
    # even if the operator only advances by a fraction of an hour.
    _snapshot(clock)

    for event in events:
        ev_time, ev_type, payload = event
        seg = ev_time - clock
        if seg > 0:
            if burning:
                print(f"Segment: {format_run_hour(data, clock)} -> {format_run_hour(data, ev_time)} ({seg} hrs, RUNNING)")
                consume_segment(seg)
            else:
                print(f"Segment: {format_run_hour(data, clock)} -> {format_run_hour(data, ev_time)} ({seg} hrs, idle)")
            clock = ev_time
            # Snapshot at the end of every segment so level_history has
            # entries even on long contiguous segments. With max
            # segment length ≈ 24h (one run window), the chart sees at
            # least one point per window edge.
            _snapshot(clock)

        if ev_type == "run_start":
            burning = True
            print(f"  ** Plant started running at {format_run_hour(data, ev_time)}")
        elif ev_type == "run_end":
            burning = False
            print(f"  ** Plant stopped running at {format_run_hour(data, ev_time)}")
        elif ev_type == "delivery":
            if deliver_truck(payload):
                delivered_sap_orders.append(payload["sap_order"])
            # else: refused truck stays in scheduled_trucks for operator action
        print()

    if clock < end_hour:
        seg = end_hour - clock
        if burning:
            print(f"Segment: {format_run_hour(data, clock)} -> {format_run_hour(data, end_hour)} ({seg} hrs, RUNNING)")
            consume_segment(seg)
        else:
            print(f"Segment: {format_run_hour(data, clock)} -> {format_run_hour(data, end_hour)} ({seg} hrs, idle)")
        clock = end_hour
        # Final snapshot so the latest level appears in level_history
        # exactly at end_hour (not whatever the prior segment ended on).
        _snapshot(end_hour)

    data["scheduled_trucks"] = [t for t in trucks if t["sap_order"] not in delivered_sap_orders]
    data["current_run_hour"] = end_hour

    if save_back:
        data = email_hooks.send_alert_emails_if_new(data)
        from data_io import save_data
        save_data(data)
        save_msg = "Data saved."
    else:
        save_msg = "Read-only mode (--customer): data NOT saved."

    print(f"\nClock now at {format_run_hour(data, end_hour)}.")
    print(f"Delivered {len(delivered_sap_orders)} trucks. {len(data['scheduled_trucks'])} remaining.")
    print(save_msg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
