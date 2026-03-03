import math
from datetime import datetime, timedelta, time
import pandas as pd
from parse import parse_data
from collections import deque

# Import all constants from the centralized configuration file
from constants import (
    BUFFER_PCT,
    DEFAULT_STUDENTS_OTHER,
    PODS,
    SHOW_LENGTH_MAP,
    DEFAULT_SHOW_LEN,
    BREAK_LEN,
    START_HOUR,
    END_HOUR_REGULAR,
    END_HOUR_FRIDAY,
    STEP_MIN
)

from test_scheduling import run_all_tests

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------

def is_poly_course(course: str) -> bool:
    return "POLY" in (course or "").upper()

def eligible_pods_for_course(course: str):
    """If course contains 'POLY' -> only POLY pod, else exclude POLY pod."""
    if is_poly_course(course):
        return [p for p in PODS if p["pod"].strip().upper() == "POLY"]
    return [p for p in PODS if p["pod"].strip().upper() != "POLY"]

def parse_date(s: str) -> datetime:
    return pd.to_datetime(s).to_pydatetime().replace(hour=0, minute=0, second=0, microsecond=0)

def is_holiday(date: datetime, holidays: list) -> bool:
    """Check if a date is a holiday."""
    return date.date() in holidays

def get_show_length(course: str) -> int:
    """Determine show length based on course prefix mapping."""
    # Extract the first word (prefix) from the course name
    prefix = course.split()[0] if course else ""
    
    # Look up in the mapping, return default if not found
    return SHOW_LENGTH_MAP.get(prefix, DEFAULT_SHOW_LEN)

def business_days_inclusive(start: datetime, end: datetime, holidays: list):
    """Get business days (Mon-Fri) excluding holidays."""
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5 and not is_holiday(d, holidays):  # Mon-Fri only, no holidays
            days.append(d)
        d += timedelta(days=1)
    return days

def minutes(t: time) -> int:
    return t.hour * 60 + t.minute

def time_from_minutes(m: int) -> time:
    return time(m // 60, m % 60)

def get_end_hour_for_date(date: datetime) -> time:
    """Return closing time based on day of week."""
    if date.weekday() == 4:  # Friday (0=Monday, 4=Friday)
        return END_HOUR_FRIDAY
    else:
        return END_HOUR_REGULAR

def candidate_starts_for_date(date: datetime, show_len: int, step=STEP_MIN):
    """Generate candidate start times based on day of week and show length."""
    start_m = minutes(START_HOUR)
    end_hour = get_end_hour_for_date(date)
    last_start = minutes(end_hour) - show_len
    if last_start < start_m:
        return []
    return list(range(start_m, last_start + 1, step))

def make_bucket_targets(candidate_starts: list, total_seats_target: int, pods_for_course: list, by_shows: bool = True):
    """
    Return:
      - pos_to_bucket(pos) -> bucket index
      - bucket_targets -> list[int]

    If by_shows=True, bucket_targets represent number of SHOWS allowed per bucket.
    If by_shows=False, bucket_targets represent number of SEATS (legacy behavior).
    """

    if not candidate_starts:
        return lambda pos: 0, [0]

    # capacities for pods that can run this course
    caps = [p["capacity"] for p in pods_for_course] or [1]
    min_cap = min(caps)
    avg_cap = max(1, sum(caps) // len(caps))

    # --- FIXED BEHAVIOR ---
    # When balancing by shows, choose number of buckets B based on the number
    # of shows we actually want to place (total_seats_target in this call will be
    # the day_shows_target or remaining_shows). This prevents collapsing to B=1.
    # total_seats_target represents the number of shows we aim to place here.
    desired_shows = max(1, int(total_seats_target))
    # number of buckets should be at most the number of candidate slots,
    # and at most desired_shows (one bucket per show is reasonable).
    B = min(len(candidate_starts), desired_shows)

    # optional cap to avoid too many buckets (tweakable)
    B = max(1, min(B, 12))

    # distribute the target (shows or seats) across B buckets evenly
    if by_shows:
        total_target = max(1, int(total_seats_target))
    else:
        total_target = total_seats_target

    base = total_target // B
    rem = total_target % B
    bucket_targets = [base + (1 if i < rem else 0) for i in range(B)]

    def pos_to_bucket(pos: int) -> int:
        # Uniform mapping of positions into buckets (earliest slots -> bucket 0)
        # pos is 0..len(candidate_starts)-1
        return (pos * B) // len(candidate_starts)

    return pos_to_bucket, bucket_targets

def distribute_proportional_counts(weights: list, total: int) -> list:
    """
    Distribute `total` integer units across positions proportional to `weights[]`.
    Uses largest-remainder (Hamilton) method.
    """
    n = len(weights)
    if n == 0:
        return []
    if total <= 0:
        return [0] * n

    W = sum(weights)
    if W == 0:
        base = total // n
        rem = total % n
        return [base + (1 if i < rem else 0) for i in range(n)]

    raw = [ (total * w) / W for w in weights ]
    floors = [int(math.floor(r)) for r in raw]
    rem = total - sum(floors)
    # fractional parts with index
    fractions = sorted(enumerate([r - f for r, f in zip(raw, floors)]), key=lambda x: -x[1])
    for idx in range(rem):
        i = fractions[idx][0]
        floors[i] += 1
    return floors

def export_shows_per_hour(schedule_df: pd.DataFrame,
                          output_path: str = "output/shows_per_hour.csv"):
    """
    Generates CSV with number of shows per hour bucket:

    9–10 AM
    10–11 AM
    11–12 PM
    12–1 PM
    1–2 PM
    2–3 PM
    3–4 PM
    4–5 PM
    """

    df = schedule_df.copy()

    # Convert Start column to datetime
    df["Start"] = pd.to_datetime(df["Start"], format="%H:%M")

    # Extract hour
    df["Hour"] = df["Start"].dt.hour

    # Map hour to required bucket labels
    bucket_map = {
        9:  "9–10 AM",
        10: "10–11 AM",
        11: "11–12 PM",
        12: "12–1 PM",
        13: "1–2 PM",
        14: "2–3 PM",
        15: "3–4 PM",
        16: "4–5 PM"
    }

    df = df[df["Hour"].isin(bucket_map.keys())]

    df["Hour Bucket"] = df["Hour"].map(bucket_map)

    # Preserve correct order
    bucket_order = [
        "9–10 AM",
        "10–11 AM",
        "11–12 PM",
        "12–1 PM",
        "1–2 PM",
        "2–3 PM",
        "3–4 PM",
        "4–5 PM"
    ]

    shows_per_hour = (
        df.groupby("Hour Bucket")
          .size()
          .reindex(bucket_order, fill_value=0)
          .reset_index(name="Number of Shows")
    )

    # Save CSV
    shows_per_hour.to_csv(output_path, index=False)

    print(f"Shows-per-hour CSV saved to: {output_path}")

    return shows_per_hour

def export_shows_per_day(schedule_df: pd.DataFrame, output_path: str = "output/shows_per_day.csv"):
    """
    Generates a CSV file with number of shows scheduled per day.

    Parameters:
    -----------
    schedule_df : pd.DataFrame
        The schedule dataframe returned by your scheduler.
    
    output_path : str
        Path where the CSV should be saved.
    """

    # Count shows per date
    shows_per_day = (
        schedule_df
        .groupby("Date")
        .size()
        .reset_index(name="Number of Shows")
        .sort_values("Date")
    )

    # Save to CSV
    shows_per_day.to_csv(output_path, index=False)

    print(f"Shows-per-day CSV saved to: {output_path}")

    return shows_per_day

def export_shows_per_pod(schedule_df: pd.DataFrame,
                         output_path: str = "output/shows_per_pod.csv"):
    """
    Generate CSV and DataFrame with number of shows scheduled on each pod,
    plus total seats (sum of pod capacities used).
    """
    if schedule_df.empty:
        pod_summary = pd.DataFrame(columns=["Pod", "Shows Scheduled", "Total Seats"])
    else:
        pod_summary = (
            schedule_df
            .groupby("Pod", as_index=False)
            .agg(**{
                "Shows Scheduled": ("Pod", "size"),
                "Total Seats": ("Pod Capacity", "sum")
            })
            .sort_values("Shows Scheduled", ascending=False)
            .reset_index(drop=True)
        )

    pod_summary.to_csv(output_path, index=False)
    print(f"Shows-per-pod CSV saved to: {output_path}")
    return pod_summary

def interleaved_positions_by_bucket(candidate_starts: list, pos_to_bucket):
    """
    Produce a list of candidate position indices (0..len(candidate_starts)-1)
    with the following behavior:
      - Group positions by bucket (pos_to_bucket).
      - For each bucket, create an in/out sequence: [first, last, second, second_last, ...]
      - Finally, round-robin across buckets: take the next item from bucket0, then bucket1, ...
        and repeat until all are exhausted.

    This yields: spread across buckets, and within each bucket picks earliest, latest,
    then next-earliest, next-latest, etc.
    """
    # group positions by bucket preserving ascending order
    buckets = {}
    for pos_idx, _m in enumerate(candidate_starts):
        b = pos_to_bucket(pos_idx)
        buckets.setdefault(b, []).append(pos_idx)

    # build in/out sequence per bucket
    bucket_queues = {}
    for b, pos_list in buckets.items():
        left = 0
        right = len(pos_list) - 1
        order = []
        take_left = True
        while left <= right:
            if take_left:
                order.append(pos_list[left])
                left += 1
            else:
                order.append(pos_list[right])
                right -= 1
            take_left = not take_left
        bucket_queues[b] = deque(order)

    # round-robin across buckets in ascending bucket index order
    result = []
    for b in sorted(bucket_queues.keys()):
        pass  # ensure buckets exist in sorted order (no-op)
    while True:
        added = False
        for b in sorted(bucket_queues.keys()):
            if bucket_queues[b]:
                result.append(bucket_queues[b].popleft())
                added = True
        if not added:
            break
    return result

def schedule(students: dict, data: list, holidays: list) -> pd.DataFrame:
    """
    Generate a schedule for shows based on students, pods, and activity data.
    
    Parameters:
    -----------
    students : dict
        Dictionary mapping course names to number of students.
        Example: {"Bio 181": 576, "Bio 100": 350}
    
    data : list
        List of tuples: (Course, Mod/Act, Open Date, Close Date).
        Example: [("Bio 100", "M1 A1", "Friday, January 16, 2026", "Wednesday, January 28, 2026"), ...]
    
    holidays : list
        List of datetime.date objects representing holidays.
        Example: [datetime(2026, 1, 19).date(), ...]
    
    Returns:
    --------
    tuple of (pd.DataFrame, pd.DataFrame)
        schedule_df: DataFrame containing the schedule with columns:
            Course, Mod/Act, Date, Start, End, Pod, Pod Capacity, Show Length
        summary_df: DataFrame containing summary with columns:
            Course, Mod/Act, Students, Buffer%, Seats Required, Scheduled Seats, Shows Scheduled, Show Length, Open, Close
    """
    # -----------------------------
    # INTERNAL STATE / SHARED DATA
    # -----------------------------
    ops_used_starts = {}   # (day_key, ops_group) -> set(start_min)
    ops_used_ends = {}     # (day_key, ops_group) -> set(end_min)
    all_bookings = {}      # (day_key, pod, start_min) -> (course, mod_act, end_min, show_len)
    pod_usage_count = {p["pod"]: 0 for p in PODS}

    # Rows to collect
    schedule_rows = []
    summary_rows = []

    # -----------------------------
    # SMALL HELPERS (close over state above)
    # -----------------------------
    def pods_sorted_for_slot(course: str):
        """Return pods eligible for course, sorted by (-capacity, usage_count)"""
        pods = eligible_pods_for_course(course)
        return sorted(pods, key=lambda p: (-p["capacity"], pod_usage_count[p["pod"]]))

    def can_place(day_key: str, pod: str, ops_group: str, start_min: int,
                  course: str, mod_act: str, date_obj: datetime, show_len: int) -> bool:
        """
        Check if a show can be placed at (day_key, pod, start_min) respecting:
          - ops group unique start/end constraints
          - closing time
          - pod's existing bookings and BREAK_LEN between different activities
          - overlapping same-activity constraint
        """
        end_min = start_min + show_len

        # ops-group constraints
        if start_min in ops_used_starts.get((day_key, ops_group), set()):
            return False
        if end_min in ops_used_ends.get((day_key, ops_group), set()):
            return False

        # closing time
        if end_min > minutes(get_end_hour_for_date(date_obj)):
            return False

        # pod booking conflicts
        for (d, p, existing_start), details in all_bookings.items():
            if d != day_key or p != pod:
                continue
            existing_course, existing_mod_act, existing_end, _ = details
            same_activity = (existing_course == course and existing_mod_act == mod_act)

            if same_activity:
                # simple overlap disallowed
                if not (end_min <= existing_start or start_min >= existing_end):
                    return False
            else:
                # require BREAK_LEN between different activities
                if start_min < existing_end + BREAK_LEN and end_min > existing_start:
                    return False
                if existing_start < end_min + BREAK_LEN and existing_end > start_min:
                    return False

        return True

    def place_booking(day_key: str, pod: str, ops_group: str, start_min: int,
                      course: str, mod_act: str, show_len: int):
        """Record a successful placement in the scheduler state and increment usage counters."""
        end_min = start_min + show_len
        ops_used_starts.setdefault((day_key, ops_group), set()).add(start_min)
        ops_used_ends.setdefault((day_key, ops_group), set()).add(end_min)
        all_bookings[(day_key, pod, start_min)] = (course, mod_act, end_min, show_len)
        pod_usage_count[pod] += 1

    def record_schedule_row(course: str, mod_act: str, day_key: str, start_min: int, show_len: int, pod: str, pod_capacity: int):
        """Append a row to the schedule_rows list (formatted as expected)."""
        start_t = time_from_minutes(start_min)
        end_t = time_from_minutes(start_min + show_len)
        schedule_rows.append({
            "Course": course,
            "Mod/Act": mod_act,
            "Date": day_key,
            "Start": start_t.strftime("%H:%M"),
            "End": end_t.strftime("%H:%M"),
            "Pod": pod,
            "Pod Capacity": pod_capacity,
            "Show Length": show_len,
        })

    # -----------------------------
    # CORE: place_shows_on_day (modular engine)
    # -----------------------------
    def place_shows_on_day(
        day_dt: datetime,
        course: str,
        mod_act: str,
        show_len: int,
        seats_needed: int,
        current_capacity: int,
        shows_count: int,
        day_show_target: int,
        day_shows_filled_map: dict,
        enforce_bucket_limits: bool = True
    ):
        """
        Try to place shows on a given day `day_dt` until either:
          - current_capacity >= seats_needed (done), or
          - day_show_target (if enforce_bucket_limits) reached, or
          - no more candidate slots.

        Returns updated (current_capacity, shows_count).
        This function mutates scheduler state (all_bookings, ops_used_*, pod_usage_count, schedule_rows).
        """
        day_key = day_dt.strftime("%Y-%m-%d")
        candidate_starts = candidate_starts_for_date(day_dt, show_len)
        if not candidate_starts:
            return current_capacity, shows_count

        # Determine bucket mapping & targets (when enforcing per-day show caps)
        pods_for_course = eligible_pods_for_course(course)
        if enforce_bucket_limits:
            pos_to_bucket, bucket_targets = make_bucket_targets(candidate_starts, max(1, day_show_target), pods_for_course, by_shows=True)
            bucket_filled = [0] * len(bucket_targets)
        else:
            # seat-driven pass: single infinite bucket
            pos_to_bucket = lambda pos: 0
            bucket_targets = [float("inf")]
            bucket_filled = [0]

        # Interleaved order per bucket (first, last, second, second-last, ...), round-robin across buckets
        pos_order = interleaved_positions_by_bucket(candidate_starts, pos_to_bucket)

        # Keep local copy of the current number of shows filled for this day
        filled = day_shows_filled_map.get(day_key, 0)

        for pos_idx in pos_order:
            if current_capacity >= seats_needed:
                break
            if enforce_bucket_limits and filled >= day_show_target:
                break

            start_min = candidate_starts[pos_idx]
            bucket_idx = pos_to_bucket(pos_idx)

            # Skip if this bucket already filled its target
            if bucket_idx < 0 or bucket_idx >= len(bucket_targets):
                continue
            if bucket_filled[bucket_idx] >= bucket_targets[bucket_idx]:
                continue

            # Try pods in preferred order for this slot
            for podinfo in pods_sorted_for_slot(course):
                pod_name = podinfo["pod"]
                pod_cap = podinfo["capacity"]
                ops_grp = podinfo["ops_group"]

                if can_place(day_key, pod_name, ops_grp, start_min, course, mod_act, day_dt, show_len):
                    # Place the booking and record it
                    place_booking(day_key, pod_name, ops_grp, start_min, course, mod_act, show_len)
                    record_schedule_row(course, mod_act, day_key, start_min, show_len, pod_name, pod_cap)

                    current_capacity += pod_cap
                    shows_count += 1
                    filled += 1
                    day_shows_filled_map[day_key] = filled
                    # mark bucket filled for this placement
                    bucket_filled[bucket_idx] += 1
                    # placed - move to next position
                    break

        return current_capacity, shows_count

    # -----------------------------
    # PREP: build windows from input data
    # -----------------------------
    df = pd.DataFrame(data, columns=["Course", "Mod/Act", "Open Date", "Close Date"])
    df["Open Date"] = df["Open Date"].apply(parse_date)
    df["Close Date"] = df["Close Date"].apply(parse_date)

    windows = (
        df.groupby(["Course", "Mod/Act"], as_index=False)
          .agg(open_date=("Open Date", "min"), close_date=("Close Date", "max"))
    ).sort_values(["close_date", "open_date"]).reset_index(drop=True)

    # -----------------------------
    # MAIN LOOP: iterate each (course, mod)
    # -----------------------------
    for _, w in windows.iterrows():
        course = w["Course"]
        mod_act = w["Mod/Act"]
        open_dt = w["open_date"]
        close_dt = w["close_date"]

        # Student seats required (with buffer)
        student_count = students.get(course, DEFAULT_STUDENTS_OTHER)
        seats_required = math.ceil(student_count * (1.0 + BUFFER_PCT))

        # Show length for this course
        show_len = get_show_length(course)

        # Working business days for the window
        days = business_days_inclusive(open_dt, close_dt, holidays)
        if not days:
            raise ValueError(f"No business days in window for ({course}, {mod_act}).")

        # Running totals for this (course,mod) pair
        total_capacity = 0
        shows_for_pair = 0

        # Compute weighted per-day show targets
        num_days = len(days)
        weights = list(range(1, num_days + 1))
        eligible_pods = eligible_pods_for_course(course)
        caps = [p["capacity"] for p in eligible_pods] or [1]
        avg_cap = max(1, sum(caps) // len(caps))
        estimated_total_shows = max(1, math.ceil(seats_required / avg_cap))
        per_day_shows = distribute_proportional_counts(weights, estimated_total_shows)

        # Map date -> per-day target and initialize filled counters
        day_list = days[:]  # chronological ascending
        per_day_shows_map = {day_list[i].strftime("%Y-%m-%d"): per_day_shows[i] for i in range(len(day_list))}
        day_shows_filled_map = {day.strftime("%Y-%m-%d"): 0 for day in day_list}

        # Debug prints preserved from original
        print("DEBUG: days (chronological):", [d.date().isoformat() for d in day_list])
        print("DEBUG: weights:", weights)
        print("DEBUG: eligible_pods caps:", [p['capacity'] for p in eligible_pods])
        print("DEBUG: avg_cap (used):", avg_cap)
        print("DEBUG: seats_required:", seats_required)
        print("DEBUG: estimated_total_shows:", estimated_total_shows)
        print("DEBUG: per_day_shows (list):", per_day_shows)
        print("DEBUG: per_day_shows_map:", per_day_shows_map)

        # -----------------------------
        # PASS 1: Distributed pass — fill per-day targets (last->first)
        # -----------------------------
        for day_dt in reversed(days):
            if total_capacity >= seats_required:
                break
            day_key = day_dt.strftime("%Y-%m-%d")
            day_target = per_day_shows_map.get(day_key, 0)
            total_capacity, shows_for_pair = place_shows_on_day(
                day_dt=day_dt,
                course=course,
                mod_act=mod_act,
                show_len=show_len,
                seats_needed=seats_required,
                current_capacity=total_capacity,
                shows_count=shows_for_pair,
                day_show_target=day_target,
                day_shows_filled_map=day_shows_filled_map,
                enforce_bucket_limits=True
            )

        # -----------------------------
        # PASS 2: Catch-up pass — try remaining per-day targets (last->first)
        # -----------------------------
        if total_capacity < seats_required:
            for day_dt in reversed(days):
                if total_capacity >= seats_required:
                    break
                day_key = day_dt.strftime("%Y-%m-%d")
                remaining_allowed = max(0, per_day_shows_map.get(day_key, 0) - day_shows_filled_map.get(day_key, 0))
                if remaining_allowed <= 0:
                    continue
                total_capacity, shows_for_pair = place_shows_on_day(
                    day_dt=day_dt,
                    course=course,
                    mod_act=mod_act,
                    show_len=show_len,
                    seats_needed=seats_required,
                    current_capacity=total_capacity,
                    shows_count=shows_for_pair,
                    day_show_target=remaining_allowed,
                    day_shows_filled_map=day_shows_filled_map,
                    enforce_bucket_limits=True
                )

        # -----------------------------
        # PASS 3: Seat-driven pass — place any remaining seats ignoring per-day caps (last->first)
        # -----------------------------
        if total_capacity < seats_required:
            for day_dt in reversed(days):
                if total_capacity >= seats_required:
                    break
                total_capacity, shows_for_pair = place_shows_on_day(
                    day_dt=day_dt,
                    course=course,
                    mod_act=mod_act,
                    show_len=show_len,
                    seats_needed=seats_required,
                    current_capacity=total_capacity,
                    shows_count=shows_for_pair,
                    day_show_target=float("inf"),
                    day_shows_filled_map=day_shows_filled_map,
                    enforce_bucket_limits=False
                )

        # If after all passes we still couldn't reach required seats — error out
        if total_capacity < seats_required:
            raise ValueError(
                f"Not enough capacity to schedule ({course}, {mod_act}) within {open_dt.date()}..{close_dt.date()} "
                f"under current constraints. Could only schedule {total_capacity} of {seats_required} seats."
            )

        # Build summary row for this (course,mod)
        summary_rows.append({
            "Course": course,
            "Mod/Act": mod_act,
            "Students": student_count,
            "Buffer%": BUFFER_PCT,
            "Seats Required": seats_required,
            "Scheduled Seats": total_capacity,
            "Shows Scheduled": shows_for_pair,
            "Show Length": show_len,
            "Open": open_dt.date().isoformat(),
            "Close": close_dt.date().isoformat(),
        })

    # -----------------------------
    # OUTPUT DATAFRAMES
    # -----------------------------
    schedule_df = pd.DataFrame(schedule_rows).sort_values(["Date", "Start", "Pod"]).reset_index(drop=True)
    summary_df = pd.DataFrame(summary_rows).sort_values(["Course", "Mod/Act"]).reset_index(drop=True)

    return schedule_df, summary_df


# -----------------------------
# EXAMPLE USAGE / MAIN EXECUTION
# -----------------------------
if __name__ == "__main__":
    # Example inputs — replace with parse_data() in production
    # STUDENTS = {"Bio 100": 500}
    
    # DATA = [ 
    #         ("Bio 100", "M1 A1", "Friday, January 23, 2026", "Wednesday, January 28, 2026"), 
    #         # ("Bio 181", "M1 A1", "Friday, January 16, 2026", "Wednesday, January 28, 2026"), 
    #         # ("CHM 113", "CHM M1 A1", "Tuesday, January 20, 2026", "Monday, February 2, 2026"), 
    #         # ("Bio 100", "M1 A2", "Monday, January 26, 2026", "Wednesday, February 4, 2026"), 
    #         # ("Bio 181", "M1 A2", "Monday, January 26, 2026", "Wednesday, February 4, 2026"), 
    #         # ("Bio 182", "M4 A1", "Tuesday, January 27, 2026", "Thursday, February 5, 2026"), 
    #         # ("Bio 100", "M1 A3", "Monday, February 2, 2026", "Wednesday, February 11, 2026"), 
    #         # ("Bio 181", "M1 A3", "Monday, February 2, 2026", "Wednesday, February 11, 2026"), 
    #         # ("CHM 114", "CHM M1 A1", "Monday, February 2, 2026", "Wednesday, February 11, 2026"), 
    #     ]
    
    # HOLIDAYS = [
    #     datetime(2026, 1, 19).date(),  # Mon, 1/19/26
    #     datetime(2026, 3, 9).date(),   # Mon, 3/9/26
    #     datetime(2026, 3, 10).date(),  # Tue, 3/10/26
    #     datetime(2026, 3, 11).date(),  # Wed, 3/11/26
    #     datetime(2026, 3, 12).date(),  # Thu, 3/12/26
    #     datetime(2026, 3, 13).date(),  # Fri, 3/13/26
    # ]

    # If you want to use parse_data(), uncomment:
    DATA, HOLIDAYS, STUDENTS = parse_data()
    
    # Call the schedule function
    schedule_df, summary_df = schedule(
        students=STUDENTS,
        data=DATA,
        holidays=HOLIDAYS
    )

    export_shows_per_day(schedule_df)
    export_shows_per_hour(schedule_df)
    export_shows_per_pod(schedule_df)
    
    # Print outputs
    print("=== SHOW LENGTH MAPPING ===")
    for prefix, length in SHOW_LENGTH_MAP.items():
        print(f"{prefix} -> {length} mins")
    print(f"Default -> {DEFAULT_SHOW_LEN} mins")
    print()
    
    print("=== SUMMARY ===")
    print(summary_df.to_string(index=False))
    
    print("\n=== SCHEDULE ===")
    print(schedule_df[["Course", "Mod/Act", "Date", "Start", "End", "Pod", "Pod Capacity", "Show Length"]].to_string(index=False))
    
    # Ensure proper sorting first
    schedule_df_sorted = schedule_df.sort_values(
        ["Course", "Mod/Act", "Date", "Start"]
    )
    
    # Print grouped output
    for (course, mod), group in schedule_df_sorted.groupby(["Course", "Mod/Act"]):
        print(f"\n===== {course} | {mod} =====")
        print(group[["Date", "Start", "End", "Pod", "Pod Capacity", "Show Length"]].to_string(index=False))
    
    # Optional exports:
    schedule_df.to_csv("output/show_schedule.csv", index=False)
    summary_df.to_csv("output/show_summary.csv", index=False)

    run_all_tests(schedule_df, STUDENTS, HOLIDAYS)
    
    print(f"\n Successfully scheduled {len(schedule_df)} shows.")
    print("Note: Operational hours - 9 AM to 5 PM (Mon-Thu), 9 AM to 1 PM (Fri)")
    print(f"Note: No scheduling on holidays: {', '.join([h.strftime('%m/%d/%y') for h in HOLIDAYS])}")