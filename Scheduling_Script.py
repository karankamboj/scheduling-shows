import math
from datetime import datetime, timedelta, time
import pandas as pd
from parse import parse_data

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
    # STATE (internal to function)
    # -----------------------------
    # Shared-ops constraint: pods in same ops_group cannot share the same start time
    ops_used_starts = {}  # (day_key, ops_group) -> set(start_min)
    ops_used_ends   = {}  # (day_key, ops_group) -> set(end_min)
    
    # Track all bookings with their details
    # Format: (day_key, pod, start_min) -> (course, mod_act, end_min, show_len)
    all_bookings = {}
    
    # Soft balancing: spread usage across pods (tie-breaker)
    pod_usage_count = {p["pod"]: 0 for p in PODS}
    
    def can_place(day_key: str, pod: str, ops_group: str, start_min: int,
                course: str, mod_act: str, date_obj: datetime, show_len: int) -> bool:
        end_min = start_min + show_len

        # ops-team: no same start time within group
        if start_min in ops_used_starts.get((day_key, ops_group), set()):
            return False

        # ops-team: no same end time within group  -  NEW
        if end_min in ops_used_ends.get((day_key, ops_group), set()):
            return False
        
        # Check if show would end after closing time
        end_hour = get_end_hour_for_date(date_obj)
        if end_min > minutes(end_hour):
            return False
        
        # Check against all existing bookings in the same pod
        for (d, p, existing_start), details in all_bookings.items():
            if d == day_key and p == pod:
                existing_course, existing_mod_act, existing_end, existing_len = details
                
                # Check if same activity (same course AND same mod/act)
                same_activity = (existing_course == course and existing_mod_act == mod_act)
                
                if same_activity:
                    # Same activity: just check for overlap
                    if not (end_min <= existing_start or start_min >= existing_end):
                        return False
                else:
                    # Different activity: need BREAK_LEN minute break
                    # Check if new show starts within BREAK_LEN mins after existing show ends
                    if start_min < existing_end + BREAK_LEN and end_min > existing_start:
                        return False
                    # Check if existing show starts within BREAK_LEN mins after new show ends
                    if existing_start < end_min + BREAK_LEN and existing_end > start_min:
                        return False
        
        return True
    
    def place(day_key: str, pod: str, ops_group: str, start_min: int,
            course: str, mod_act: str, show_len: int):
        end_min = start_min + show_len

        ops_used_starts.setdefault((day_key, ops_group), set()).add(start_min)
        ops_used_ends.setdefault((day_key, ops_group), set()).add(end_min)  # NEW

        all_bookings[(day_key, pod, start_min)] = (course, mod_act, end_min, show_len)
        pod_usage_count[pod] += 1
    
    def pods_sorted_for_slot(course: str):
        pods = eligible_pods_for_course(course)
        return sorted(pods, key=lambda p: (-p["capacity"], pod_usage_count[p["pod"]]))
    
    # -----------------------------
    # BUILD WINDOWS (Course+Mod/Act)
    # -----------------------------
    df = pd.DataFrame(data, columns=["Course", "Mod/Act", "Open Date", "Close Date"])
    df["Open Date"] = df["Open Date"].apply(parse_date)
    df["Close Date"] = df["Close Date"].apply(parse_date)
    
    windows = (
        df.groupby(["Course", "Mod/Act"], as_index=False)
          .agg(open_date=("Open Date", "min"), close_date=("Close Date", "max"))
    ).sort_values(["close_date", "open_date"]).reset_index(drop=True)
    
    # -----------------------------
    # MAIN SCHEDULING LOOP
    # -----------------------------
    schedule_rows = []
    summary_rows = []
    
    for _, w in windows.iterrows():
        course = w["Course"]
        mod = w["Mod/Act"]
        open_dt = w["open_date"]
        close_dt = w["close_date"]
    
        student_count = students.get(course, DEFAULT_STUDENTS_OTHER)
        seats_required = math.ceil(student_count * (1.0 + BUFFER_PCT))
    
        # Get show length for this course from mapping
        show_len = get_show_length(course)
        
        days = business_days_inclusive(open_dt, close_dt, holidays)
        if not days:
            raise ValueError(f"No business days in window for ({course}, {mod}).")
    
        total_capacity = 0
        shows_for_pair = 0
        
        # -----------------------------
        # Weighted per-day show targets
        # -----------------------------
        # number of days and weights 1..N
        num_days = len(days)
        weights = list(range(1, num_days + 1))  # day1 weight=1 ... dayN weight=num_days

        # Estimate total shows required (rough) by avg pod capacity
        eligible_pods = eligible_pods_for_course(course)
        print("eligible pods ",eligible_pods)
        caps = [p["capacity"] for p in eligible_pods] or [1]
        avg_cap = max(1, sum(caps) // len(caps))
        # At least 1 show. estimated_total_shows is how many shows roughly needed to meet seats_required
        estimated_total_shows = max(1, math.ceil(seats_required / avg_cap))

        # compute integer per-day show targets proportional to weights
        per_day_shows = distribute_proportional_counts(weights, estimated_total_shows)
        # map day_key -> show target; days is chronological ascending
        day_list = days[:]  # chronological
        per_day_shows_map = {day_list[idx].strftime("%Y-%m-%d"): per_day_shows[idx] for idx in range(len(day_list))}
        # track how many shows we've placed per day (across passes)
        day_shows_filled_map = {day.strftime("%Y-%m-%d"): 0 for day in day_list}

        print("DEBUG: days (chronological):", [d.date().isoformat() for d in day_list])
        print("DEBUG: weights:", weights)
        print("DEBUG: eligible_pods caps:", [p['capacity'] for p in eligible_pods])
        print("DEBUG: avg_cap (used):", avg_cap)
        print("DEBUG: seats_required:", seats_required)
        print("DEBUG: estimated_total_shows:", estimated_total_shows)
        print("DEBUG: per_day_shows (list):", per_day_shows)
        print("DEBUG: per_day_shows_map:", per_day_shows_map)

        # --- PASS 1: Distributed (Last to First) ---
        # Try to fill each day up to its calculated show count target
        for i, d in enumerate(reversed(days)):
            if total_capacity >= seats_required:
                break
                
            # note: loop order is reversed(days) to preserve your original ordering logic
            day_key = d.strftime("%Y-%m-%d")
            # number of shows we should place on this day (weighted)
            day_shows_target = per_day_shows_map.get(day_key, 0)
            # print(day_shows_target, "is target")
            day_shows_filled = day_shows_filled_map.get(day_key, 0)
            day_capacity_filled = 0
            
            candidate_starts = candidate_starts_for_date(d, show_len)
            if not candidate_starts:
                continue

            # Prepare pods and bucket targets — use show-count balancing (by_shows=True)
            pods_for_course = eligible_pods_for_course(course)
            # Use bucket targets sized to the day's show target (or at least 1)
            pos_to_bucket, bucket_targets = make_bucket_targets(candidate_starts, max(1, day_shows_target), pods_for_course, by_shows=True)
            # bucket_filled counts number of SHOWS placed into each bucket
            bucket_filled = [0] * len(bucket_targets)

            for pos_idx, start_min in enumerate(candidate_starts):
                if total_capacity >= seats_required or day_shows_filled >= day_shows_target:
                    break

                bucket_idx = pos_to_bucket(pos_idx)
                # if this bucket already reached its show target, skip this start
                if bucket_filled[bucket_idx] >= bucket_targets[bucket_idx]:
                    continue

                for podinfo in pods_sorted_for_slot(course):
                    pod, cap, grp = podinfo["pod"], podinfo["capacity"], podinfo["ops_group"]

                    if can_place(day_key, pod, grp, start_min, course, mod, d, show_len):
                        place(day_key, pod, grp, start_min, course, mod, show_len)
                        start_t, end_t = time_from_minutes(start_min), time_from_minutes(start_min + show_len)

                        schedule_rows.append({
                            "Course": course, "Mod/Act": mod, "Date": day_key,
                            "Start": start_t.strftime("%H:%M"), "End": end_t.strftime("%H:%M"),
                            "Pod": pod, "Pod Capacity": cap, "Show Length": show_len,
                        })
                        total_capacity += cap
                        # print("cap is ",total_capacity, "for date", d)
                        shows_for_pair += 1
                        day_capacity_filled += cap
                        # increment by 1 show (not by seats) so we balance shows across buckets
                        bucket_filled[bucket_idx] += 1
                        day_shows_filled += 1
                        day_shows_filled_map[day_key] = day_shows_filled
                        break

        # --- PASS 2: Catch-up (Last to First) ---
        # If still need more seats, try to place additional shows:
        if total_capacity < seats_required:

            # First attempt: place the remaining targeted shows per day (if any), iterating last->first
            for d in reversed(days):
                if total_capacity >= seats_required:
                    break

                day_key = d.strftime("%Y-%m-%d")
                allowed_shows_on_day = max(0, per_day_shows_map.get(day_key, 0) - day_shows_filled_map.get(day_key, 0))
                if allowed_shows_on_day <= 0:
                    continue

                candidate_starts = candidate_starts_for_date(d, show_len)
                if not candidate_starts:
                    continue

                pods_for_course = eligible_pods_for_course(course)
                # bucket targets sized to allowed_shows_on_day
                pos_to_bucket, bucket_targets = make_bucket_targets(candidate_starts, max(1, allowed_shows_on_day), pods_for_course, by_shows=True)
                bucket_filled = [0] * len(bucket_targets)

                day_shows_filled = day_shows_filled_map.get(day_key, 0)

                for pos_idx, start_min in enumerate(candidate_starts):
                    if total_capacity >= seats_required or day_shows_filled >= per_day_shows_map.get(day_key, 0):
                        break

                    bucket_idx = pos_to_bucket(pos_idx)
                    if bucket_filled[bucket_idx] >= bucket_targets[bucket_idx]:
                        continue

                    for podinfo in pods_sorted_for_slot(course):
                        pod, cap, grp = podinfo["pod"], podinfo["capacity"], podinfo["ops_group"]

                        if can_place(day_key, pod, grp, start_min, course, mod, d, show_len):
                            place(day_key, pod, grp, start_min, course, mod, show_len)
                            start_t, end_t = time_from_minutes(start_min), time_from_minutes(start_min + show_len)

                            schedule_rows.append({
                                "Course": course, "Mod/Act": mod, "Date": day_key,
                                "Start": start_t.strftime("%H:%M"), "End": end_t.strftime("%H:%M"),
                                "Pod": pod, "Pod Capacity": cap, "Show Length": show_len,
                            })
                            total_capacity += cap
                            shows_for_pair += 1
                            bucket_filled[bucket_idx] += 1
                            day_shows_filled += 1
                            day_shows_filled_map[day_key] = day_shows_filled
                            break

            # --- PASS 3: Catch-up (Last to First)
            # Second attempt if still short on seats: allow extra shows (seat-driven) ignoring per-day show caps
            if total_capacity < seats_required:
                for d in reversed(days):
                    if total_capacity >= seats_required:
                        break

                    day_key = d.strftime("%Y-%m-%d")
                    candidate_starts = candidate_starts_for_date(d, show_len)
                    if not candidate_starts:
                        continue

                    pods_for_course = eligible_pods_for_course(course)

                    for pos_idx, start_min in enumerate(candidate_starts):
                        if total_capacity >= seats_required:
                            break

                        # do not restrict by show count here; allow any placements that fit
                        for podinfo in pods_sorted_for_slot(course):
                            pod, cap, grp = podinfo["pod"], podinfo["capacity"], podinfo["ops_group"]

                            if can_place(day_key, pod, grp, start_min, course, mod, d, show_len):
                                place(day_key, pod, grp, start_min, course, mod, show_len)
                                start_t, end_t = time_from_minutes(start_min), time_from_minutes(start_min + show_len)

                                schedule_rows.append({
                                    "Course": course, "Mod/Act": mod, "Date": day_key,
                                    "Start": start_t.strftime("%H:%M"), "End": end_t.strftime("%H:%M"),
                                    "Pod": pod, "Pod Capacity": cap, "Show Length": show_len,
                                })
                                total_capacity += cap
                                shows_for_pair += 1
                                day_shows_filled_map[day_key] = day_shows_filled_map.get(day_key, 0) + 1
                                break
        
        if total_capacity < seats_required:
            raise ValueError(
                f"Not enough capacity to schedule ({course}, {mod}) within {open_dt.date()}..{close_dt.date()} "
                f"under current constraints. Could only schedule {total_capacity} of {seats_required} seats."
            )
        
        summary_rows.append({
            "Course": course,
            "Mod/Act": mod,
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
    # CREATE OUTPUT DATAFRAMES
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