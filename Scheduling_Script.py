import math
from datetime import datetime, timedelta, time
import pandas as pd
import openpyxl
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
    STEP_MIN,
    XSL_PATH
)
from test_scheduling import run_all_tests
# -----------------------------
# HELPER FUNCTIONS
# -----------------------------

def parse_sheet():
    df = openpyxl.load_workbook(XSL_PATH)
    ws = df["Spring 26 Highlevel and Academi"]

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
    return list(range(start_m, last_start + 1, step))


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

        # ops-team: no same end time within group  ✅ NEW
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
                    # Different activity: need 10-minute break
                    # Check if new show starts within 10 mins after existing show ends
                    if start_min < existing_end + BREAK_LEN and end_min > existing_start:
                        return False
                    # Check if existing show starts within 10 mins after new show ends
                    if existing_start < end_min + BREAK_LEN and existing_end > start_min:
                        return False
        
        return True
    
    def place(day_key: str, pod: str, ops_group: str, start_min: int,
            course: str, mod_act: str, show_len: int):
        end_min = start_min + show_len

        ops_used_starts.setdefault((day_key, ops_group), set()).add(start_min)
        ops_used_ends.setdefault((day_key, ops_group), set()).add(end_min)  # ✅ NEW

        all_bookings[(day_key, pod, start_min)] = (course, mod_act, end_min, show_len)
        pod_usage_count[pod] += 1
    
    def pods_sorted_for_slot():
        # Prefer higher capacity; then least-used to spread
        return sorted(PODS, key=lambda p: (-p["capacity"], pod_usage_count[p["pod"]]))
    
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
        
        # Calculate weights for Pass 1 (1 to N)
        num_days = len(days)
        total_weight = sum(range(1, num_days + 1))
        
        # --- PASS 1: Distributed (First to Last) ---
        # Try to fill each day up to its calculated target seat count
        for i, d in enumerate(days):
            if total_capacity >= seats_required:
                break
                
            day_weight = i + 1
            daily_target = math.ceil((day_weight / total_weight) * seats_required)
            day_capacity_filled = 0
            
            day_key = d.strftime("%Y-%m-%d")
            candidate_starts = candidate_starts_for_date(d, show_len)
            
            for start_min in candidate_starts:
                if total_capacity >= seats_required or day_capacity_filled >= daily_target:
                    break
                
                for podinfo in pods_sorted_for_slot():
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
                        day_capacity_filled += cap
                        break

        # --- PASS 2: Catch-up (Last to First) ---
        # If still need more seats, fill remaining starting from the end
        if total_capacity < seats_required:
            for d in reversed(days):
                if total_capacity >= seats_required:
                    break
                    
                day_key = d.strftime("%Y-%m-%d")
                candidate_starts = candidate_starts_for_date(d, show_len)
                
                for start_min in candidate_starts:
                    if total_capacity >= seats_required:
                        break
                    
                    for podinfo in pods_sorted_for_slot():
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
    # Define inputs
    # STUDENTS = {"Bio 181": 576, "Bio 100": 350}
    
    # DATA = [
    #     ("Bio 100", "M1 A1", "Friday, January 16, 2026", "Wednesday, January 28, 2026"),
    #     ("Bio 181", "M1 A1", "Friday, January 16, 2026", "Wednesday, January 28, 2026"),
    #     ("CHM 113", "CHM M1 A1", "Tuesday, January 20, 2026", "Monday, February 2, 2026"),
    #     ("Bio 100", "M1 A2", "Monday, January 26, 2026", "Wednesday, February 4, 2026"),
    #     ("Bio 181", "M1 A2", "Monday, January 26, 2026", "Wednesday, February 4, 2026"),
    #     ("Bio 182", "M4 A1", "Tuesday, January 27, 2026", "Thursday, February 5, 2026"),
    #     ("Bio 100", "M1 A3", "Monday, February 2, 2026", "Wednesday, February 11, 2026"),
    #     ("Bio 181", "M1 A3", "Monday, February 2, 2026", "Wednesday, February 11, 2026"),
    #     ("CHM 114", "CHM M1 A1", "Monday, February 2, 2026", "Wednesday, February 11, 2026"),
    # ]
    
    # HOLIDAYS = [
    #     datetime(2026, 1, 19).date(),  # Mon, 1/19/26
    #     datetime(2026, 3, 9).date(),   # Mon, 3/9/26
    #     datetime(2026, 3, 10).date(),  # Tue, 3/10/26
    #     datetime(2026, 3, 11).date(),  # Wed, 3/11/26
    #     datetime(2026, 3, 12).date(),  # Thu, 3/12/26
    #     datetime(2026, 3, 13).date(),  # Fri, 3/13/26
    # ]

    DATA, HOLIDAYS, STUDENTS = parse_data()
    
    # Call the schedule function
    schedule_df, summary_df = schedule(
        students=STUDENTS,
        data=DATA,
        holidays=HOLIDAYS
    )
    
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
    
    print(f"\n✅ Successfully scheduled {len(schedule_df)} shows.")
    print("Note: Operational hours - 9 AM to 5 PM (Mon-Thu), 9 AM to 1 PM (Fri)")
    print(f"Note: No scheduling on holidays: {', '.join([h.strftime('%m/%d/%y') for h in HOLIDAYS])}")