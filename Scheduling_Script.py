import math
from datetime import datetime, timedelta, time
import pandas as pd

# -----------------------------
# INPUTS
# -----------------------------
BUFFER_PCT = 0.10

STUDENTS = {"Bio 181": 576, "Bio 100": 350}
DEFAULT_STUDENTS_OTHER = 200

PODS = [
    {"pod": "CRTVC 1", "capacity": 6,  "ops_group": "B"},
    {"pod": "CRTVC 2", "capacity": 6,  "ops_group": "B"},
    {"pod": "CRTVC 3", "capacity": 24, "ops_group": "A"},
    {"pod": "CRTVC 4", "capacity": 24, "ops_group": "A"},
    {"pod": "CRTVC 5", "capacity": 28, "ops_group": "B"},  # 27 + 1
    {"pod": "CRTVC 6", "capacity": 28, "ops_group": "B"},  # 27 + 1
]

DATA = [
    ("Bio 100", "M1 A1", "Friday, January 16, 2026", "Wednesday, January 28, 2026"),
    ("Bio 181", "M1 A1", "Friday, January 16, 2026", "Wednesday, January 28, 2026"),
    ("CHM 113", "CHM M1 A1", "Tuesday, January 20, 2026", "Monday, February 2, 2026"),
    ("Bio 100", "M1 A2", "Monday, January 26, 2026", "Wednesday, February 4, 2026"),
    ("Bio 181", "M1 A2", "Monday, January 26, 2026", "Wednesday, February 4, 2026"),
    ("Bio 182", "M4 A1", "Tuesday, January 27, 2026", "Thursday, February 5, 2026"),
    ("Bio 100", "M1 A3", "Monday, February 2, 2026", "Wednesday, February 11, 2026"),
    ("Bio 181", "M1 A3", "Monday, February 2, 2026", "Wednesday, February 11, 2026"),
    ("CHM 114", "CHM M1 A1", "Monday, February 2, 2026", "Wednesday, February 11, 2026"),
]

SHOW_LEN = 20  # minutes
BREAK_LEN = 10  # minutes break between different activities in same pod
START_HOUR = time(9, 0)
END_HOUR_REGULAR = time(17, 0)  # Regular closing time (Mon-Thu)
END_HOUR_FRIDAY = time(13, 0)   # Early closing on Fridays (1 PM)
STEP_MIN = 5  # 5-min grid

# -----------------------------
# HELPERS
# -----------------------------
def parse_date(s: str) -> datetime:
    return pd.to_datetime(s).to_pydatetime().replace(hour=0, minute=0, second=0, microsecond=0)

def business_days_inclusive(start: datetime, end: datetime):
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri only
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

def candidate_starts_for_date(date: datetime, step=STEP_MIN):
    """Generate candidate start times based on day of week."""
    start_m = minutes(START_HOUR)
    end_hour = get_end_hour_for_date(date)
    last_start = minutes(end_hour) - SHOW_LEN
    return list(range(start_m, last_start + 1, step))

# -----------------------------
# STATE (global across all modules)
# -----------------------------
# Shared-ops constraint: pods in same ops_group cannot share the same start time
ops_used_starts = {}  # (day_key, ops_group) -> set(start_min)

# Track all bookings with their details
# Format: (day_key, pod, start_min) -> (course, mod_act, end_min)
all_bookings = {}

# Soft balancing: spread usage across pods (tie-breaker)
pod_usage_count = {p["pod"]: 0 for p in PODS}

def can_place(day_key: str, pod: str, ops_group: str, start_min: int, course: str, mod_act: str, date_obj: datetime) -> bool:
    # ops-team: no same start time within group
    if start_min in ops_used_starts.get((day_key, ops_group), set()):
        return False
    
    end_min = start_min + SHOW_LEN
    
    # Check if show would end after closing time
    end_hour = get_end_hour_for_date(date_obj)
    if end_min > minutes(end_hour):
        return False
    
    # Check against all existing bookings in the same pod
    for (d, p, existing_start), details in all_bookings.items():
        if d == day_key and p == pod:
            existing_course, existing_mod_act, existing_end = details
            
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

def place(day_key: str, pod: str, ops_group: str, start_min: int, course: str, mod_act: str):
    end_min = start_min + SHOW_LEN
    ops_used_starts.setdefault((day_key, ops_group), set()).add(start_min)
    all_bookings[(day_key, pod, start_min)] = (course, mod_act, end_min)
    pod_usage_count[pod] += 1

def pods_sorted_for_slot():
    # Prefer higher capacity; then least-used to spread
    return sorted(PODS, key=lambda p: (-p["capacity"], pod_usage_count[p["pod"]]))

# -----------------------------
# BUILD WINDOWS (Course+Mod/Act)
# -----------------------------
df = pd.DataFrame(DATA, columns=["Course", "Mod/Act", "Open Date", "Close Date"])
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

    students = STUDENTS.get(course, DEFAULT_STUDENTS_OTHER)
    seats_required = math.ceil(students * (1.0 + BUFFER_PCT))

    days = business_days_inclusive(open_dt, close_dt)
    if not days:
        raise ValueError(f"No business days in window for ({course}, {mod}).")

    total_capacity = 0
    shows_for_pair = 0
    
    # Try each day in order
    for d in days:
        if total_capacity >= seats_required:
            break
            
        day_key = d.strftime("%Y-%m-%d")
        candidate_starts = candidate_starts_for_date(d)
        
        # Try each time slot
        for start_min in candidate_starts:
            if total_capacity >= seats_required:
                break
            
            # Try each pod
            for podinfo in pods_sorted_for_slot():
                pod = podinfo["pod"]
                cap = podinfo["capacity"]
                grp = podinfo["ops_group"]
                
                if can_place(day_key, pod, grp, start_min, course, mod, d):
                    place(day_key, pod, grp, start_min, course, mod)
                    
                    start_t = time_from_minutes(start_min)
                    end_t = time_from_minutes(start_min + SHOW_LEN)
                    
                    schedule_rows.append({
                        "Course": course,
                        "Mod/Act": mod,
                        "Date": day_key,
                        "Start": start_t.strftime("%H:%M"),
                        "End": end_t.strftime("%H:%M"),
                        "Pod": pod,
                        "Pod Capacity": cap,
                    })
                    
                    total_capacity += cap
                    shows_for_pair += 1
                    break  # Found a pod for this time slot
    
    if total_capacity < seats_required:
        raise ValueError(
            f"Not enough capacity to schedule ({course}, {mod}) within {open_dt.date()}..{close_dt.date()} "
            f"under current constraints. Could only schedule {total_capacity} of {seats_required} seats."
        )
    
    summary_rows.append({
        "Course": course,
        "Mod/Act": mod,
        "Students": students,
        "Buffer%": BUFFER_PCT,
        "Seats Required": seats_required,
        "Scheduled Seats": total_capacity,
        "Shows Scheduled": shows_for_pair,
        "Open": open_dt.date().isoformat(),
        "Close": close_dt.date().isoformat(),
    })

# -----------------------------
# OUTPUT
# -----------------------------
schedule_df = pd.DataFrame(schedule_rows).sort_values(["Date", "Start", "Pod"]).reset_index(drop=True)
summary_df = pd.DataFrame(summary_rows).sort_values(["Course", "Mod/Act"]).reset_index(drop=True)

print("=== SUMMARY ===")
print(summary_df.to_string(index=False))

print("\n=== SCHEDULE ===")
print(schedule_df.to_string(index=False))

# Ensure proper sorting first
schedule_df_sorted = schedule_df.sort_values(
    ["Course", "Mod/Act", "Date", "Start"]
)

# Print grouped output
for (course, mod), group in schedule_df_sorted.groupby(["Course", "Mod/Act"]):
    print(f"\n===== {course} | {mod} =====")
    print(group.to_string(index=False))

# Optional exports:
schedule_df.to_csv("show_schedule.csv", index=False)
summary_df.to_csv("show_summary.csv", index=False)

print(f"\n✅ Successfully scheduled {len(schedule_rows)} shows.")
print("Note: Operational hours - 9 AM to 5 PM (Mon-Thu), 9 AM to 1 PM (Fri)")