from Scheduling_Script import schedule
from datetime import datetime

# Define inputs
STUDENTS = {"Bio 181": 576, "Bio 100": 350}

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

HOLIDAYS = [
    datetime(2026, 1, 19).date(),  # Mon, 1/19/26
    datetime(2026, 3, 9).date(),   # Mon, 3/9/26
    datetime(2026, 3, 10).date(),  # Tue, 3/10/26
    datetime(2026, 3, 11).date(),  # Wed, 3/11/26
    datetime(2026, 3, 12).date(),  # Thu, 3/12/26
    datetime(2026, 3, 13).date(),  # Fri, 3/13/26
]

# Call the schedule function
schedule_df, summary_df = schedule(
    students=STUDENTS,
    data=DATA,
    holidays=HOLIDAYS
)
import pandas as pd

# Show durations mapping based on course prefix
SHOW_LENGTH_MAP = {
    "Bio": 30,      # Bio -> 30 mins
    "CHM": 30,      # Chm -> 30 mins  
    "Astronomy": 30, # Astronomy -> 30 mins
    "Art": 30,      # Art -> 30 mins
    "Scm": 20,      # Scm -> 20 mins
    # Default will be 20 minutes
}

DEFAULT_SHOW_LEN = 20  # minutes for courses not in the map
BREAK_LEN_MIN = 10  # 10-minute break between different mod/acts

WORK_START_MIN = 9 * 60      # 09:00
WORK_END_MIN_REGULAR = 17 * 60  # 17:00 (Mon-Thu)
WORK_END_MIN_FRIDAY = 13 * 60   # 13:00 (Friday)

POD_CAPACITY = {
    "CRTVC 1": 6,
    "CRTVC 2": 6,
    "CRTVC 3": 24,
    "CRTVC 4": 24,
    "CRTVC 5": 28,  # 27 + 1
    "CRTVC 6": 28,  # 27 + 1
}

OPS_GROUP = {
    "CRTVC 1": "B",
    "CRTVC 2": "B",
    "CRTVC 3": "A",
    "CRTVC 4": "A",
    "CRTVC 5": "B",
    "CRTVC 6": "B",
}

STUDENTS = {"Bio 181": 576, "Bio 100": 350}
DEFAULT_STUDENTS_OTHER = 200
BUFFER_PCT = 0.10

# HOLIDAYS (No scheduling on these dates)
HOLIDAYS = [
    pd.Timestamp(2026, 1, 19).date(),  # Mon, 1/19/26
    pd.Timestamp(2026, 3, 9).date(),   # Mon, 3/9/26
    pd.Timestamp(2026, 3, 10).date(),  # Tue, 3/10/26
    pd.Timestamp(2026, 3, 11).date(),  # Wed, 3/11/26
    pd.Timestamp(2026, 3, 12).date(),  # Thu, 3/12/26
    pd.Timestamp(2026, 3, 13).date(),  # Fri, 3/13/26
]

def _prep(schedule_df: pd.DataFrame) -> pd.DataFrame:
    df = schedule_df.copy()

    # Ensure types
    df["Date"] = pd.to_datetime(df["Date"])
    df["Date_only"] = df["Date"].dt.date
    df["Start_dt"] = pd.to_datetime(df["Start"], format="%H:%M")
    df["End_dt"] = pd.to_datetime(df["End"], format="%H:%M")

    # Convert to minutes from midnight for easy interval math
    df["Start_min"] = df["Start_dt"].dt.hour * 60 + df["Start_dt"].dt.minute
    df["End_min"] = df["End_dt"].dt.hour * 60 + df["End_dt"].dt.minute
    
    # Calculate show length
    df["Show Length"] = df["End_min"] - df["Start_min"]
    
    # Add day of week (0=Monday, 4=Friday)
    df["DayOfWeek"] = df["Date"].dt.weekday

    return df

def get_expected_show_length(course: str) -> int:
    """Determine expected show length based on course prefix mapping."""
    # Extract the first word (prefix) from the course name
    prefix = course.split()[0] if course else ""
    
    # Look up in the mapping, return default if not found
    return SHOW_LENGTH_MAP.get(prefix, DEFAULT_SHOW_LEN)

def test_capacity(schedule_df: pd.DataFrame):
    """
    1) Capacity:
      - Every Course+Mod/Act has enough scheduled seats to cover students + buffer
      - Pod capacity column matches the defined mapping
    """
    df = _prep(schedule_df)

    # Pod capacity correctness per row
    bad_caps = df[df.apply(lambda r: POD_CAPACITY.get(r["Pod"]) != int(r["Pod Capacity"]), axis=1)]
    assert bad_caps.empty, f"Pod capacity mismatch rows:\n{bad_caps[['Date','Start','Pod','Pod Capacity']]}"

    # Total seats per (Course, Mod/Act)
    seats = (
        df.groupby(["Course", "Mod/Act"])["Pod Capacity"]
          .sum()
          .reset_index(name="ScheduledSeats")
    )

    def required_seats(course: str) -> int:
        n = STUDENTS.get(course, DEFAULT_STUDENTS_OTHER)
        return int((n * (1.0 + BUFFER_PCT) + 0.9999999))  # ceil without math import

    seats["RequiredSeats"] = seats["Course"].apply(required_seats)

    insufficient = seats[seats["ScheduledSeats"] < seats["RequiredSeats"]]
    assert insufficient.empty, f"Insufficient seats:\n{insufficient}"

    # Allow at most ONE extra show worth of seats beyond requirement.
    # This prevents cases like 60 students but 10 shows of 20 seats.
    MAX_POD_CAP = max(POD_CAPACITY.values())

    too_much = seats[seats["ScheduledSeats"] > (seats["RequiredSeats"] + MAX_POD_CAP)]
    assert too_much.empty, (
        "Overscheduled seats (more than 1 extra show worth beyond requirement):\n"
        f"{too_much}"
    )


def test_no_overlap_of_pods(schedule_df: pd.DataFrame):
    """
    2) No overlap of pods (within same pod on same date, show intervals must not overlap).
    """
    df = _prep(schedule_df)

    violations = []
    for (date, pod), g in df.groupby(["Date_only", "Pod"]):
        g = g.sort_values("Start_min")
        prev_end = None
        prev_row = None
        for _, row in g.iterrows():
            if prev_end is not None and row["Start_min"] < prev_end:
                violations.append((date, pod, prev_row["Start"], prev_row["End"], row["Start"], row["End"]))
            prev_end = row["End_min"]
            prev_row = row

    assert not violations, (
        "Overlap detected (Date, Pod, PrevStart, PrevEnd, Start, End):\n"
        + "\n".join(map(str, violations[:30]))
    )


def test_ops_team_start_end_uniqueness(schedule_df: pd.DataFrame):
    """
    3) Shows are not scheduled to start OR end at the same time in pods sharing ops teams.
       - Team A: CRTVC 3,4
       - Team B: CRTVC 1,2,5,6
    """
    df = _prep(schedule_df)

    # Add ops group
    df["OpsGroup"] = df["Pod"].map(OPS_GROUP)

    # Check unique start times per (Date, OpsGroup)
    for (date, grp), g in df.groupby(["Date_only", "OpsGroup"]):
        starts = g["Start_min"].tolist()
        if len(starts) != len(set(starts)):
            dup = g[g.duplicated("Start_min", keep=False)][["Course","Mod/Act","Pod","Start","End","Date"]]
            raise AssertionError(f"Duplicate START time in ops group {grp} on {date}:\n{dup.to_string(index=False)}")

    # Check unique end times per (Date, OpsGroup)
    for (date, grp), g in df.groupby(["Date_only", "OpsGroup"]):
        ends = g["End_min"].tolist()
        if len(ends) != len(set(ends)):
            dup = g[g.duplicated("End_min", keep=False)][["Course","Mod/Act","Pod","Start","End","Date"]]
            raise AssertionError(f"Duplicate END time in ops group {grp} on {date}:\n{dup.to_string(index=False)}")


def test_show_runtime_correctness(schedule_df: pd.DataFrame):
    """
    4) Every show must have the correct duration based on course prefix mapping.
    """
    df = _prep(schedule_df)

    violations = []
    for _, row in df.iterrows():
        expected_len = get_expected_show_length(row["Course"])
        actual_len = row["Show Length"]
        
        if actual_len != expected_len:
            violations.append((
                row["Date_only"], row["Pod"], row["Course"], row["Mod/Act"],
                row["Start"], row["End"], f"Expected {expected_len} min, got {actual_len} min"
            ))
    
    assert not violations, (
        "Shows with incorrect duration:\n"
        + "\n".join([f"Date: {v[0]}, Pod: {v[1]}, Course: {v[2]}, Mod/Act: {v[3]}, "
                     f"Time: {v[4]}-{v[5]}, Issue: {v[6]}" for v in violations])
    )


def test_weekdays_only(schedule_df: pd.DataFrame):
    """
    Dates must be Mon–Fri (no Sat/Sun).
    """
    df = schedule_df.copy()
    df["Date_dt"] = pd.to_datetime(df["Date"])

    # weekday: Mon=0 ... Sun=6
    weekend = df[df["Date_dt"].dt.weekday >= 5]
    assert weekend.empty, (
        "Found shows scheduled on weekend:\n"
        + weekend[["Course","Mod/Act","Date","Start","End","Pod"]].to_string(index=False)
    )


def test_no_shows_on_holidays(schedule_df: pd.DataFrame):
    """
    No shows should be scheduled on holidays.
    """
    df = schedule_df.copy()
    df["Date_dt"] = pd.to_datetime(df["Date"])
    df["Date_only"] = df["Date_dt"].dt.date
    
    holiday_shows = df[df["Date_only"].isin(HOLIDAYS)]
    
    assert holiday_shows.empty, (
        "Found shows scheduled on holidays:\n"
        + holiday_shows[["Course","Mod/Act","Date","Start","End","Pod"]].to_string(index=False)
        + f"\n\nHolidays: {', '.join([h.strftime('%m/%d/%y') for h in HOLIDAYS])}"
    )


def test_within_working_hours(schedule_df: pd.DataFrame):
    """
    Shows must be within operational hours:
    - Mon-Thu: 9am–5pm
    - Friday: 9am–1pm
    """
    df = _prep(schedule_df)

    violations = []
    for _, row in df.iterrows():
        start_min = row["Start_min"]
        end_min = row["End_min"]
        day_of_week = row["DayOfWeek"]
        
        # Check start time (always 9:00 AM)
        if start_min < WORK_START_MIN:
            violations.append((row["Date_only"], row["Pod"], row["Course"], row["Mod/Act"], 
                             row["Start"], row["End"], "Starts before 9:00 AM"))
        
        # Check end time based on day of week
        if day_of_week == 4:  # Friday
            if end_min > WORK_END_MIN_FRIDAY:
                violations.append((row["Date_only"], row["Pod"], row["Course"], row["Mod/Act"], 
                                 row["Start"], row["End"], f"Ends after 13:00 on Friday"))
        else:  # Monday-Thursday
            if end_min > WORK_END_MIN_REGULAR:
                violations.append((row["Date_only"], row["Pod"], row["Course"], row["Mod/Act"], 
                                 row["Start"], row["End"], f"Ends after 17:00"))

    assert not violations, (
        "Found shows outside operational hours:\n"
        + "\n".join([f"Date: {v[0]}, Pod: {v[1]}, Course: {v[2]}, Mod/Act: {v[3]}, "
                     f"Time: {v[4]}-{v[5]}, Issue: {v[6]}" for v in violations])
    )


def test_break_between_different_mod_acts(schedule_df: pd.DataFrame):
    """
    8) Test that shows in the same pod have at least 10-minute break 
    between them when they are for different modules/activities.
    """
    df = _prep(schedule_df)
    
    violations = []
    for (date, pod), g in df.groupby(["Date_only", "Pod"]):
        g = g.sort_values("Start_min")
        for i in range(len(g) - 1):
            current_row = g.iloc[i]
            next_row = g.iloc[i + 1]
            
            # Check if same course and module/activity
            same_course = current_row["Course"] == next_row["Course"]
            same_mod_act = current_row["Mod/Act"] == next_row["Mod/Act"]
            same_activity = same_course and same_mod_act
            
            if not same_activity:
                # Different activity - need at least 10 min break
                time_between = next_row["Start_min"] - current_row["End_min"]
                if time_between < BREAK_LEN_MIN:
                    violations.append((
                        date, pod,
                        current_row["Course"], current_row["Mod/Act"],
                        current_row["Start"], current_row["End"],
                        next_row["Course"], next_row["Mod/Act"],
                        next_row["Start"], next_row["End"],
                        f"Only {time_between} min between different activities"
                    ))
            else:
                # Same activity - can be back-to-back (no break needed)
                # Just ensure they don't overlap
                time_between = next_row["Start_min"] - current_row["End_min"]
                if time_between < 0:
                    violations.append((
                        date, pod,
                        current_row["Course"], current_row["Mod/Act"],
                        current_row["Start"], current_row["End"],
                        next_row["Course"], next_row["Mod/Act"],
                        next_row["Start"], next_row["End"],
                        f"Overlap of {abs(time_between)} min for same activity"
                    ))
    
    assert not violations, (
        "Break requirement violations in same pod:\n"
        + "\n".join([f"Date: {v[0]}, Pod: {v[1]}, "
                     f"First: {v[2]} {v[3]} ({v[4]}-{v[5]}), "
                     f"Second: {v[6]} {v[7]} ({v[8]}-{v[9]}), "
                     f"Issue: {v[10]}" for v in violations[:20]])
    )


def run_all_tests(schedule_df: pd.DataFrame):
    test_capacity(schedule_df)
    test_no_overlap_of_pods(schedule_df)
    test_ops_team_start_end_uniqueness(schedule_df)
    test_show_runtime_correctness(schedule_df)
    test_within_working_hours(schedule_df)
    test_weekdays_only(schedule_df)
    test_no_shows_on_holidays(schedule_df)
    test_break_between_different_mod_acts(schedule_df)
    print("✅ All constraint tests passed!")


run_all_tests(schedule_df)