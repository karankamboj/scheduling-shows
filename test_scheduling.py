from Scheduling_Script import schedule_df

import pandas as pd

SHOW_LEN_MIN = 20

WORK_START_MIN = 9 * 60      # 09:00
WORK_END_MIN = 17 * 60       # 17:00

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


def _prep(schedule_df: pd.DataFrame) -> pd.DataFrame:
    df = schedule_df.copy()

    # Ensure types
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df["Start_dt"] = pd.to_datetime(df["Start"], format="%H:%M")
    df["End_dt"] = pd.to_datetime(df["End"], format="%H:%M")

    # Convert to minutes from midnight for easy interval math
    df["Start_min"] = df["Start_dt"].dt.hour * 60 + df["Start_dt"].dt.minute
    df["End_min"] = df["End_dt"].dt.hour * 60 + df["End_dt"].dt.minute

    return df


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
    for (date, pod), g in df.groupby(["Date", "Pod"]):
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
    for (date, grp), g in df.groupby(["Date", "OpsGroup"]):
        starts = g["Start_min"].tolist()
        if len(starts) != len(set(starts)):
            dup = g[g.duplicated("Start_min", keep=False)][["Course","Mod/Act","Pod","Start","End","Date"]]
            raise AssertionError(f"Duplicate START time in ops group {grp} on {date}:\n{dup.to_string(index=False)}")

    # Check unique end times per (Date, OpsGroup)
    for (date, grp), g in df.groupby(["Date", "OpsGroup"]):
        ends = g["End_min"].tolist()
        if len(ends) != len(set(ends)):
            dup = g[g.duplicated("End_min", keep=False)][["Course","Mod/Act","Pod","Start","End","Date"]]
            raise AssertionError(f"Duplicate END time in ops group {grp} on {date}:\n{dup.to_string(index=False)}")


def test_show_runtime_20min(schedule_df: pd.DataFrame):
    """
    4) Every show must be exactly 20 minutes.
    """
    df = _prep(schedule_df)

    bad = df[(df["End_min"] - df["Start_min"]) != SHOW_LEN_MIN]
    assert bad.empty, (
        "Shows with non-20-min duration:\n"
        + bad[["Course","Mod/Act","Date","Pod","Start","End"]].to_string(index=False)
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

def test_within_working_hours(schedule_df: pd.DataFrame):
    """
    Shows must be within 9am–5pm.
    """
    df = schedule_df.copy()

    start_dt = pd.to_datetime(df["Start"], format="%H:%M")
    end_dt = pd.to_datetime(df["End"], format="%H:%M")

    df["Start_min"] = start_dt.dt.hour * 60 + start_dt.dt.minute
    df["End_min"] = end_dt.dt.hour * 60 + end_dt.dt.minute

    bad = df[(df["Start_min"] < WORK_START_MIN) | (df["End_min"] > WORK_END_MIN)]
    assert bad.empty, (
        "Found shows outside working hours (09:00–17:00):\n"
        + bad[["Course","Mod/Act","Date","Start","End","Pod"]].to_string(index=False)
    )

def run_all_tests(schedule_df: pd.DataFrame):
    test_capacity(schedule_df)
    test_no_overlap_of_pods(schedule_df)
    test_ops_team_start_end_uniqueness(schedule_df)
    test_show_runtime_20min(schedule_df)
    test_within_working_hours(schedule_df)
    test_weekdays_only(schedule_df)
    print("✅ All constraint tests passed!")

run_all_tests(schedule_df)
