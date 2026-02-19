"""
Configuration constants for the scheduling system.
"""
from datetime import time

# -----------------------------
# GLOBAL CONFIGURATION
# -----------------------------
BUFFER_PCT = 0.10
DEFAULT_STUDENTS_OTHER = 200

PODS = [
    {"pod": "CRTVC 1", "capacity": 6,  "ops_group": "B"},
    {"pod": "CRTVC 2", "capacity": 6,  "ops_group": "B"},
    {"pod": "CRTVC 3", "capacity": 24, "ops_group": "A"},
    {"pod": "CRTVC 4", "capacity": 24, "ops_group": "A"},
    {"pod": "CRTVC 5", "capacity": 28, "ops_group": "B"},  # 27 + 1
    {"pod": "CRTVC 6", "capacity": 28, "ops_group": "B"},  # 27 + 1
]

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
BREAK_LEN = 10  # minutes break between different activities in same pod
START_HOUR = time(9, 0)
END_HOUR_REGULAR = time(17, 0)  # Regular closing time (Mon-Thu)
END_HOUR_FRIDAY = time(13, 0)   # Early closing on Fridays (1 PM)
STEP_MIN = 5  # 5-min grid

XSL_PATH="/Users/karankamboj/Documents/Work/Scheduling/data.xlsx"

# Testing Constants

# Constants for tests (derived from central config where possible)
BREAK_LEN_MIN = BREAK_LEN  # 10-minute break between different mod/acts

# Mappings derived from PODS
POD_CAPACITY = {p["pod"]: p["capacity"] for p in PODS}
OPS_GROUP = {p["pod"]: p["ops_group"] for p in PODS}

WORK_START_MIN = 9 * 60      # 09:00
WORK_END_MIN_REGULAR = 17 * 60  # 17:00 (Mon-Thu)
WORK_END_MIN_FRIDAY = 13 * 60   # 13:00 (Friday)