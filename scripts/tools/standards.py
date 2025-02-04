# standards.py

"""
This module provides standardized table names and related constants
for database operations across the project.
"""
# Dimension table definitions
def dim_trackers() -> str:
    """Returns the standardized name for the trackers dimension table."""
    return "dim_trackers"

def fct_trackers() -> str:
    """Returns the standardized name for the fact trackers table."""
    return "fct_trackers"


# Create a dictionary of default tables for easy import
DEFAULT_TABLES = {
    "DIM_TRACKERS": dim_trackers(),
    "FCT_TRACKERS": fct_trackers(),
}