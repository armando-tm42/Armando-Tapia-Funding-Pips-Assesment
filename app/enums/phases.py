"""
Phase enums for trading account levels
"""
from enum import IntEnum

class Phase(IntEnum):
    """Trading account phase enum with integer values"""
    STUDENT = 0
    PRACTITIONER = 1
    SENIOR = 2
    MASTER = 3 