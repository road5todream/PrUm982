"""
Читаемые имена для бинарных enum полей NAV (BESTNAV/ADRNAV): как в ASCII OEM7/Unicore.
Неизвестные коды возвращаются как str(code).
"""
from __future__ import annotations

from typing import Dict

_SOLUTION_STATUS: Dict[int, str] = {
    -1: "NONE",
    0: "SOL_COMPUTED",
    1: "INSUFFICIENT_OBS",
    2: "NO_CONVERGENCE",
    3: "SINGULARITY",
    4: "COV_TRACE",
    5: "TEST_DIST",
    6: "COLD_START",
    7: "V_H_LIMIT",
    8: "VARIANCE",
    9: "RESIDUALS",
    13: "INTEGRITY_WARNING",
    18: "PENDING",
    19: "INVALID_FIX",
    20: "UNAUTHORIZED",
    22: "INVALID_RATE",
    23: "INSUFFICIENT_ACCURACY",
    24: "AUTHORIZING",
}

_POSITION_VELOCITY_TYPE: Dict[int, str] = {
    0: "NONE",
    1: "FIXEDPOS",
    2: "FIXEDHEIGHT",
    8: "DOPPLER_VELOCITY",
    16: "SINGLE",
    17: "PSRDIFF",
    18: "WAAS",
    32: "L1_FLOAT",
    33: "IONOFREE_FLOAT",
    34: "GOFLOAT",
    48: "L1_INT",
    49: "WIDE_INT",
    50: "NARROW_INT",
    51: "CODELINE",
    52: "SUPER_WIDE_LANE",
    53: "L1_HALF_CYCLE_PENDING",
    54: "OVERRDETERMINED",
    64: "PPP_CONVERGING",
    68: "PPP",
    73: "INS_PSRSP",
    74: "INS_RTKFLOAT",
    75: "INS_RTKFIXED",
    76: "INS_DR",
}

_DATUM_ID: Dict[int, str] = {
    0: "UNKNOWN",
    61: "WGS84",
    62: "USER",
    63: "MARK1",
    64: "MARK2",
}


def solution_status_to_str(code: int) -> str:
    return _SOLUTION_STATUS.get(code, str(code))


def position_velocity_type_to_str(code: int) -> str:
    return _POSITION_VELOCITY_TYPE.get(code, str(code))


def datum_id_to_str(code: int) -> str:
    return _DATUM_ID.get(code, str(code))
