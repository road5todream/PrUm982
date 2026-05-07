"""Остальные data output команды (не OBSV*): AGRIC, BASEINFO, BESTNAV, log, unlog и т.д."""
import re
import struct
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from um982.core import Um982Core
from um982.utils import parse_unicore_header

from .base import _run_data_query, _make_unicore_header_checker
from .common import UNICORE_SYNC


# --- AGRIC / HWSTATUS / AGC / MODE / DOP ведомый (2121) ---

AGRIC_MESSAGE_ID = 11276
# Тело после 24-байтного заголовка кадра, без 4-байтного CRC кадра (рук. §7.3.21, до внутреннего CRC 228+4).
_AGRIC_BODY_PARSE_MIN = 228

# HWSTATUS (§7.3.54, message ID 218): 40 байт полей + 4 байта внутреннего CRC в теле кадра.
HWSTATUS_MESSAGE_ID = 218
_HWSTATUS_BODY_WITH_INNER_CRC = 44
_HWSTATUS_BODY_FIELDS = 40
_HWSTATUS_STRUCT = struct.Struct("<ifffIffBBHII")


def _agric_binary_total_len_candidates(message_length: int) -> List[int]:
    """
    Варианты полной длины кадра Unicore (от 0xAA до конца 4-байтного CRC включительно).

    По документации message_length = header + data + CRC. На приёмниках иногда встречается:
    - длина без финального CRC кадра (нужно +4);
    - в поле попадает «длина команды» из тела AGRIC (228 или 232) — тогда полный кадр 24+payload+4.
    """
    out: List[int] = []
    seen: set[int] = set()
    ml = int(message_length)
    if ml <= 0 or ml > 8192:
        return out

    def add(t: int) -> None:
        if 52 <= t <= 8192 and t not in seen:
            seen.add(t)
            out.append(t)

    add(ml)
    add(ml + 4)
    for pl in (228, 232):
        add(24 + pl + 4)
    return out


def _agric_body_from_frame(data: bytes, sync_off: int, total_len: int) -> Optional[bytes]:
    """Байты полезной нагрузки AGRIC: сразу после 24-байтного заголовка, без внешнего CRC кадра."""
    if total_len < 52 or len(data) < sync_off + total_len:
        return None
    end = sync_off + total_len - 4
    if end < sync_off + 24:
            return None
    return data[sync_off + 24 : end]


def _agric_try_unpack_frame(data: bytes, sync_off: int, header_message_length: int) -> Optional[Tuple[Dict[str, Any], int, int]]:
    """
    Подобрать total_len и разобрать AGRIC. Возвращает (dict, total_len, outer_crc_u32) или None.
    """
    for total in _agric_binary_total_len_candidates(header_message_length):
        if len(data) < sync_off + total:
            continue
        body = _agric_body_from_frame(data, sync_off, total)
        if not body:
                    continue
        unpacked = _unpack_agric_body(body)
        if not unpacked:
                    continue
        crc_val = struct.unpack("<I", data[sync_off + total - 4 : sync_off + total])[0]
        return unpacked, total, crc_val
    return None


def _hwstatus_binary_total_len_candidates(message_length: int) -> List[int]:
    """
    Полная длина кадра Unicore (sync…внешний CRC). Поле message_length иногда без финальных +4
    или совпадает с «логической» длиной; для HWSTATUS типичны 68 (24+40+4) и 72 (24+44+4).
    """
    out: List[int] = []
    seen: set[int] = set()
    ml = int(message_length)
    if ml <= 0 or ml > 8192:
        return out

    def add(t: int) -> None:
        if 52 <= t <= 8192 and t not in seen:
            seen.add(t)
            out.append(t)

    add(ml)
    add(ml + 4)
    add(24 + _HWSTATUS_BODY_FIELDS + 4)
    add(24 + _HWSTATUS_BODY_WITH_INNER_CRC + 4)
    return out


def _hwstatus_body_from_frame(data: bytes, sync_off: int, total_len: int) -> Optional[bytes]:
    """Тело HWSTATUS: байты сразу после 24-байтного заголовка, без внешнего CRC кадра."""
    if total_len < 52 or len(data) < sync_off + total_len:
        return None
    end = sync_off + total_len - 4
    if end < sync_off + 24:
        return None
    return data[sync_off + 24 : end]


def _unpack_hwstatus_body40(blob40: bytes, *, inner_crc_u32: Optional[int] = None) -> Dict[str, Any]:
    (
        temp1,
        dc09,
        dc10,
        dc18,
        clockflag,
        clock_drift,
        reserved1,
        hw_flag,
        reserved2,
        pll_lock,
        reserved3,
        reserved4,
    ) = _HWSTATUS_STRUCT.unpack(blob40[:40])
    hw_flag = int(hw_flag) & 0xFF
    hw_flag_bits = {
        "oscillator_type": (hw_flag >> 0) & 1,
        "vcxo_tcxo": (hw_flag >> 1) & 1,
        "osc_freq": (hw_flag >> 2) & 1,
        "osc_crystal_support": (hw_flag >> 3) & 1,
        "check_status": (hw_flag >> 7) & 1,
    }
    out: Dict[str, Any] = {
        "temp1": int(temp1),
        "temp1_celsius": float(temp1) / 1000.0,
        "dc09": float(dc09),
        "dc10": float(dc10),
        "dc18": float(dc18),
        "clockflag": int(clockflag),
        "clockflag_valid": int(clockflag) == 1,
        "clock_drift": float(clock_drift),
        "hw_flag": hw_flag,
        "hw_flag_hex": f"0x{hw_flag:02X}",
        "hw_flag_bits": hw_flag_bits,
        "pll_lock": int(pll_lock) & 0xFFFF,
        "pll_lock_hex": f"0x{int(pll_lock) & 0xFFFF:04X}",
        "reserved": {
            "reserved1": float(reserved1),
            "reserved2": int(reserved2) & 0xFF,
            "reserved3": int(reserved3),
            "reserved4": int(reserved4),
        },
    }
    if inner_crc_u32 is not None:
        out["inner_crc"] = f"0x{inner_crc_u32:08X}"
    return out


def _hwstatus_try_unpack_frame(
    data: bytes, sync_off: int, header_message_length: int
) -> Optional[Tuple[Dict[str, Any], int, int]]:
    """Подобрать total_len и разобрать HWSTATUS. Возвращает (поля, total_len, внешний CRC u32) или None."""
    for total in _hwstatus_binary_total_len_candidates(header_message_length):
        if len(data) < sync_off + total:
            continue
        body = _hwstatus_body_from_frame(data, sync_off, total)
        if not body:
            continue
        inner: Optional[int] = None
        if len(body) >= _HWSTATUS_BODY_WITH_INNER_CRC:
            blob40 = body[:_HWSTATUS_BODY_FIELDS]
            inner = struct.unpack("<I", body[_HWSTATUS_BODY_FIELDS : _HWSTATUS_BODY_WITH_INNER_CRC])[0]
        elif len(body) >= _HWSTATUS_BODY_FIELDS:
            blob40 = body[:_HWSTATUS_BODY_FIELDS]
        else:
            continue
        try:
            unpacked = _unpack_hwstatus_body40(blob40, inner_crc_u32=inner)
        except struct.error:
            continue
        crc_val = struct.unpack("<I", data[sync_off + total - 4 : sync_off + total])[0]
        return unpacked, total, crc_val
    return None


_HWSTATUS_ASCII_COMPLETE_RE = re.compile(
    r"#HWSTATUS[AB].*?\*[0-9a-fA-F]{8}",
    re.DOTALL | re.IGNORECASE,
)


def _parse_hwstatus_ascii_tokens(tokens: List[str]) -> Optional[Dict[str, Any]]:
    """Поля после «;» в ASCII HWSTATUS (§7.3.54), порядок как в бинарной структуре."""
    if len(tokens) < 9:
        return None

    def _tok_int(s: str) -> int:
        return int(s.strip(), 0)

    def _tok_float(s: str) -> float:
        return float(s.strip())

    try:
        temp1 = _tok_int(tokens[0])
        dc09 = _tok_float(tokens[1])
        dc10 = _tok_float(tokens[2])
        dc18 = _tok_float(tokens[3])
        clockflag = _tok_int(tokens[4])
        clock_drift = _tok_float(tokens[5])
        reserved1 = _tok_float(tokens[6])
        hw_flag = _tok_int(tokens[7]) & 0xFF
        pll_lock = _tok_int(tokens[8]) & 0xFFFF
        reserved3 = _tok_int(tokens[9]) if len(tokens) > 9 else 0
        reserved4 = _tok_int(tokens[10]) if len(tokens) > 10 else 0
    except (ValueError, IndexError):
        return None

    hw_flag_bits = {
        "oscillator_type": (hw_flag >> 0) & 1,
        "vcxo_tcxo": (hw_flag >> 1) & 1,
        "osc_freq": (hw_flag >> 2) & 1,
        "osc_crystal_support": (hw_flag >> 3) & 1,
        "check_status": (hw_flag >> 7) & 1,
    }
    return {
        "temp1": temp1,
        "temp1_celsius": temp1 / 1000.0,
        "dc09": dc09,
        "dc10": dc10,
        "dc18": dc18,
        "clockflag": clockflag,
        "clockflag_valid": clockflag == 1,
        "clock_drift": clock_drift,
        "hw_flag": hw_flag,
        "hw_flag_hex": f"0x{hw_flag:02X}",
        "hw_flag_bits": hw_flag_bits,
        "pll_lock": pll_lock,
        "pll_lock_hex": f"0x{pll_lock:04X}",
        "reserved": {
            "reserved1": reserved1,
            "reserved2": None,
            "reserved3": reserved3,
            "reserved4": reserved4,
        },
    }


def _check_hwstatus_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        search_start = max(0, len(data) - 10000)
        for i in range(search_start, len(data) - 24):
            if data[i : i + 3] != UNICORE_SYNC:
                continue
            header = parse_unicore_header(data[i : i + 24])
            if not header or header.message_id != HWSTATUS_MESSAGE_ID:
                continue
            trip = _hwstatus_try_unpack_frame(data, i, header.message_length)
            if trip is not None:
                return True
            cands = _hwstatus_binary_total_len_candidates(header.message_length)
            if cands:
                mx = max(cands)
                if len(data) < i + mx:
                    return False
        return len(data) > 1000
    try:
        text = data.decode("ascii", errors="ignore")
        m = _HWSTATUS_ASCII_COMPLETE_RE.search(text)
        if m and ";" in m.group(0):
            return True
    except Exception:
        pass
    return len(data) > 500


_AGRIC_ASCII_COMPLETE_RE = re.compile(
    r"#AGRIC[AB].*?\*[0-9a-fA-F]{8}",
    re.DOTALL | re.IGNORECASE,
)

_POSTYPE_NAMES: Dict[int, str] = {
    0: "NONE",
    1: "FIXED",
    2: "FLOAT",
    3: "SBAS",
    4: "DGPS",
    5: "SINGLE",
    6: "PSEUDO",
    8: "SIMULATION",
    10: "PPP_CONVERGING",
    11: "PPP",
    12: "INS",
    13: "INS_PSRSP",
    14: "INS_PSRDIFF",
    15: "INS_RTKFLOAT",
    16: "INS_RTKFIXED",
}

_HEADING_STATUS_NAMES: Dict[int, str] = {
    0: "INVALID",
    1: "VALID",
    2: "HEADING",
    3: "ALIGNING",
    4: "HIGH_VARIANCE",
    5: "SOLUTION_POOR",
}


def _postype_text(code: int) -> str:
    return _POSTYPE_NAMES.get(int(code) & 0xFF, f"UNKNOWN({code})")


def _heading_status_text(code: int) -> str:
    return _HEADING_STATUS_NAMES.get(int(code) & 0xFF, f"UNKNOWN({code})")


def _unpack_agric_body(body: bytes) -> Optional[Dict[str, Any]]:
    """Разбор тела AGRIC (байты H..) по §7.3.21; при len(body)>=232 последние 4 байта — CRC поля сообщения."""
    if len(body) < _AGRIC_BODY_PARSE_MIN:
        return None
    o = 0
    try:
        gnss = body[o : o + 4].decode("ascii", errors="ignore").strip("\x00 ")
        o += 4
        length = body[o]
        o += 1
        year, month, day, hour, minute, second = struct.unpack("<6B", body[o : o + 6])
        o += 6
        postype = body[o]
        o += 1
        heading_status = body[o]
        o += 1
        num_gps, num_bds, num_glo = struct.unpack("<3B", body[o : o + 3])
        o += 3
        baseline_n, baseline_e, baseline_u = struct.unpack("<3f", body[o : o + 12])
        o += 12
        baseline_n_std, baseline_e_std, baseline_u_std = struct.unpack("<3f", body[o : o + 12])
        o += 12
        heading, pitch, roll = struct.unpack("<3f", body[o : o + 12])
        o += 12
        speed = struct.unpack("<f", body[o : o + 4])[0]
        o += 4
        vel_n, vel_e, vel_u = struct.unpack("<3f", body[o : o + 12])
        o += 12
        vel_n_std, vel_e_std, vel_u_std = struct.unpack("<3f", body[o : o + 12])
        o += 12
        lat, lon, hgt = struct.unpack("<3d", body[o : o + 24])
        o += 24
        ecef_x, ecef_y, ecef_z = struct.unpack("<3d", body[o : o + 24])
        o += 24
        lat_std, lon_std, hgt_std = struct.unpack("<3f", body[o : o + 12])
        o += 12
        ecef_x_std, ecef_y_std, ecef_z_std = struct.unpack("<3f", body[o : o + 12])
        o += 12
        base_lat, base_lon, base_alt = struct.unpack("<3d", body[o : o + 24])
        o += 24
        sec_lat, sec_lon, sec_alt = struct.unpack("<3d", body[o : o + 24])
        o += 24
        gps_week_second = struct.unpack("<i", body[o : o + 4])[0]
        o += 4
        diffage = struct.unpack("<f", body[o : o + 4])[0]
        o += 4
        speed_heading = struct.unpack("<f", body[o : o + 4])[0]
        o += 4
        undulation = struct.unpack("<f", body[o : o + 4])[0]
        o += 4
        reserved_f1, reserved_f2 = struct.unpack("<2f", body[o : o + 8])
        o += 8
        num_gal, speed_type, res_b1, res_b2 = struct.unpack("<4B", body[o : o + 4])
        o += 4
        inner_crc = None
        if len(body) >= 232:
            inner_crc = struct.unpack("<I", body[228:232])[0]
    except (struct.error, IndexError):
        return None

    out: Dict[str, Any] = {
        "gnss": gnss,
        "length": int(length),
        "datetime": {
            "year": int(year),
            "month": int(month),
            "day": int(day),
            "hour": int(hour),
            "minute": int(minute),
            "second": int(second),
        },
        "postype": int(postype),
        "postype_text": _postype_text(postype),
        "heading_status": int(heading_status),
        "heading_status_text": _heading_status_text(heading_status),
        "satellites": {
            "gps": int(num_gps),
            "bds": int(num_bds),
            "glo": int(num_glo),
            "gal": int(num_gal),
        },
        "baseline": {
            "n": float(baseline_n),
            "e": float(baseline_e),
            "u": float(baseline_u),
            "north": float(baseline_n),
            "east": float(baseline_e),
            "up": float(baseline_u),
            "n_std": float(baseline_n_std),
            "e_std": float(baseline_e_std),
            "u_std": float(baseline_u_std),
        },
        "attitude": {"heading": float(heading), "pitch": float(pitch), "roll": float(roll)},
        "heading": {"degree": float(heading)},
        "velocity": {
            "speed": float(speed),
            "n": float(vel_n),
            "e": float(vel_e),
            "u": float(vel_u),
            "north": float(vel_n),
            "east": float(vel_e),
            "up": float(vel_u),
            "n_std": float(vel_n_std),
            "e_std": float(vel_e_std),
            "u_std": float(vel_u_std),
        },
        "position": {
            "lat": float(lat),
            "lon": float(lon),
            "hgt": float(hgt),
            "lat_std": float(lat_std),
            "lon_std": float(lon_std),
            "hgt_std": float(hgt_std),
        },
        "ecef": {
            "x": float(ecef_x),
            "y": float(ecef_y),
            "z": float(ecef_z),
            "x_std": float(ecef_x_std),
            "y_std": float(ecef_y_std),
            "z_std": float(ecef_z_std),
        },
        "base_position": {"lat": float(base_lat), "lon": float(base_lon), "alt": float(base_alt)},
        "secondary_position": {"lat": float(sec_lat), "lon": float(sec_lon), "alt": float(sec_alt)},
        "gps_week_second": int(gps_week_second),
        "diffage": float(diffage),
        "speed_heading": float(speed_heading),
        "undulation": float(undulation),
        "reserved_floats": (float(reserved_f1), float(reserved_f2)),
        "speed_type": int(speed_type),
        "reserved_tail": (int(res_b1), int(res_b2)),
    }
    if inner_crc is not None:
        out["payload_crc"] = f"0x{inner_crc:08X}"
    # Совместимость с GUI/старыми тестами
    out["rover_position"] = {
        "lat": float(lat),
        "lon": float(lon),
        "hgt": float(hgt),
    }
    return out


def _parse_agric_tokens(tokens: List[str]) -> Optional[Dict[str, Any]]:
    """Те же поля, что в бинарном теле: список ASCII-токенов после «;» (полный или короткий хвост как в примере §7.3.21)."""
    i = 0
    n = len(tokens)

    def pop_f() -> float:
        nonlocal i
        v = float(tokens[i])
        i += 1
        return v

    def pop_i() -> int:
        nonlocal i
        v = int(float(tokens[i]))
        i += 1
        return v

    if n < 52:
        return None
    try:
        gnss = tokens[i]
        i += 1
        length = pop_i()
        year, month, day, hour, minute, second = pop_i(), pop_i(), pop_i(), pop_i(), pop_i(), pop_i()
        postype = pop_i()
        heading_status = pop_i()
        num_gps, num_bds, num_glo = pop_i(), pop_i(), pop_i()
        baseline_n, baseline_e, baseline_u = pop_f(), pop_f(), pop_f()
        baseline_n_std, baseline_e_std, baseline_u_std = pop_f(), pop_f(), pop_f()
        # В ASCII-примере §7.3.21 после Baseline_UStd идут два дополнительных 0.0000 (не отражены в бинарной таблице).
        if i + 3 <= n:
            try:
                z0, z1, z2 = float(tokens[i]), float(tokens[i + 1]), float(tokens[i + 2])
            except (ValueError, IndexError):
                z0 = z1 = z2 = 0.0
            if abs(z0) < 1e-6 and abs(z1) < 1e-6 and abs(z2) > 1e-6:
                i += 2
        heading, pitch, roll = pop_f(), pop_f(), pop_f()
        speed = pop_f()
        vel_n, vel_e, vel_u = pop_f(), pop_f(), pop_f()
        # ASCII: блок VelStd N/E/U может отсутствовать (пример §7.3.21 — сразу после Vel_U идёт lat).
        try:
            nxt = float(tokens[i])
        except (ValueError, IndexError):
            nxt = 0.0
        if 15.0 <= abs(nxt) <= 90.0:
            vel_n_std = vel_e_std = vel_u_std = 0.0
        else:
            vel_n_std = pop_f()
            vel_e_std = pop_f()
            try:
                nxt2 = float(tokens[i])
            except (ValueError, IndexError):
                nxt2 = 0.0
            if 15.0 <= abs(nxt2) <= 90.0:
                vel_u_std = 0.0
            elif abs(nxt2) < 5.0:
                vel_u_std = pop_f()
            else:
                vel_u_std = 0.0
        lat, lon, hgt = pop_f(), pop_f(), pop_f()
        ecef_x, ecef_y, ecef_z = pop_f(), pop_f(), pop_f()
        lat_std, lon_std, hgt_std = pop_f(), pop_f(), pop_f()
        ecef_x_std, ecef_y_std, ecef_z_std = pop_f(), pop_f(), pop_f()

        base_lat = base_lon = base_alt = 0.0
        sec_lat = sec_lon = sec_alt = 0.0
        rem = n - i
        if rem >= 16:
            base_lat, base_lon, base_alt = pop_f(), pop_f(), pop_f()
            sec_lat, sec_lon, sec_alt = pop_f(), pop_f(), pop_f()
            gps_week_second = pop_i()
        else:
            # Короткий ASCII (пример §7.3.21): 12 токенов — 3 float, week, 5 float (diffage…2×reserved), 3 uchar
            reserved_pre = pop_f(), pop_f(), pop_f()
            gps_week_second = pop_i()
            diffage = pop_f()
            speed_heading = pop_f()
            undulation = pop_f()
            reserved_f1, reserved_f2 = pop_f(), pop_f()
            # В примере §7.3.21 после 5 float остаётся 2 uchar (num GAL, speed type); два reserved — 0
            if n - i >= 4:
                num_gal, speed_type, res_b1, res_b2 = pop_i(), pop_i(), pop_i(), pop_i()
            else:
                num_gal = pop_i()
                speed_type = pop_i()
                res_b1, res_b2 = 0, 0
            out_short: Dict[str, Any] = {
                "gnss": gnss,
                "length": length,
                "datetime": {
                    "year": year,
                    "month": month,
                    "day": day,
                    "hour": hour,
                    "minute": minute,
                    "second": second,
                },
                "postype": postype,
                "postype_text": _postype_text(postype),
                "heading_status": heading_status,
                "heading_status_text": _heading_status_text(heading_status),
                "satellites": {"gps": num_gps, "bds": num_bds, "glo": num_glo, "gal": num_gal},
                "baseline": {
                    "n": baseline_n,
                    "e": baseline_e,
                    "u": baseline_u,
                    "north": baseline_n,
                    "east": baseline_e,
                    "up": baseline_u,
                    "n_std": baseline_n_std,
                    "e_std": baseline_e_std,
                    "u_std": baseline_u_std,
                },
                "attitude": {"heading": heading, "pitch": pitch, "roll": roll},
                "heading": {"degree": heading},
                "velocity": {
                    "speed": speed,
                    "n": vel_n,
                    "e": vel_e,
                    "u": vel_u,
                    "north": vel_n,
                    "east": vel_e,
                    "up": vel_u,
                    "n_std": vel_n_std,
                    "e_std": vel_e_std,
                    "u_std": vel_u_std,
                },
                "position": {
                    "lat": lat,
                    "lon": lon,
                    "hgt": hgt,
                    "lat_std": lat_std,
                    "lon_std": lon_std,
                    "hgt_std": hgt_std,
                },
                "ecef": {
                    "x": ecef_x,
                    "y": ecef_y,
                    "z": ecef_z,
                    "x_std": ecef_x_std,
                    "y_std": ecef_y_std,
                    "z_std": ecef_z_std,
                },
                "base_position": {"lat": base_lat, "lon": base_lon, "alt": base_alt},
                "secondary_position": {"lat": sec_lat, "lon": sec_lon, "alt": sec_alt},
                "gps_week_second": gps_week_second,
                "diffage": diffage,
                "speed_heading": speed_heading,
                "undulation": undulation,
                "reserved_floats": (reserved_pre[0], reserved_pre[1], reserved_pre[2], reserved_f1, reserved_f2),
                "speed_type": speed_type,
                "reserved_tail": (res_b1, res_b2),
            }
            out_short["rover_position"] = {"lat": lat, "lon": lon, "hgt": hgt}
            return out_short

        diffage = pop_f()
        speed_heading = pop_f()
        undulation = pop_f()
        reserved_f1, reserved_f2 = pop_f(), pop_f()
        num_gal = pop_i()
        speed_type = pop_i()
        res_b1, res_b2 = pop_i(), pop_i()
    except (ValueError, IndexError):
        return None

    out = {
                    "gnss": gnss,
                    "length": length,
                    "datetime": {
                        "year": year,
                        "month": month,
                        "day": day,
                        "hour": hour,
                        "minute": minute,
                        "second": second,
                    },
                    "postype": postype,
        "postype_text": _postype_text(postype),
                    "heading_status": heading_status,
        "heading_status_text": _heading_status_text(heading_status),
        "satellites": {"gps": num_gps, "bds": num_bds, "glo": num_glo, "gal": num_gal},
                    "baseline": {
                        "n": baseline_n,
                        "e": baseline_e,
                        "u": baseline_u,
            "north": baseline_n,
            "east": baseline_e,
            "up": baseline_u,
                        "n_std": baseline_n_std,
                        "e_std": baseline_e_std,
                        "u_std": baseline_u_std,
                    },
                    "attitude": {"heading": heading, "pitch": pitch, "roll": roll},
        "heading": {"degree": heading},
                    "velocity": {
                        "speed": speed,
                        "n": vel_n,
                        "e": vel_e,
                        "u": vel_u,
            "north": vel_n,
            "east": vel_e,
            "up": vel_u,
                        "n_std": vel_n_std,
                        "e_std": vel_e_std,
                        "u_std": vel_u_std,
                    },
                    "position": {
                        "lat": lat,
                        "lon": lon,
                        "hgt": hgt,
                        "lat_std": lat_std,
                        "lon_std": lon_std,
                        "hgt_std": hgt_std,
                    },
        "ecef": {
            "x": ecef_x,
            "y": ecef_y,
            "z": ecef_z,
            "x_std": ecef_x_std,
            "y_std": ecef_y_std,
            "z_std": ecef_z_std,
        },
        "base_position": {"lat": base_lat, "lon": base_lon, "alt": base_alt},
        "secondary_position": {"lat": sec_lat, "lon": sec_lon, "alt": sec_alt},
        "gps_week_second": gps_week_second,
        "diffage": diffage,
        "speed_heading": speed_heading,
        "undulation": undulation,
        "reserved_floats": (reserved_f1, reserved_f2),
        "speed_type": speed_type,
        "reserved_tail": (res_b1, res_b2),
    }
    out["rover_position"] = {"lat": lat, "lon": lon, "hgt": hgt}
    return out


def _check_agric_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for j in range(max(0, len(data) - 12000), len(data) - 24):
            if data[j : j + 3] != UNICORE_SYNC:
                continue
            hdr = parse_unicore_header(data[j : j + 24])
            if hdr and hdr.message_id == AGRIC_MESSAGE_ID and hdr.message_length > 0:
                if _agric_try_unpack_frame(data, j, hdr.message_length) is not None:
                    return True
        return len(data) > 8000
    try:
        text = data.decode("ascii", errors="ignore")
        return _AGRIC_ASCII_COMPLETE_RE.search(text) is not None
    except Exception:
        pass
    return len(data) > 8000


def _parse_agric_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 28:
            return None
        for i in range(len(data) - 24):
            if data[i : i + 3] != UNICORE_SYNC:
                continue
            header = parse_unicore_header(data[i : i + 24])
            if not header or header.message_id != AGRIC_MESSAGE_ID:
                continue
            trip = _agric_try_unpack_frame(data, i, header.message_length)
            if trip is None:
                continue
            unpacked, used_total, crc_value = trip
            out = {
                "format": "binary",
                "header": {
                    "message_id": header.message_id,
                    "message_length": header.message_length,
                    "frame_bytes": used_total,
                },
                **unpacked,
                "crc": f"0x{crc_value:08X}",
                    "message_offset": i,
                }
            return out
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        m = _AGRIC_ASCII_COMPLETE_RE.search(text)
        if not m:
            return None
        chunk_full = m.group(0)
        star = chunk_full.rfind("*")
        if star < 0:
            return None
        chunk = chunk_full[:star]
        crc_hex = chunk_full[star + 1 : star + 9]
        if not re.fullmatch(r"[0-9a-fA-F]{8}", crc_hex, flags=re.I):
            return None
        if ";" not in chunk:
            return None
        _, body_part = chunk.split(";", 1)
        body_tokens = [t.strip() for t in re.split(r"[\r\n,]+", body_part) if t.strip() != ""]
        fields = _parse_agric_tokens(body_tokens)
        if not fields:
            return None
        return {
            "format": "ascii",
            **fields,
            "ascii_crc_hex": crc_hex,
            "raw": chunk,
        }
    except Exception:
        return None


def _parse_hwstatus_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i : i + 3] != UNICORE_SYNC:
                continue
            header = parse_unicore_header(data[i : i + 24])
            if not header or header.message_id != HWSTATUS_MESSAGE_ID:
                continue
            trip = _hwstatus_try_unpack_frame(data, i, header.message_length)
            if trip is None:
                continue
            unpacked, used_total, crc_value = trip
            return {
                "format": "binary",
                "header": {
                    "message_id": header.message_id,
                    "message_length": header.message_length,
                    "frame_bytes": used_total,
                },
                **unpacked,
                "crc": f"0x{crc_value:08X}",
                "message_offset": i,
            }
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        m = _HWSTATUS_ASCII_COMPLETE_RE.search(text)
        if not m:
            return None
        chunk_full = m.group(0)
        star = chunk_full.rfind("*")
        if star < 0:
            return None
        chunk = chunk_full[:star]
        crc_hex = chunk_full[star + 1 : star + 9]
        if not re.fullmatch(r"[0-9a-fA-F]{8}", crc_hex, flags=re.I):
            return None
        if ";" not in chunk:
            return None
        _, body_part = chunk.split(";", 1)
        body_tokens = [t.strip() for t in re.split(r"[\r\n,]+", body_part) if t.strip() != ""]
        fields = _parse_hwstatus_ascii_tokens(body_tokens)
        if not fields:
            return None
        return {
            "format": "ascii",
            **fields,
            "ascii_crc_hex": crc_hex,
            "raw": chunk,
        }
    except Exception:
        return None


def _parse_agc_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 220:
                    continue
                offset = i + 24
                # §7.3.55: 20 bytes полей + 4 bytes CRC после заголовка.
                if len(data) < offset + 24:
                    continue
                try:
                    antl1 = struct.unpack("<h", data[offset : offset + 2])[0]
                    p = offset + 2
                    antl2 = struct.unpack("<h", data[p : p + 2])[0]
                    p += 2
                    antl5 = struct.unpack("<h", data[p : p + 2])[0]
                    p += 2
                    reserved1 = struct.unpack("<h", data[p : p + 2])[0]
                    p += 2
                    reserved2 = struct.unpack("<h", data[p : p + 2])[0]
                    p += 2
                    antl2l1 = struct.unpack("<h", data[p : p + 2])[0]
                    p += 2
                    antl2l2 = struct.unpack("<h", data[p : p + 2])[0]
                    p += 2
                    antl2l5 = struct.unpack("<h", data[p : p + 2])[0]
                    p += 2
                    reserved3 = struct.unpack("<h", data[p : p + 2])[0]
                    p += 2
                    reserved4 = struct.unpack("<h", data[p : p + 2])[0]
                    p += 2
                    crc_wire = struct.unpack("<I", data[p : p + 4])[0]

                    crc_value = crc_wire

                    return {
                        "format": "binary",
                        "header": {
                            "message_id": header.message_id,
                            "message_length": header.message_length,
                        },
                        "master_antenna": {
                            "l1": antl1 if antl1 >= 0 else None,
                            "l2": antl2 if antl2 >= 0 else None,
                            "l5": antl5 if antl5 >= 0 else None,
                        },
                        "slave_antenna": {
                            "l1": antl2l1 if antl2l1 >= 0 else None,
                            "l2": antl2l2 if antl2l2 >= 0 else None,
                            "l5": antl2l5 if antl2l5 >= 0 else None,
                        },
                        "reserved": {
                            "reserved1": reserved1,
                            "reserved2": reserved2,
                            "reserved3": reserved3,
                            "reserved4": reserved4,
                        },
                        "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                        "message_offset": i,
                    }
                except (struct.error, IndexError):
                    continue
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        m = re.search(r"#AGCA[\s\S]*?\*[0-9a-fA-F]{7,8}", text, flags=re.I)
        if not m:
            return None
        chunk_full = m.group(0)
        star = chunk_full.rfind("*")
        if star < 0:
            return None
        line = chunk_full[:star]
        crc_hex = chunk_full[star + 1 : star + 9]
        if ";" not in line:
            return None
        _hdr, body = line.split(";", 1)
        toks = [t.strip() for t in re.split(r"[\r\n,]+", body) if t.strip() != ""]
        if len(toks) < 10:
            return None

        vals: List[int] = []
        for t in toks[:10]:
            try:
                vals.append(int(t, 10))
            except ValueError:
                vals.append(-1)
        antl1, antl2, antl5, reserved1, reserved2, ant2l1, ant2l2, ant2l5, reserved3, reserved4 = vals
        return {
            "format": "ascii",
            "master_antenna": {
                "l1": antl1 if antl1 >= 0 else None,
                "l2": antl2 if antl2 >= 0 else None,
                "l5": antl5 if antl5 >= 0 else None,
            },
            "slave_antenna": {
                "l1": ant2l1 if ant2l1 >= 0 else None,
                "l2": ant2l2 if ant2l2 >= 0 else None,
                "l5": ant2l5 if ant2l5 >= 0 else None,
            },
            "reserved": {
                "reserved1": reserved1,
                "reserved2": reserved2,
                "reserved3": reserved3,
                "reserved4": reserved4,
            },
            "ascii_crc_hex": crc_hex,
            "raw": line,
        }
    except Exception:
        return None


def _parse_mode_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        mode_pos = text.find("#MODE,")
        if mode_pos < 0:
            return None
        semicolon_pos = text.find(";", mode_pos + 6)
        if semicolon_pos <= mode_pos:
            return None
        crc_pos = text.find("*", semicolon_pos)
        if crc_pos < 0:
            crc_pos = text.find("\r", semicolon_pos)
        if crc_pos < 0:
            crc_pos = text.find("\n", semicolon_pos)
        if crc_pos < 0:
            crc_pos = min(len(text), semicolon_pos + 500)

        line = text[mode_pos:crc_pos]
        if not line.startswith("#MODE,"):
            return None
        parts = line.split(";", 1)
        if len(parts) < 2:
            return None
        data_part = parts[1]
        data_part_clean = data_part.split("*")[0].strip()
        mode_str = data_part_clean
        mode_parts = mode_str.split()

        mode_type = None
        mode_subtype = None
        heading_mode = None

        if len(mode_parts) >= 2 and mode_parts[0].upper() == "MODE":
            mode_type = mode_parts[1].upper()
            if mode_type == "ROVER" and len(mode_parts) >= 3:
                mode_subtype = mode_parts[2].upper()
            if mode_type == "HEADING2":
                for idx, part in enumerate(mode_parts):
                    if part.upper() == "HEADINGMODE" and idx + 1 < len(mode_parts):
                        heading_mode = mode_parts[idx + 1].upper()
                        break

        return {
            "format": "ascii",
            "mode": mode_type,
            "mode_subtype": mode_subtype,
            "heading_mode": heading_mode,
            "raw": line,
            "mode_string": mode_str,
        }
    except Exception:
        return None


def query_agric(
    core: Um982Core,
    port: Optional[str] = None,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    if port:
        command = f"AGRICB {port} {rate}" if binary else f"AGRICA {port} {rate}"
    else:
        command = f"AGRICB {rate}" if binary else f"AGRICA {rate}"

    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_agric_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=28 if binary else 24,
        check_complete=_check_agric_complete,
        result_key="agric",
    )


def query_hwstatus(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"HWSTATUSB {rate}" if binary else f"HWSTATUSA {rate}"

    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_hwstatus_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=_check_hwstatus_complete,
        result_key="hwstatus",
    )


def query_agc(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"AGCB {rate}" if binary else f"AGCA {rate}"

    # 220 – AGC
    check_complete = _make_unicore_header_checker(
        220,
        min_length=0,
        ascii_tag=b"#AGCA,",
        ascii_window=200,
        binary_min_total=80,
        ascii_min_total=120,
    )

    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_agc_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=check_complete,
        result_key="agc",
    )


def query_mode(
    core: Um982Core,
    mode_arg: str,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"MODE {mode_arg}"

    def check_mode_complete(data: bytes, is_binary: bool) -> bool:
        # MODE команда поддерживает только ASCII формат
        try:
            text = data.decode("ascii", errors="ignore")
            if "#MODE," in text:
                mode_pos = text.find("#MODE,")
                if mode_pos >= 0:
                    end_pos = min(len(text), mode_pos + 500)
                    if "*" in text[mode_pos:end_pos]:
                        return True
        except Exception:
            pass
        return len(data) > 500

    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_mode_message,
        binary=False,
        add_crlf=add_crlf,
        read_attempts=20,
        check_complete=check_mode_complete,
        result_key="mode",
    )


# --- UNILOGLIST, log, unlog вынесены в .logging ---
# --- UTC (GPSUTC, BD3UTC) вынесен в .time_utc ---

