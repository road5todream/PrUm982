"""PVT-решения: PVTSLN (§7.3.22, message_id 1021) — query_pvtsln, _parse_pvtsln_message."""
from __future__ import annotations

import re
import struct
from typing import Any, Dict, List, Optional, Tuple

from um982.core import Um982Core
from um982.utils import parse_unicore_header

from .common import UNICORE_SYNC, _run_data_query
from .nav_enums import position_velocity_type_to_str

PVTSLN_MESSAGE_ID = 1021
# Полезная нагрузка после 24-байтного заголовка Unicore: 224 байта полей + 4 байта CRC сообщения (§7.3.22).
PVTSLN_PAYLOAD_BYTES = 228
PVTSLN_FIELD_BYTES = 224

_PVTSLN_STRUCT = struct.Struct("<ifddffffifddf4Bdddifff4B6fH41H")

_PVTSLN_ASCII_COMPLETE_RE = re.compile(
    r"#PVTSLN[AB].*?\*[0-9a-fA-F]{8}",
    re.DOTALL | re.IGNORECASE,
)


def _pvtsln_total_len_candidates(message_length: int) -> List[int]:
    """Варианты полной длины кадра (как у AGRIC): в заголовке иногда только длина payload (228)."""
    seen: set[int] = set()
    out: List[int] = []

    def add(t: int) -> None:
        if 52 <= t <= 8192 and t not in seen:
            seen.add(t)
            out.append(t)

    ml = int(message_length)
    if ml > 0:
        add(ml)
        add(ml + 4)
    add(24 + PVTSLN_PAYLOAD_BYTES + 4)
    return out


def _unpack_pvtsln_body(body: bytes) -> Optional[Dict[str, Any]]:
    """Разбор 224 байт полей PVTSLN (без 4-байтного CRC сообщения)."""
    if len(body) < PVTSLN_FIELD_BYTES:
        return None
    blob = body[:PVTSLN_FIELD_BYTES]
    try:
        u = _PVTSLN_STRUCT.unpack(blob)
    except struct.error:
        return None

    (
        bestpos_type,
        bestpos_hgt,
        bestpos_lat,
        bestpos_lon,
        bestpos_hgtstd,
        bestpos_latstd,
        bestpos_lonstd,
        bestpos_diffage,
        psrpos_type,
        psrpos_hgt,
        psrpos_lat,
        psrpos_lon,
        undulation,
        bestpos_svs,
        bestpos_solnsvs,
        psrpos_svs,
        psrpos_solnsvs,
        psrvel_north,
        psrvel_east,
        psrvel_ground,
        heading_type,
        heading_length,
        heading_degree,
        heading_pitch,
        heading_trackedsvs,
        heading_solnsvs,
        heading_l1svs,
        heading_l1l2svs,
        gdop,
        pdop,
        hdop,
        htdop,
        tdop,
        cutoff,
        prn_no,
    ) = u[:35]
    prn_list = list(u[35:])

    def _etype(code: int) -> str:
        try:
            return position_velocity_type_to_str(int(code))
        except (TypeError, ValueError):
            return str(code)

    return {
        "best_position": {
            "type": int(bestpos_type),
            "type_text": _etype(bestpos_type),
            "height": float(bestpos_hgt),
            "lat": float(bestpos_lat),
            "lon": float(bestpos_lon),
            "hgt_std": float(bestpos_hgtstd),
            "lat_std": float(bestpos_latstd),
            "lon_std": float(bestpos_lonstd),
            "diff_age": float(bestpos_diffage),
            "svs": int(bestpos_svs),
            "solnsvs": int(bestpos_solnsvs),
        },
        "psr_position": {
            "type": int(psrpos_type),
            "type_text": _etype(psrpos_type),
            "height": float(psrpos_hgt),
            "lat": float(psrpos_lat),
            "lon": float(psrpos_lon),
            "svs": int(psrpos_svs),
            "solnsvs": int(psrpos_solnsvs),
        },
        "undulation": float(undulation),
        "psr_velocity": {
            "north": float(psrvel_north),
            "east": float(psrvel_east),
            "ground": float(psrvel_ground),
        },
        "heading_block": {
            "type": int(heading_type),
            "type_text": _etype(heading_type),
            "length": float(heading_length),
            "degree": float(heading_degree),
            "pitch": float(heading_pitch),
            "trackedsvs": int(heading_trackedsvs),
            "solnsvs": int(heading_solnsvs),
            "l1svs": int(heading_l1svs),
            "l1l2svs": int(heading_l1l2svs),
        },
        "dop": {
            "gdop": float(gdop),
            "pdop": float(pdop),
            "hdop": float(hdop),
            "htdop": float(htdop),
            "tdop": float(tdop),
        },
        "cutoff": float(cutoff),
        "prn_no": int(prn_no),
        "prn_list": prn_list,
    }


def _pvtsln_try_unpack_frame(data: bytes, sync_off: int, header_message_length: int) -> Optional[Tuple[Dict[str, Any], int, int]]:
    for total in _pvtsln_total_len_candidates(header_message_length):
        if len(data) < sync_off + total:
            continue
        body = data[sync_off + 24 : sync_off + total - 4]
        if len(body) < PVTSLN_FIELD_BYTES:
            continue
        unpacked = _unpack_pvtsln_body(body)
        if not unpacked:
            continue
        inner_crc = None
        if len(body) >= PVTSLN_PAYLOAD_BYTES:
            inner_crc = struct.unpack("<I", body[PVTSLN_FIELD_BYTES:PVTSLN_PAYLOAD_BYTES])[0]
        crc_val = struct.unpack("<I", data[sync_off + total - 4 : sync_off + total])[0]
        out = {**unpacked, "payload_crc": f"0x{inner_crc:08X}"} if inner_crc is not None else unpacked
        return out, total, crc_val
    return None


def _check_pvtsln_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for j in range(max(0, len(data) - 12000), len(data) - 24):
            if data[j : j + 3] != UNICORE_SYNC:
                continue
            hdr = parse_unicore_header(data[j : j + 24])
            if hdr and hdr.message_id == PVTSLN_MESSAGE_ID and hdr.message_length > 0:
                if _pvtsln_try_unpack_frame(data, j, hdr.message_length) is not None:
                    return True
        return len(data) > 5000
    try:
        text = data.decode("ascii", errors="ignore")
        return _PVTSLN_ASCII_COMPLETE_RE.search(text) is not None
    except Exception:
        pass
    return len(data) > 3000


def _parse_pvtsln_ascii_tokens(tokens: List[str]) -> Optional[Dict[str, Any]]:
    """ASCII тело после «;» по примеру §7.3.22 (типы решения — строки вроде SINGLE / NONE)."""
    n = len(tokens)
    if n < 36:
        return None
    i = 0

    def _tok_str(idx: int) -> str:
        if idx >= n:
            return ""
        return tokens[idx].strip()

    def _f(idx: int) -> float:
        try:
            return float(tokens[idx])
        except (ValueError, IndexError):
            return 0.0

    def _i(idx: int) -> int:
        try:
            return int(float(tokens[idx]))
        except (ValueError, IndexError):
            return 0

    bestpos_type_s = _tok_str(i)
    i += 1
    bestpos_hgt = _f(i)
    i += 1
    bestpos_lat = _f(i)
    i += 1
    bestpos_lon = _f(i)
    i += 1
    bestpos_hgtstd = _f(i)
    i += 1
    bestpos_latstd = _f(i)
    i += 1
    bestpos_lonstd = _f(i)
    i += 1
    bestpos_diffage = _f(i)
    i += 1

    psrpos_type_s = _tok_str(i)
    i += 1
    psrpos_hgt = _f(i)
    i += 1
    psrpos_lat = _f(i)
    i += 1
    psrpos_lon = _f(i)
    i += 1
    undulation = _f(i)
    i += 1

    bestpos_svs = _i(i)
    i += 1
    bestpos_solnsvs = _i(i)
    i += 1
    psrpos_svs = _i(i)
    i += 1
    psrpos_solnsvs = _i(i)
    i += 1

    psrvel_north = _f(i)
    i += 1
    psrvel_east = _f(i)
    i += 1
    psrvel_ground = _f(i)
    i += 1

    heading_type_s = _tok_str(i)
    i += 1
    heading_length = _f(i)
    i += 1
    heading_degree = _f(i)
    i += 1
    heading_pitch = _f(i)
    i += 1

    # В §7.3.22 в бинарном виде 4×uchar; в ASCII-примере перед DOP иногда только три целых (0,0,0).
    hsv: List[int] = []
    while len(hsv) < 4 and i < n:
        t = tokens[i]
        if "." in t or not re.fullmatch(r"-?\d+", t):
            break
        hsv.append(int(t, 10))
        i += 1
    while len(hsv) < 4:
        hsv.append(0)
    heading_trackedsvs, heading_solnsvs, heading_l1svs, heading_l1l2svs = hsv[0], hsv[1], hsv[2], hsv[3]

    gdop = _f(i)
    i += 1
    pdop = _f(i)
    i += 1
    hdop = _f(i)
    i += 1
    htdop = _f(i)
    i += 1
    tdop = _f(i)
    i += 1

    cutoff = _f(i)
    i += 1

    prn_no = _i(i)
    i += 1
    if prn_no < 0 or prn_no > 41 or i + prn_no > n:
        return None
    prn_list = [_i(i + k) for k in range(prn_no)]
    i += prn_no

    core: Dict[str, Any] = {
        "best_position": {
            "type": bestpos_type_s,
            "height": bestpos_hgt,
            "lat": bestpos_lat,
            "lon": bestpos_lon,
            "hgt_std": bestpos_hgtstd,
            "lat_std": bestpos_latstd,
            "lon_std": bestpos_lonstd,
            "diff_age": bestpos_diffage,
            "svs": bestpos_svs,
            "solnsvs": bestpos_solnsvs,
        },
        "psr_position": {
            "type": psrpos_type_s,
            "height": psrpos_hgt,
            "lat": psrpos_lat,
            "lon": psrpos_lon,
            "svs": psrpos_svs,
            "solnsvs": psrpos_solnsvs,
        },
        "undulation": undulation,
        "psr_velocity": {
            "north": psrvel_north,
            "east": psrvel_east,
            "ground": psrvel_ground,
        },
        "heading_block": {
            "type": heading_type_s,
            "length": heading_length,
            "degree": heading_degree,
            "pitch": heading_pitch,
            "trackedsvs": heading_trackedsvs,
            "solnsvs": heading_solnsvs,
            "l1svs": heading_l1svs,
            "l1l2svs": heading_l1l2svs,
        },
        "dop": {
            "gdop": gdop,
            "pdop": pdop,
            "hdop": hdop,
            "htdop": htdop,
            "tdop": tdop,
        },
        "cutoff": cutoff,
        "prn_no": prn_no,
        "prn_list": prn_list,
    }
    return _pvtsln_add_gui_aliases(core)


def _pvtsln_add_gui_aliases(core: Dict[str, Any]) -> Dict[str, Any]:
    """Ключи bestpos / heading / velocity для um982_gui и старых вызовов."""
    bp = core["best_position"]
    core["bestpos"] = {
        "type": bp.get("type_text") or bp.get("type"),
        "lat": bp["lat"],
        "lon": bp["lon"],
        "hgt": bp["height"],
        "hgt_std": bp.get("hgt_std"),
        "lat_std": bp.get("lat_std"),
        "lon_std": bp.get("lon_std"),
        "diff_age": bp.get("diff_age"),
    }
    hb = core["heading_block"]
    core["heading"] = {
        "degree": hb["degree"],
        "pitch": hb.get("pitch"),
        "length": hb.get("length"),
        "type": hb.get("type_text") or hb.get("type"),
    }
    pv = core["psr_velocity"]
    core["velocity"] = {
        "north": pv["north"],
        "east": pv["east"],
        "ground": pv.get("ground"),
    }
    return core


def _parse_pvtsln_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 28:
            return None
        for i in range(len(data) - 24):
            if data[i : i + 3] != UNICORE_SYNC:
                continue
            header = parse_unicore_header(data[i : i + 24])
            if not header or header.message_id != PVTSLN_MESSAGE_ID:
                continue
            trip = _pvtsln_try_unpack_frame(data, i, header.message_length)
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
            return _pvtsln_add_gui_aliases(out)
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        m = _PVTSLN_ASCII_COMPLETE_RE.search(text)
        if not m:
            return None
        chunk_full = m.group(0)
        star = chunk_full.rfind("*")
        if star < 0:
            return None
        crc_hex = chunk_full[star + 1 : star + 9]
        if not re.fullmatch(r"[0-9a-fA-F]{8}", crc_hex, flags=re.I):
            return None
        chunk = chunk_full[:star]
        if ";" not in chunk:
            return None
        _, data_part = chunk.split(";", 1)
        tokens = [t.strip() for t in re.split(r"[\r\n,]+", data_part) if t.strip() != ""]
        fields = _parse_pvtsln_ascii_tokens(tokens)
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


def query_pvtsln(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"PVTSLNB {rate}" if binary else f"PVTSLNA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_pvtsln_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=28 if binary else 24,
        check_complete=_check_pvtsln_complete,
        result_key="pvtsln",
    )
