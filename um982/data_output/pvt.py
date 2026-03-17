"""PVT-решения: query_pvtsln, _parse_pvtsln_message."""
import re
import struct
from typing import Any, Dict, Optional

from um982.core import Um982Core
from um982.utils import parse_unicore_header

from .common import _run_data_query


def _check_pvtsln_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        search_start = max(0, len(data) - 10000)
        for i in range(search_start, len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 1021:
                    msg_length = header.message_length
                    if len(data) >= i + msg_length:
                        return True
        return len(data) > 5000
    try:
        text = data.decode("ascii", errors="ignore")
        pvtslna_pos = text.find("#PVTSLNA,")
        if pvtslna_pos >= 0:
            end_pos = min(len(text), pvtslna_pos + 2000)
            if "*" in text[pvtslna_pos:end_pos]:
                semicolon_pos = text.find(";", pvtslna_pos)
                if pvtslna_pos < semicolon_pos < end_pos:
                    return True
    except Exception:
        pass
    return len(data) > 3000


def _parse_pvtsln_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 1021:
                    continue
                offset = i + 24
                if len(data) < offset + 224:
                    continue

                bestpos_type = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                bestpos_hgt = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                bestpos_lat = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                bestpos_lon = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8

                bestpos_hgtstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                bestpos_latstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                bestpos_lonstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                bestpos_diffage = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                psrpos_type = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                psrpos_hgt = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                psrpos_lat = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                psrpos_lon = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8

                psrpos_hgtstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                psrpos_latstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                psrpos_lonstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                psrpos_diffage = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                bestvel_type = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                bestvel_north = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                bestvel_east = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                bestvel_up = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                bestvel_northstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                bestvel_eaststd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                bestvel_upstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                psrvel_type = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                psrvel_north = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                psrvel_east = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                psrvel_up = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                psrvel_northstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                psrvel_eaststd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                psrvel_upstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                head_type = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                head_heading = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                head_pitch = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                head_roll = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                head_baselength = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                head_headingstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                head_pitchstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                head_rollstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                head_dualanttype = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                head_dualantheading = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                head_dualantheadingstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                head_dualantpitch = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                head_dualantpitchstd = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                msg_length = header.message_length
                crc_value = None
                if msg_length > 0 and len(data) >= i + msg_length:
                    crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]

                return {
                    "format": "binary",
                    "best_position": {
                        "type": bestpos_type,
                        "height": bestpos_hgt,
                        "lat": bestpos_lat,
                        "lon": bestpos_lon,
                        "hgt_std": bestpos_hgtstd,
                        "lat_std": bestpos_latstd,
                        "lon_std": bestpos_lonstd,
                        "diff_age": bestpos_diffage,
                    },
                    "psr_position": {
                        "type": psrpos_type,
                        "height": psrpos_hgt,
                        "lat": psrpos_lat,
                        "lon": psrpos_lon,
                        "hgt_std": psrpos_hgtstd,
                        "lat_std": psrpos_latstd,
                        "lon_std": psrpos_lonstd,
                        "diff_age": psrpos_diffage,
                    },
                    "best_velocity": {
                        "type": bestvel_type,
                        "north": bestvel_north,
                        "east": bestvel_east,
                        "up": bestvel_up,
                        "north_std": bestvel_northstd,
                        "east_std": bestvel_eaststd,
                        "up_std": bestvel_upstd,
                    },
                    "psr_velocity": {
                        "type": psrvel_type,
                        "north": psrvel_north,
                        "east": psrvel_east,
                        "up": psrvel_up,
                        "north_std": psrvel_northstd,
                        "east_std": psrvel_eaststd,
                        "up_std": psrvel_upstd,
                    },
                    "heading": {
                        "type": head_type,
                        "heading": head_heading,
                        "pitch": head_pitch,
                        "roll": head_roll,
                        "baselength": head_baselength,
                        "heading_std": head_headingstd,
                        "pitch_std": head_pitchstd,
                        "roll_std": head_rollstd,
                    },
                    "dual_antenna_heading": {
                        "type": head_dualanttype,
                        "heading": head_dualantheading,
                        "heading_std": head_dualantheadingstd,
                        "pitch": head_dualantpitch,
                        "pitch_std": head_dualantpitchstd,
                    },
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        start = text.find("#PVTSLNA")
        if start < 0:
            return None
        star = text.find("*", start)
        if star < 0:
            end = text.find("\r", start)
            if end < 0:
                end = text.find("\n", start)
            if end < 0:
                end = len(text)
        else:
            end = star
        chunk = text[start:end]
        parts = chunk.split(";", 1)
        if len(parts) < 2:
            return None
        data_part = parts[1]
        tokens = [f.strip() for f in re.split(r"[,\\r\\n]+", data_part) if f.strip() != ""]
        if len(tokens) < 40:
            return None

        def _f(idx: int, default: float = 0.0) -> float:
            if idx >= len(tokens):
                return default
            try:
                return float(tokens[idx])
            except ValueError:
                return default

        def _i(idx: int, default: int = 0) -> int:
            if idx >= len(tokens):
                return default
            try:
                return int(tokens[idx])
            except ValueError:
                return default

        bestpos_type = _i(0)
        bestpos_hgt = _f(1)
        bestpos_lat = _f(2)
        bestpos_lon = _f(3)
        bestpos_hgtstd = _f(4)
        bestpos_latstd = _f(5)
        bestpos_lonstd = _f(6)
        bestpos_diffage = _f(7)

        psrpos_type = _i(8)
        psrpos_hgt = _f(9)
        psrpos_lat = _f(10)
        psrpos_lon = _f(11)
        psrpos_hgtstd = _f(12)
        psrpos_latstd = _f(13)
        psrpos_lonstd = _f(14)
        psrpos_diffage = _f(15)

        bestvel_type = _i(16)
        bestvel_north = _f(17)
        bestvel_east = _f(18)
        bestvel_up = _f(19)
        bestvel_northstd = _f(20)
        bestvel_eaststd = _f(21)
        bestvel_upstd = _f(22)

        psrvel_type = _i(23)
        psrvel_north = _f(24)
        psrvel_east = _f(25)
        psrvel_up = _f(26)
        psrvel_northstd = _f(27)
        psrvel_eaststd = _f(28)
        psrvel_upstd = _f(29)

        head_type = _i(30)
        head_heading = _f(31)
        head_pitch = _f(32)
        head_roll = _f(33)
        head_baselength = _f(34)
        head_headingstd = _f(35)
        head_pitchstd = _f(36)
        head_rollstd = _f(37)

        head_dualanttype = _i(38)
        head_dualantheading = _f(39)
        head_dualantheadingstd = _f(40)
        head_dualantpitch = _f(41)
        head_dualantpitchstd = _f(42)

        return {
            "format": "ascii",
            "best_position": {
                "type": bestpos_type,
                "height": bestpos_hgt,
                "lat": bestpos_lat,
                "lon": bestpos_lon,
                "hgt_std": bestpos_hgtstd,
                "lat_std": bestpos_latstd,
                "lon_std": bestpos_lonstd,
                "diff_age": bestpos_diffage,
            },
            "psr_position": {
                "type": psrpos_type,
                "height": psrpos_hgt,
                "lat": psrpos_lat,
                "lon": psrpos_lon,
                "hgt_std": psrpos_hgtstd,
                "lat_std": psrpos_latstd,
                "lon_std": psrpos_lonstd,
                "diff_age": psrpos_diffage,
            },
            "best_velocity": {
                "type": bestvel_type,
                "north": bestvel_north,
                "east": bestvel_east,
                "up": bestvel_up,
                "north_std": bestvel_northstd,
                "east_std": bestvel_eaststd,
                "up_std": bestvel_upstd,
            },
            "psr_velocity": {
                "type": psrvel_type,
                "north": psrvel_north,
                "east": psrvel_east,
                "up": psrvel_up,
                "north_std": psrvel_northstd,
                "east_std": psrvel_eaststd,
                "up_std": psrvel_upstd,
            },
            "heading": {
                "type": head_type,
                "heading": head_heading,
                "pitch": head_pitch,
                "roll": head_roll,
                "baselength": head_baselength,
                "heading_std": head_headingstd,
                "pitch_std": head_pitchstd,
                "roll_std": head_rollstd,
            },
            "dual_antenna_heading": {
                "type": head_dualanttype,
                "heading": head_dualantheading,
                "heading_std": head_dualantheadingstd,
                "pitch": head_dualantpitch,
                "pitch_std": head_dualantpitchstd,
            },
            "raw": chunk,
        }
    except Exception:
        pass
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
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
        check_complete=_check_pvtsln_complete,
        result_key="pvtsln",
    )
