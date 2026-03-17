import struct
from typing import Optional, Dict, Any

from um982.core import Um982Core
from um982.utils import parse_unicore_header

from .base import _run_data_query, _make_unicore_header_checker


# --- AGRIC / HWSTATUS / AGC / MODE / ARDDOPH ---

def _parse_agric_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 11276:
                    continue
                offset = i + 24
                if len(data) < offset + 232:
                    continue

                gnss = data[offset : offset + 4].decode("ascii", errors="ignore").rstrip("\x00")
                offset += 4
                length = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1

                year = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                month = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                day = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                hour = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                minute = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                second = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1

                postype = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                heading_status = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1

                num_gps = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                num_bds = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                num_glo = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1

                baseline_n = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                baseline_e = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                baseline_u = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                baseline_n_std = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                baseline_e_std = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                baseline_u_std = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                heading = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                pitch = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                roll = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                speed = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                vel_n = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                vel_e = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                vel_u = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                vel_n_std = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                vel_e_std = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                vel_u_std = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                lat = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                lon = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                hgt = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8

                lat_std = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                lon_std = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                hgt_std = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                baseline_length = struct.unpack("<f", data[offset : offset + 4])[0]

                msg_length = header.message_length
                crc_value = None
                if msg_length > 0 and len(data) >= i + msg_length:
                    crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]

                return {
                    "format": "binary",
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
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
                    "heading_status": heading_status,
                    "satellites": {"gps": num_gps, "bds": num_bds, "glo": num_glo},
                    "baseline": {
                        "n": baseline_n,
                        "e": baseline_e,
                        "u": baseline_u,
                        "n_std": baseline_n_std,
                        "e_std": baseline_e_std,
                        "u_std": baseline_u_std,
                        "length": baseline_length,
                    },
                    "attitude": {"heading": heading, "pitch": pitch, "roll": roll},
                    "velocity": {
                        "speed": speed,
                        "n": vel_n,
                        "e": vel_e,
                        "u": vel_u,
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
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    return None


def _parse_hwstatus_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 218:
                    continue
                offset = i + 24
                if len(data) < offset + 44:
                    continue
                try:
                    temp1 = struct.unpack("<i", data[offset : offset + 4])[0]
                    offset += 4
                    dc09 = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    dc10 = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    dc18 = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    clockflag = struct.unpack("<I", data[offset : offset + 4])[0]
                    offset += 4
                    clock_drift = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    reserved1 = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    hw_flag = struct.unpack("<B", data[offset : offset + 1])[0]
                    offset += 1
                    reserved2 = struct.unpack("<B", data[offset : offset + 1])[0]
                    offset += 1
                    pll_lock = struct.unpack("<H", data[offset : offset + 2])[0]
                    offset += 2
                    reserved3 = struct.unpack("<I", data[offset : offset + 4])[0]
                    offset += 4
                    reserved4 = struct.unpack("<I", data[offset : offset + 4])[0]

                    hw_flag_bits = {
                        "oscillator_type": (hw_flag >> 0) & 1,
                        "vcxo_tcxo": (hw_flag >> 1) & 1,
                        "osc_freq": (hw_flag >> 2) & 1,
                        "osc_crystal_support": (hw_flag >> 3) & 1,
                        "check_status": (hw_flag >> 7) & 1,
                    }

                    msg_length = header.message_length
                    crc_value = None
                    if msg_length > 0 and len(data) >= i + msg_length:
                        crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]

                    return {
                        "format": "binary",
                        "header": {
                            "message_id": header.message_id,
                            "message_length": header.message_length,
                        },
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
        pos = text.find("#HWSTATUSA,")
        if pos < 0:
            return None
        end = text.find("*", pos)
        if end < 0:
            end = text.find("\r", pos)
            if end < 0:
                end = text.find("\n", pos)
            if end < 0:
                end = min(len(text), pos + 500)
        line = text[pos:end]
        return {"format": "ascii", "raw": line}
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
                if len(data) < offset + 20:
                    continue
                try:
                    antl1 = struct.unpack("<h", data[offset : offset + 2])[0]
                    offset += 2
                    antl2 = struct.unpack("<h", data[offset : offset + 2])[0]
                    offset += 2
                    antl5 = struct.unpack("<h", data[offset : offset + 2])[0]
                    offset += 2
                    reserved1 = struct.unpack("<h", data[offset : offset + 2])[0]
                    offset += 2
                    reserved2 = struct.unpack("<h", data[offset : offset + 2])[0]
                    offset += 2
                    antl2l1 = struct.unpack("<h", data[offset : offset + 2])[0]
                    offset += 2
                    antl2l2 = struct.unpack("<h", data[offset : offset + 2])[0]
                    offset += 2
                    antl2l5 = struct.unpack("<h", data[offset : offset + 2])[0]
                    offset += 2
                    reserved3 = struct.unpack("<h", data[offset : offset + 2])[0]
                    offset += 2
                    reserved4 = struct.unpack("<h", data[offset : offset + 2])[0]

                    msg_length = header.message_length
                    crc_value = None
                    if msg_length > 0 and len(data) >= i + msg_length:
                        crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]

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
        pos = text.find("#AGCA,")
        if pos < 0:
            return None
        end = text.find("*", pos)
        if end < 0:
            end = text.find("\r", pos)
            if end < 0:
                end = text.find("\n", pos)
            if end < 0:
                end = min(len(text), pos + 200)
        line = text[pos:end]
        return {"format": "ascii", "raw": line}
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

    # 11276 – message ID AGRIC; полезная нагрузка ~232 байта
    check_complete = _make_unicore_header_checker(
        11276,
        min_length=232,
        ascii_tag=b"#AGRICA,",
        ascii_window=2000,
        binary_min_total=5000,
        ascii_min_total=3000,
    )

    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_agric_message,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.5,
        read_attempts=5 if binary else 4,
        read_timeout=0.6 if binary else 0.5,
        check_complete=check_complete,
        result_key="agric",
    )


def query_hwstatus(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"HWSTATUSB {rate}" if binary else f"HWSTATUSA {rate}"

    # 218 – HWSTATUS
    check_complete = _make_unicore_header_checker(
        218,
        min_length=44,
        ascii_tag=b"#HWSTATUSA,",
        ascii_window=500,
        binary_min_total=1000,
        ascii_min_total=500,
    )

    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_hwstatus_message,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.5,
        read_attempts=10,
        read_timeout=1.0,
        check_complete=check_complete,
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
        min_length=20,
        ascii_tag=b"#AGCA,",
        ascii_window=200,
        binary_min_total=1000,
        ascii_min_total=500,
    )

    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_agc_message,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.5,
        read_attempts=10,
        read_timeout=1.0,
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
        wait_time=0.8,
        read_attempts=6,
        read_timeout=0.8,
        check_complete=check_mode_complete,
        result_key="mode",
    )



