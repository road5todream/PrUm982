import struct
from typing import Any, Dict, Optional

from um982.core import Um982Core
from um982.utils import parse_unicore_header

from .base import _run_data_query

BASEINFO_BINARY_PAYLOAD_LEN = 44

STATUS_VALID = 0
STATUS_INVALID = 1
STATUS_TEXT = {STATUS_VALID: "valid", STATUS_INVALID: "invalid"}


def _check_baseinfo_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 176:
                    msg_length = header.message_length
                    if msg_length > 0:
                        if len(data) >= i + msg_length:
                            return True
                    else:
                        return True
    else:
        try:
            if b"#BASEINFOA" in data:
                return True
            text = data.decode("ascii", errors="ignore")
            if "#BASEINFOA" in text:
                return True
        except Exception:
            pass
    return len(data) > 100


def _parse_baseinfo_message(data: bytes, binary: bool = False) -> Optional[dict]:
    """
    Парсер BASEINFO (Message ID 176).

    ASCII: #BASEINFOA,...;Status,X,Y,Z,StationID,Reserved*CRC
    Binary: 24-byte header + Status(4) + X,Y,Z(8*3) + StationID(8) + Reserved(4) + CRC(4).
    """
    if binary:
        return _parse_baseinfo_binary(data)
    return _parse_baseinfo_ascii(data)


def _parse_baseinfo_ascii(data: bytes) -> Optional[dict]:
    """Парсинг ASCII BASEINFOA: после ';' идут Status,X,Y,Z,StationID,Reserved до '*'."""
    try:
        text = data.decode("ascii", errors="ignore")
        idx = text.find("#BASEINFOA")
        if idx < 0:
            return None
        semi = text.find(";", idx)
        if semi < 0:
            return None
        star = text.find("*", semi)
        if star < 0:
            return None
        body = text[semi + 1 : star].strip()
        parts = [p.strip() for p in body.split(",")]
        if len(parts) < 6:
            return None
        # Status: 8 hex digits (e.g. 00000000) -> int
        try:
            status = int(parts[0], 16) if len(parts[0]) <= 8 else int(parts[0], 10)
        except ValueError:
            status = 0
        try:
            x = float(parts[1])
            y = float(parts[2])
            z = float(parts[3])
        except (ValueError, IndexError):
            return None
        station_id = parts[4].strip('"')
        try:
            reserved = int(parts[5], 10) if parts[5] else 0
        except ValueError:
            reserved = 0
        return {
            "format": "ascii",
            "status": status,
            "status_text": STATUS_TEXT.get(status, "unknown"),
            "x": x,
            "y": y,
            "z": z,
            "station_id": station_id,
            "reserved": reserved,
        }
    except Exception:
        return None


def _parse_baseinfo_binary(data: bytes) -> Optional[dict]:
    """Парсинг бинарного BASEINFOB: заголовок 24 байта, затем payload 44 байта."""
    min_len = 24 + BASEINFO_BINARY_PAYLOAD_LEN
    if len(data) < min_len:
        return None
    for i in range(len(data) - min_len + 1):
        if data[i] != 0xAA or data[i + 1] != 0x44 or data[i + 2] != 0xB5:
            continue
        header = parse_unicore_header(data[i : i + 24])
        if not header or header.message_id != 176:
            continue
        offset = i + 24
        if len(data) < offset + BASEINFO_BINARY_PAYLOAD_LEN:
            continue
        try:
            status = struct.unpack("<I", data[offset : offset + 4])[0]
            offset += 4
            x, y, z = struct.unpack("<ddd", data[offset : offset + 24])
            offset += 24
            station_id_bytes = data[offset : offset + 8]
            station_id = station_id_bytes.decode("ascii", errors="ignore").rstrip("\x00").strip()
            offset += 8
            reserved = struct.unpack("<I", data[offset : offset + 4])[0]
            offset += 4
            # CRC at offset (4 bytes) — не разбираем, только проверяем длину
            return {
                "format": "binary",
                "status": status,
                "status_text": STATUS_TEXT.get(status, "unknown"),
                "x": x,
                "y": y,
                "z": z,
                "station_id": station_id,
                "reserved": reserved,
                "header": {
                    "message_id": header.message_id,
                    "message_length": header.message_length,
                },
            }
        except Exception:
            continue
    return None


def query_baseinfo(
    core: Um982Core,
    rate: int = 1,
    trigger: Optional[str] = None,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    if trigger and trigger.upper() == "ONCHANGED":
        command = "BASEINFOB ONCHANGED" if binary else "BASEINFOA ONCHANGED"
    else:
        command = f"BASEINFOB {rate}" if binary else f"BASEINFOA {rate}"

    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_baseinfo_message,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.5,
        read_attempts=10,
        read_timeout=1.5,
        check_complete=_check_baseinfo_complete,
        result_key="baseinfo",
    )

