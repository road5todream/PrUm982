"""
Парсинг VERSION (ASCII #VERSIONA / бинарный id 37) для фонового приёма и общего использования.
"""
from __future__ import annotations

import re
import struct
from dataclasses import asdict
from typing import Dict, Optional

from um982.utils import parse_unicore_header

_PRODUCT_NAMES: Dict[int, str] = {
    0: "UNKNOWN",
    1: "UB4B0",
    2: "UM4B0",
    3: "UM480",
    4: "UM440",
    5: "UM482",
    6: "UM442",
    7: "UB482",
    8: "UT4B0",
    9: "UT900",
    10: "UB362L",
    11: "UB4B0M",
    12: "UB4B0J",
    13: "UM482L",
    14: "UM4B0L",
    15: "UT910",
    16: "CLAP-B",
    17: "UM982",
    18: "UM980",
    19: "UM960",
    20: "UM980i",
    21: "UM980A",
    22: "UM960A",
    23: "CLAP-C",
    24: "UM960L",
}


def _product_name(product_type: int) -> str:
    return _PRODUCT_NAMES.get(product_type, f"UNKNOWN({product_type})")


def parse_version_rx(data: bytes, binary: bool = False) -> Optional[dict]:
    """
    Разбор ответа VERSION (совместимо с UM982UART._parse_version_message).
    binary=True — ожидается бинарный кадр или поток с VERSIONB (message_id 37).
    """
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 37:
                    msg_length = header.message_length or 336
                    if len(data) < i + msg_length:
                        continue
                    offset = i + 24
                    if len(data) < offset + 4:
                        continue
                    product_type = struct.unpack("<I", data[offset : offset + 4])[0]
                    offset += 4
                    if len(data) < offset + 33:
                        continue
                    sw_version_build = (
                        data[offset : offset + 33].decode("ascii", errors="ignore").rstrip("\x00").rstrip()
                    )
                    offset += 33
                    if len(data) < offset + 129:
                        continue
                    psn = data[offset : offset + 129].decode("ascii", errors="ignore").rstrip("\x00").rstrip()
                    offset += 129
                    if len(data) < offset + 66:
                        continue
                    auth = data[offset : offset + 66].decode("ascii", errors="ignore").rstrip("\x00").rstrip()
                    offset += 66
                    if len(data) < offset + 33:
                        continue
                    efuse_id = data[offset : offset + 33].decode("ascii", errors="ignore").rstrip("\x00").rstrip()
                    offset += 33
                    if len(data) < offset + 43:
                        continue
                    comp_time = data[offset : offset + 43].decode("ascii", errors="ignore").rstrip("\x00").rstrip()
                    crc_offset = i + msg_length - 4
                    crc_value = None
                    if len(data) >= crc_offset + 4:
                        crc_value = struct.unpack("<I", data[crc_offset : crc_offset + 4])[0]
                    return {
                        "format": "binary",
                        "product_type": product_type,
                        "product_name": _product_name(product_type),
                        "sw_version": sw_version_build,
                        "auth": auth,
                        "psn": psn,
                        "efuse_id": efuse_id,
                        "comp_time": comp_time,
                        "header": asdict(header),
                        "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                        "message_offset": i,
                    }
        return None

    try:
        version_marker = b"#VERSIONA"
        version_pos = data.find(version_marker)
        if version_pos >= 0:
            text = data[version_pos:].decode("ascii", errors="ignore")
        else:
            text = data.decode("ascii", errors="ignore")
        for match in re.finditer(r"#VERSIONA[^\r\n]*", text):
            line = match.group(0).strip()
            if not line:
                continue
            parts = line.split(";")
            if len(parts) < 2:
                continue
            header_part = parts[0].replace("#VERSIONA,", "")
            data_part = parts[1]
            data_fields: list[str] = []
            current_field = ""
            in_quotes = False
            for char in data_part:
                if char == '"':
                    in_quotes = not in_quotes
                elif char == "," and not in_quotes:
                    if current_field:
                        data_fields.append(current_field)
                        current_field = ""
                elif char == "*" and not in_quotes:
                    if current_field:
                        data_fields.append(current_field)
                    break
                else:
                    current_field += char
            if len(data_fields) >= 6:
                return {
                    "format": "ascii",
                    "product_name": data_fields[0] if len(data_fields) > 0 else "",
                    "sw_version": data_fields[1] if len(data_fields) > 1 else "",
                    "psn": data_fields[2] if len(data_fields) > 2 else "",
                    "auth": data_fields[3] if len(data_fields) > 3 else "",
                    "efuse_id": data_fields[4] if len(data_fields) > 4 else "",
                    "comp_time": data_fields[5] if len(data_fields) > 5 else "",
                    "raw": line,
                }
    except Exception:
        pass

    if not binary:
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 37:
                    return parse_version_rx(data[i:], binary=True)
    return None
