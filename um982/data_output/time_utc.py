"""Преобразование времени в UTC: GPSUTC, BD3UTC — запросы, парсеры, общая модель UtcOffsetParams."""
import re
import struct
from dataclasses import dataclass
from typing import Any, Dict, Optional

from um982.core import Um982Core
from um982.utils import parse_unicore_header

from .base import _run_data_query, _make_unicore_header_checker


# --- Общая модель параметров смещения UTC ---

@dataclass
class UtcOffsetParams:
    """Унифицированные параметры перевода системного времени в UTC (GPS или BDS-3)."""
    system: str  # "GPS" | "BD3"
    utc_wn: int = 0
    tot: int = 0
    A0: float = 0.0
    A1: float = 0.0
    A2: Optional[float] = None  # только BD3
    wn_lsf: int = 0
    dn: int = 0
    delta_ls: int = 0
    delta_lsf: int = 0
    delta_utc: Optional[int] = None  # только GPS
    reserved: int = 0
    reserved2: Optional[int] = None  # только BD3
    format: str = "ascii"
    raw: Optional[str] = None
    header: Optional[Dict[str, Any]] = None
    crc: Optional[str] = None
    message_offset: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "system": self.system,
            "utc_wn": self.utc_wn,
            "tot": self.tot,
            "A0": self.A0,
            "A1": self.A1,
            "wn_lsf": self.wn_lsf,
            "dn": self.dn,
            "delta_ls": self.delta_ls,
            "delta_lsf": self.delta_lsf,
            "reserved": self.reserved,
            "format": self.format,
        }
        if self.A2 is not None:
            out["A2"] = self.A2
        if self.delta_utc is not None:
            out["delta_utc"] = self.delta_utc
        if self.reserved2 is not None:
            out["reserved2"] = self.reserved2
        if self.raw is not None:
            out["raw"] = self.raw
        if self.header is not None:
            out["header"] = dict(self.header)
        if self.crc is not None:
            out["crc"] = self.crc
        if self.message_offset is not None:
            out["message_offset"] = self.message_offset
        return out

    @classmethod
    def from_parsed(cls, d: Dict[str, Any], system: str) -> "UtcOffsetParams":
        """Собрать UtcOffsetParams из результата парсера GPSUTC или BD3UTC."""
        return cls(
            system=system,
            utc_wn=d.get("utc_wn", 0),
            tot=d.get("tot", 0),
            A0=d.get("A0", 0.0),
            A1=d.get("A1", 0.0),
            A2=d.get("A2"),
            wn_lsf=d.get("wn_lsf", 0),
            dn=d.get("dn", 0),
            delta_ls=d.get("delta_ls", 0),
            delta_lsf=d.get("delta_lsf", 0),
            delta_utc=d.get("delta_utc"),
            reserved=d.get("reserved", 0),
            reserved2=d.get("reserved2"),
            format=d.get("format", "ascii"),
            raw=d.get("raw"),
            header=d.get("header"),
            crc=d.get("crc"),
            message_offset=d.get("message_offset"),
        )


def _utc_complete_checker(message_id: int, ascii_marker: bytes):
    return _make_unicore_header_checker(
        message_id,
        ascii_tag=ascii_marker,
        ascii_window=500,
        binary_min_total=80,
        ascii_min_total=50,
    )


# --- Парсеры ---

def _parse_gpsutc_message(data: bytes, binary: bool = False) -> Optional[dict]:
    """Парсинг ответа GPSUTC (параметры перевода времени GPS в UTC)."""
    if binary:
        if len(data) < 24:
            return None
        payload_len = 48
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 19:
                    continue
                offset = i + 24
                if len(data) < offset + payload_len:
                    continue

                utc_wn = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                tot = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                a0 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                a1 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                wn_lsf = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                dn = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                delta_ls = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                delta_lsf = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                delta_utc = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                reserved = struct.unpack("<I", data[offset : offset + 4])[0]

                msg_length = header.message_length
                crc_value = None
                if msg_length > 0 and len(data) >= i + msg_length:
                    crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]

                return {
                    "format": "binary",
                    "utc_wn": utc_wn,
                    "tot": tot,
                    "A0": a0,
                    "A1": a1,
                    "wn_lsf": wn_lsf,
                    "dn": dn,
                    "delta_ls": delta_ls,
                    "delta_lsf": delta_lsf,
                    "delta_utc": delta_utc,
                    "reserved": reserved,
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        marker = b"#GPSUTCA"
        pos = data.find(marker)
        if pos >= 0:
            text = data[pos:].decode("ascii", errors="ignore")
        else:
            text = data.decode("ascii", errors="ignore")

        pattern = r"#GPSUTCA[\s\S]*?\*[0-9a-fA-F]{8}"
        for match in re.finditer(pattern, text):
            line = match.group(0)
            idx = line.find("*")
            if idx < 0:
                continue
            data_part_clean = line[:idx].replace("#GPSUTCA", "").strip()
            fields = [f.strip() for f in re.split(r"[,\r\n]+", data_part_clean) if f.strip()]
            if len(fields) < 10:
                continue

            if len(fields) >= 19:
                data_fields = fields[9:19]
            else:
                data_fields = fields[-10:]
            if len(data_fields) < 10:
                continue

            utc_wn = int(data_fields[0])
            tot = int(data_fields[1])
            a0 = float(data_fields[2])
            a1 = float(data_fields[3])
            wn_lsf = int(data_fields[4])
            dn = int(data_fields[5])
            delta_ls = int(data_fields[6])
            delta_lsf = int(data_fields[7])
            delta_utc = int(data_fields[8])
            reserved = int(data_fields[9])

            return {
                "format": "ascii",
                "utc_wn": utc_wn,
                "tot": tot,
                "A0": a0,
                "A1": a1,
                "wn_lsf": wn_lsf,
                "dn": dn,
                "delta_ls": delta_ls,
                "delta_lsf": delta_lsf,
                "delta_utc": delta_utc,
                "reserved": reserved,
                "raw": line.strip(),
            }
    except Exception:
        pass

    return None


def _parse_bd3utc_message(data: bytes, binary: bool = False) -> Optional[dict]:
    """Парсинг ответа BD3UTC (параметры перевода времени BDS-3 в UTC)."""
    if binary:
        if len(data) < 24:
            return None
        payload_len = 56
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 22:
                    continue
                offset = i + 24
                if len(data) < offset + payload_len:
                    continue

                utc_wn = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                tot = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                a0 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                a1 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                a2 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                wn_lsf = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                dn = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                delta_ls = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                delta_lsf = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                reserved1 = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                reserved2 = struct.unpack("<I", data[offset : offset + 4])[0]

                msg_length = header.message_length
                crc_value = None
                if msg_length > 0 and len(data) >= i + msg_length:
                    crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]

                return {
                    "format": "binary",
                    "utc_wn": utc_wn,
                    "tot": tot,
                    "A0": a0,
                    "A1": a1,
                    "A2": a2,
                    "wn_lsf": wn_lsf,
                    "dn": dn,
                    "delta_ls": delta_ls,
                    "delta_lsf": delta_lsf,
                    "reserved": reserved1,
                    "reserved2": reserved2,
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        marker = b"#BD3UTCA"
        pos = data.find(marker)
        if pos >= 0:
            text = data[pos:].decode("ascii", errors="ignore")
        else:
            text = data.decode("ascii", errors="ignore")

        pattern = r"#BD3UTCA[\s\S]*?\*[0-9a-fA-F]{8}"
        for match in re.finditer(pattern, text):
            line = match.group(0)
            idx = line.find("*")
            if idx < 0:
                continue
            data_part_clean = line[:idx].replace("#BD3UTCA", "").strip()
            fields = [f.strip() for f in re.split(r"[,\r\n]+", data_part_clean) if f.strip()]
            if len(fields) < 10:
                continue

            if len(fields) >= 20:
                data_fields = fields[9:20]
            elif len(fields) >= 11:
                data_fields = fields[-11:]
            else:
                data_fields = fields[-10:] + ["0"]

            if len(data_fields) < 10:
                continue

            utc_wn = int(data_fields[0])
            tot = int(data_fields[1])
            a0 = float(data_fields[2])
            a1 = float(data_fields[3])
            a2 = float(data_fields[4])
            wn_lsf = int(data_fields[5])
            dn = int(data_fields[6])
            delta_ls = int(data_fields[7])
            delta_lsf = int(data_fields[8])
            reserved1 = int(data_fields[9])
            reserved2 = int(data_fields[10]) if len(data_fields) > 10 else 0

            return {
                "format": "ascii",
                "utc_wn": utc_wn,
                "tot": tot,
                "A0": a0,
                "A1": a1,
                "A2": a2,
                "wn_lsf": wn_lsf,
                "dn": dn,
                "delta_ls": delta_ls,
                "delta_lsf": delta_lsf,
                "reserved": reserved1,
                "reserved2": reserved2,
                "raw": line.strip(),
            }
    except Exception:
        pass

    return None


# --- Запросы ---

def query_gpsutc(
    core: Um982Core,
    rate: int = 1,
    trigger: Optional[str] = None,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    if trigger and trigger.upper() == "ONCHANGED":
        command = "GPSUTCB ONCHANGED" if binary else "GPSUTCA ONCHANGED"
    else:
        command = f"GPSUTCB {rate}" if binary else f"GPSUTCA {rate}"

    check_complete = _utc_complete_checker(19, b"#GPSUTCA")

    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_gpsutc_message,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.5,
        read_attempts=10,
        read_timeout=1.5,
        check_complete=check_complete,
        result_key="gpsutc",
    )


def query_bd3utc(
    core: Um982Core,
    rate: int = 1,
    trigger: Optional[str] = None,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    if trigger and trigger.upper() == "ONCHANGED":
        command = "BD3UTCB ONCHANGED" if binary else "BD3UTCA ONCHANGED"
    else:
        command = f"BD3UTCB {rate}" if binary else f"BD3UTCA {rate}"

    check_complete = _utc_complete_checker(22, b"#BD3UTCA")

    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_bd3utc_message,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.5,
        read_attempts=10,
        read_timeout=1.5,
        check_complete=check_complete,
        result_key="bd3utc",
    )
