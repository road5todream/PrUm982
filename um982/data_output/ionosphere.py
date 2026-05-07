"""Ионосфера всех систем: GPSION, GALION, BDSION, BD3ION — запросы, парсеры, единая модель IonosphereModel и конвертеры."""
import re
import struct
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from um982.core import Um982Core
from um982.utils import format_log_period_wire, parse_log_period_str, parse_unicore_header

from .base import _run_data_query, _make_unicore_header_checker


# --- Единая модель ионосферы и конвертеры ---

@dataclass
class IonosphereModel:
    """
    Унифицированное представление параметров ионосферы любой системы.
    alpha — коэффициенты Клонбуха (секунды, 3 или 4 или 9 в зависимости от системы);
    beta — коэффициенты (секунды), только GPS/BDS;
    sf — масштабные множители (5 байт/значений), только Galileo;
    a — альтернативное имя для alpha (a1..a9), BD3.
    """
    system: str  # "GPS" | "GAL" | "BDS" | "BD3"
    alpha: Dict[str, float] = field(default_factory=dict)  # a0,a1,a2,a3 или a1..a9
    beta: Optional[Dict[str, float]] = None   # b0..b3 для GPS/BDS
    sf: Optional[Dict[str, int]] = None      # sf1..sf5 для Galileo
    us_svid: Optional[int] = None
    us_week: Optional[int] = None
    ul_sec: Optional[int] = None
    reserved: Optional[int] = None
    format: str = "ascii"  # "ascii" | "binary"
    raw: Optional[str] = None
    header: Optional[Dict[str, Any]] = None
    crc: Optional[str] = None
    message_offset: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Единый словарь для API/отображения: все поля в одном виде."""
        out: Dict[str, Any] = {
            "system": self.system,
            "alpha": dict(self.alpha),
            "format": self.format,
        }
        if self.beta is not None:
            out["beta"] = dict(self.beta)
        if self.sf is not None:
            out["sf"] = dict(self.sf)
        if self.us_svid is not None:
            out["us_svid"] = self.us_svid
        if self.us_week is not None:
            out["us_week"] = self.us_week
        if self.ul_sec is not None:
            out["ul_sec"] = self.ul_sec
        if self.reserved is not None:
            out["reserved"] = self.reserved
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
    def from_parsed(cls, d: Dict[str, Any], system: str) -> "IonosphereModel":
        """Собрать IonosphereModel из результата парсера (любой из четырёх систем)."""
        alpha: Dict[str, float] = {}
        beta: Optional[Dict[str, float]] = None
        sf: Optional[Dict[str, int]] = None
        if "alpha" in d:
            alpha = dict(d["alpha"])
        if "a" in d:
            alpha = dict(d["a"])
        if "beta" in d:
            beta = dict(d["beta"])
        if "sf" in d:
            sf = dict(d["sf"])
        return cls(
            system=system,
            alpha=alpha,
            beta=beta,
            sf=sf,
            us_svid=d.get("us_svid"),
            us_week=d.get("us_week"),
            ul_sec=d.get("ul_sec"),
            reserved=d.get("reserved"),
            format=d.get("format", "ascii"),
            raw=d.get("raw"),
            header=d.get("header"),
            crc=d.get("crc"),
            message_offset=d.get("message_offset"),
        )


def _ionosphere_log_wire_command(
    stem: str,
    *,
    binary: bool,
    rate: Optional[Union[int, float]],
    trigger: Optional[str],
) -> str:
    """
    Строка LOG-запроса ионосферы на провод (§7.3.x): «STEMB|A», «STEMB 1», «STEMB ONCHANGED».
    Без периода (rate is None и не ONCHANGED) — только имя сообщения, как одиночный запрос без частоты.
    """
    s = stem.strip().upper()
    suf = "B" if binary else "A"
    if trigger and trigger.upper() == "ONCHANGED":
        return f"{s}{suf} ONCHANGED"
    if rate is None:
        return f"{s}{suf}"
    try:
        rf = parse_log_period_str(rate)
        if rf <= 0:
            return f"{s}{suf}"
    except ValueError:
        return f"{s}{suf}"
    return f"{s}{suf} {format_log_period_wire(rf)}"


def _ionosphere_complete_checker(message_id: int, ascii_marker: bytes) -> Any:
    """Checker для ионосферных сообщений (binary по message_id, ASCII по маркеру)."""
    return _make_unicore_header_checker(
        message_id,
        ascii_tag=ascii_marker,
        ascii_window=500,
        # Fallback «достаточно байт»: строго > порога (см. common._make_unicore_header_checker). 71 — типичный BD3ION ~72 B.
        binary_min_total=71,
        ascii_min_total=50,
    )


# --- Парсеры (бинарный + ASCII) ---

def _parse_gpsion_message(data: bytes, binary: bool = False) -> Optional[dict]:
    """Парсинг ответа GPSION (ионосферные параметры GPS)."""
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 8:
                    offset = i + 24
                    if len(data) < offset + 32:
                        continue
                    a0 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    a1 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    a2 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    a3 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    if len(data) < offset + 32:
                        continue
                    b0 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    b1 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    b2 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    b3 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    if len(data) < offset + 2:
                        continue
                    us_svid = struct.unpack("<H", data[offset : offset + 2])[0]
                    offset += 2
                    if len(data) < offset + 2:
                        continue
                    us_week = struct.unpack("<H", data[offset : offset + 2])[0]
                    offset += 2
                    if len(data) < offset + 4:
                        continue
                    ul_sec = struct.unpack("<I", data[offset : offset + 4])[0]
                    offset += 4
                    if len(data) < offset + 4:
                        continue
                    reserved = struct.unpack("<I", data[offset : offset + 4])[0]
                    msg_length = header.message_length
                    crc_value = None
                    if msg_length > 0 and len(data) >= i + msg_length:
                        crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]
                    return {
                        "format": "binary",
                        "alpha": {"a0": a0, "a1": a1, "a2": a2, "a3": a3},
                        "beta": {"b0": b0, "b1": b1, "b2": b2, "b3": b3},
                        "us_svid": us_svid,
                        "us_week": us_week,
                        "ul_sec": ul_sec,
                        "reserved": reserved,
                        "header": {"message_id": header.message_id, "message_length": header.message_length},
                        "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                        "message_offset": i,
                    }
        return None

    try:
        gpsion_marker = b"#GPSIONA"
        gpsion_pos = data.find(gpsion_marker)
        text = data[gpsion_pos:].decode("ascii", errors="ignore") if gpsion_pos >= 0 else data.decode("ascii", errors="ignore")
        pattern = r"#GPSIONA[^\r\n]*"
        for match in re.finditer(pattern, text):
            line = match.group(0).strip()
            if not line:
                continue
            parts = line.split(";")
            if len(parts) < 2:
                continue
            data_part = parts[1]
            data_part_clean = data_part.split("*")[0]
            fields = data_part_clean.split(",")
            if len(fields) < 12:
                continue
            a0 = float(fields[0]) if fields[0] else 0.0
            a1 = float(fields[1]) if fields[1] else 0.0
            a2 = float(fields[2]) if fields[2] else 0.0
            a3 = float(fields[3]) if fields[3] else 0.0
            b0 = float(fields[4]) if fields[4] else 0.0
            b1 = float(fields[5]) if fields[5] else 0.0
            b2 = float(fields[6]) if fields[6] else 0.0
            b3 = float(fields[7]) if fields[7] else 0.0
            us_svid = int(fields[8]) if fields[8] else 0
            us_week = int(fields[9]) if fields[9] else 0
            ul_sec = int(fields[10]) if fields[10] else 0
            reserved = int(fields[11]) if fields[11] else 0
            return {
                "format": "ascii",
                "alpha": {"a0": a0, "a1": a1, "a2": a2, "a3": a3},
                "beta": {"b0": b0, "b1": b1, "b2": b2, "b3": b3},
                "us_svid": us_svid,
                "us_week": us_week,
                "ul_sec": ul_sec,
                "reserved": reserved,
                "raw": line,
            }
    except Exception:
        pass
    return None


def _parse_galion_message(data: bytes, binary: bool = False) -> Optional[dict]:
    """Парсинг ответа GALION (ионосферные параметры Galileo)."""
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 9:
                    offset = i + 24
                    if len(data) < offset + 3 * 8:
                        continue
                    a0 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    a1 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    a2 = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    if len(data) < offset + 5:
                        continue
                    sf = list(data[offset : offset + 5])
                    offset += 5
                    if len(data) < offset + 4:
                        continue
                    reserved = struct.unpack("<I", data[offset : offset + 4])[0]
                    msg_length = header.message_length
                    crc_value = None
                    if msg_length > 0 and len(data) >= i + msg_length:
                        crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]
                    return {
                        "format": "binary",
                        "alpha": {"a0": a0, "a1": a1, "a2": a2},
                        "sf": {"sf1": sf[0], "sf2": sf[1], "sf3": sf[2], "sf4": sf[3], "sf5": sf[4]},
                        "reserved": reserved,
                        "header": {"message_id": header.message_id, "message_length": header.message_length},
                        "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                        "message_offset": i,
                    }
        return None

    try:
        marker = b"#GALIONA"
        pos = data.find(marker)
        text = data[pos:].decode("ascii", errors="ignore") if pos >= 0 else data.decode("ascii", errors="ignore")
        pattern = r"#GALIONA[^\r\n]*"
        for match in re.finditer(pattern, text):
            line = match.group(0).strip()
            if not line:
                continue
            parts = line.split(";")
            if len(parts) < 2:
                continue
            data_part = parts[1]
            data_part_clean = data_part.split("*")[0]
            fields = [f for f in data_part_clean.split(",") if f != ""]
            if len(fields) < 9:
                continue
            a0, a1, a2 = float(fields[0]), float(fields[1]), float(fields[2])
            sf1, sf2, sf3, sf4, sf5 = int(fields[3]), int(fields[4]), int(fields[5]), int(fields[6]), int(fields[7])
            reserved = int(fields[8])
            return {
                "format": "ascii",
                "alpha": {"a0": a0, "a1": a1, "a2": a2},
                "sf": {"sf1": sf1, "sf2": sf2, "sf3": sf3, "sf4": sf4, "sf5": sf5},
                "reserved": reserved,
                "raw": line,
            }
    except Exception:
        pass
    return None


def _parse_bdsion_message(data: bytes, binary: bool = False) -> Optional[dict]:
    """Парсинг ответа BDSION (ионосферные параметры BDS)."""
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 4:
                    continue
                offset = i + 24
                if len(data) < offset + 32:
                    continue
                a0 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                a1 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                a2 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                a3 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                if len(data) < offset + 32:
                    continue
                b0 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                b1 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                b2 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                b3 = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                if len(data) < offset + 2:
                    continue
                us_svid = struct.unpack("<H", data[offset : offset + 2])[0]
                offset += 2
                if len(data) < offset + 2:
                    continue
                us_week = struct.unpack("<H", data[offset : offset + 2])[0]
                offset += 2
                if len(data) < offset + 4:
                    continue
                ul_sec = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                if len(data) < offset + 4:
                    continue
                reserved = struct.unpack("<I", data[offset : offset + 4])[0]
                msg_length = header.message_length
                crc_value = None
                if msg_length > 0 and len(data) >= i + msg_length:
                    crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]
                return {
                    "format": "binary",
                    "alpha": {"a0": a0, "a1": a1, "a2": a2, "a3": a3},
                    "beta": {"b0": b0, "b1": b1, "b2": b2, "b3": b3},
                    "us_svid": us_svid,
                    "us_week": us_week,
                    "ul_sec": ul_sec,
                    "reserved": reserved,
                    "header": {"message_id": header.message_id, "message_length": header.message_length},
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        marker = b"#BDSIONA"
        pos = data.find(marker)
        text = data[pos:].decode("ascii", errors="ignore") if pos >= 0 else data.decode("ascii", errors="ignore")
        pattern = r"#BDSIONA[^\r\n]*"
        for match in re.finditer(pattern, text):
            line = match.group(0).strip()
            if not line:
                continue
            parts = line.split(";")
            if len(parts) < 2:
                continue
            data_part = parts[1]
            data_part_clean = data_part.split("*")[0]
            fields = [f for f in data_part_clean.split(",") if f != ""]
            if len(fields) < 12:
                continue
            a0, a1, a2, a3 = float(fields[0]), float(fields[1]), float(fields[2]), float(fields[3])
            b0, b1, b2, b3 = float(fields[4]), float(fields[5]), float(fields[6]), float(fields[7])
            us_svid, us_week, ul_sec = int(fields[8]), int(fields[9]), int(fields[10])
            reserved = int(fields[11])
            return {
                "format": "ascii",
                "alpha": {"a0": a0, "a1": a1, "a2": a2, "a3": a3},
                "beta": {"b0": b0, "b1": b1, "b2": b2, "b3": b3},
                "us_svid": us_svid,
                "us_week": us_week,
                "ul_sec": ul_sec,
                "reserved": reserved,
                "raw": line,
            }
    except Exception:
        pass
    return None


def _parse_bd3ion_message(data: bytes, binary: bool = False) -> Optional[dict]:
    """
    Парсинг ответа BD3ION (ионосферные параметры BDS-3, §7.3.9).
    Формат кадра тот же после запроса «BD3IONB 1», «BD3IONB ONCHANGED» или «BD3IONB» без периода.
    При binary при неудаче пробует ASCII (#BD3IONA).
    """
    # Бинарное тело (разд. 7.3.9): 9×FLOAT + 4 зарезервированных байта + ULONG + CRC в конце кадра.
    _bd3_payload_after_header = 9 * 4 + 4 + 4

    if binary:
        if len(data) >= 24:
            for i in range(len(data) - 24):
                if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                    header = parse_unicore_header(data[i : i + 24])
                    if header and header.message_id != 21:
                        continue
                    offset = i + 24
                    if len(data) < offset + _bd3_payload_after_header:
                        continue
                    a_vals = [struct.unpack("<f", data[offset + k * 4 : offset + (k + 1) * 4])[0] for k in range(9)]
                    offset += 9 * 4
                    offset += 4  # padding до ULONG reserved (оффсет H+40 в мануале)
                    reserved = struct.unpack("<I", data[offset : offset + 4])[0]
                    msg_length = header.message_length
                    crc_value = None
                    if msg_length > 0 and len(data) >= i + msg_length:
                        crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]
                    return {
                        "format": "binary",
                        "a": {f"a{idx + 1}": v for idx, v in enumerate(a_vals)},
                        "reserved": reserved,
                        "header": {"message_id": header.message_id, "message_length": header.message_length},
                        "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                        "message_offset": i,
                    }
        # Бинарное сообщение не найдено — пробуем разобрать ответ как ASCII (#BD3IONA)
        binary = False
        # fall through to ASCII block below

    try:
        marker = b"#BD3IONA"
        pos = data.find(marker)
        text = data[pos:].decode("ascii", errors="ignore") if pos >= 0 else data.decode("ascii", errors="ignore")
        pattern = r"#BD3IONA[^\r\n]*"
        for match in re.finditer(pattern, text):
            line = match.group(0).strip()
            if not line:
                continue
            line_nc = line.split("*", 1)[0].strip()
            if not line_nc.upper().startswith("#BD3IONA"):
                continue
            a_vals: list[float]
            reserved: int
            if ";" in line_nc:
                prefix, body = line_nc.split(";", 1)
                if not prefix.strip().upper().startswith("#BD3IONA"):
                    continue
                body_fields = [f.strip() for f in body.split(",") if f.strip() != ""]
                header_tail = prefix[len("#BD3IONA") :].lstrip(",").strip()
                # #BD3IONA;… — всё после «;»; либо #BD3IONA,заголовок;коэффициенты (реальный вывод UM982).
                if not header_tail:
                    fields = body_fields
                elif len(body_fields) >= 10:
                    fields = body_fields
                else:
                    hdr_fields = [f.strip() for f in header_tail.split(",") if f.strip() != ""]
                    fields = hdr_fields + body_fields
            else:
                body = line_nc[len("#BD3IONA") :].lstrip(",")
                fields = [f.strip() for f in body.split(",") if f.strip() != ""]

            if len(fields) >= 19:
                a_vals = [float(fields[idx]) for idx in range(9, 18)]
                reserved = int(float(fields[18]))
            elif len(fields) == 10:
                a_vals = [float(fields[idx]) for idx in range(9)]
                reserved = int(float(fields[9]))
            else:
                continue
            return {
                "format": "ascii",
                "a": {f"a{idx + 1}": v for idx, v in enumerate(a_vals)},
                "reserved": reserved,
                "raw": line,
            }
    except Exception:
        pass
    return None


# --- Запросы ---

def query_gpsion(
    core: Um982Core,
    rate: Union[int, float, None] = 1,
    trigger: Optional[str] = None,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = _ionosphere_log_wire_command("GPSION", binary=binary, rate=rate, trigger=trigger)
    check_complete = _ionosphere_complete_checker(8, b"#GPSIONA")
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_gpsion_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=check_complete,
        result_key="gpsion",
    )


def query_galion(
    core: Um982Core,
    rate: Union[int, float, None] = 1,
    trigger: Optional[str] = None,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = _ionosphere_log_wire_command("GALION", binary=binary, rate=rate, trigger=trigger)
    check_complete = _ionosphere_complete_checker(9, b"#GALIONA")
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_galion_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=check_complete,
        result_key="galion",
    )


def query_bdsion(
    core: Um982Core,
    rate: Union[int, float, None] = 1,
    trigger: Optional[str] = None,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = _ionosphere_log_wire_command("BDSION", binary=binary, rate=rate, trigger=trigger)
    check_complete = _ionosphere_complete_checker(4, b"#BDSIONA")
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_bdsion_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=check_complete,
        result_key="bdsion",
    )


def query_bd3ion(
    core: Um982Core,
    rate: Union[int, float, None] = 1,
    trigger: Optional[str] = None,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = _ionosphere_log_wire_command("BD3ION", binary=binary, rate=rate, trigger=trigger)
    check_complete = _ionosphere_complete_checker(21, b"#BD3IONA")
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_bd3ion_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=check_complete,
        result_key="bd3ion",
    )
