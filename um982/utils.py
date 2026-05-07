import math
import re
import struct
from dataclasses import asdict
from typing import List, Optional, Dict, Any, Union

from .models import UnicoreHeader, NMEAMessage, ParsedResponse


def parse_unicore_header(header_bytes: bytes) -> Optional[UnicoreHeader]:
    """Разобрать заголовок Unicore (24 байта)."""
    if len(header_bytes) < 24:
        return None

    try:
        sync1, sync2, sync3, cpu_idle = struct.unpack("<BBBB", header_bytes[0:4])

        if sync1 != 0xAA or sync2 != 0x44 or sync3 != 0xB5:
            return None

        message_id = struct.unpack("<H", header_bytes[4:6])[0]
        message_length = struct.unpack("<H", header_bytes[6:8])[0]
        time_ref = struct.unpack("<B", header_bytes[8:9])[0]
        time_status = struct.unpack("<B", header_bytes[9:10])[0]
        wn = struct.unpack("<H", header_bytes[10:12])[0]
        ms = struct.unpack("<I", header_bytes[12:16])[0]
        reserved = struct.unpack("<I", header_bytes[16:20])[0]
        version = struct.unpack("<B", header_bytes[20:21])[0]
        leap_sec = struct.unpack("<B", header_bytes[21:22])[0]
        delay_ms = struct.unpack("<H", header_bytes[22:24])[0]

        return UnicoreHeader(
            sync_bytes=f"0x{sync1:02X} 0x{sync2:02X} 0x{sync3:02X}",
            cpu_idle=cpu_idle,
            message_id=message_id,
            message_length=message_length,
            time_ref=time_ref,
            time_status=time_status,
            week_number=wn,
            seconds_of_week_ms=ms,
            reserved=reserved,
            version=version,
            leap_second=leap_sec,
            output_delay_ms=delay_ms,
        )
    except Exception:
        return None


def parse_nmea_messages(text: str) -> List[NMEAMessage]:
    """
    Разбор ASCII-строк с префиксом «$» из текста ответа приёмника (Unicore ASCII: CONFIG, COMMAND, …).

    Протокол NMEA описан в мануале Unicore отдельно (п. 7.1–7.2); строки CONFIG и прочие настройки
    относятся к ASCII-формату протокола Unicore, а не к NMEA. Имя функции сохранено для совместимости.
    """
    messages: List[NMEAMessage] = []

    nmea_pattern = r"\$[A-Z][A-Z0-9]{2,}[^$\r\n]*"

    for match in re.finditer(nmea_pattern, text):
        line = match.group(0).strip()
        if not line:
            continue

        line = line.rstrip("\r")
        parts = line.split(",")
        if not parts:
            continue

        msg_type = parts[0].replace("$", "")
        fields = parts[1:] if len(parts) > 1 else []

        data: Optional[str] = None
        checksum: Optional[str] = None

        if fields and "*" in fields[-1]:
            checksum_part = fields[-1].split("*")
            data = checksum_part[0]
            checksum = checksum_part[1] if len(checksum_part) > 1 else None
        elif fields:
            data = fields[-1]

        messages.append(
            NMEAMessage(
                type=msg_type,
                fields=fields,
                raw=line,
                data=data,
                checksum=checksum,
            )
        )

    return messages


def parse_response(data: bytes) -> ParsedResponse:
    """
    Разобрать бинарный/текстовый ответ устройства в `ParsedResponse`.

    Функция не выбрасывает исключений и пытается вернуть максимум полезной информации.
    """
    if not data:
        return ParsedResponse(raw_bytes=b"", hex="", length=0)

    result = ParsedResponse(
        raw_bytes=data,
        hex=data.hex(),
        length=len(data),
    )

    # Попытка распарсить как ASCII-текст (предложения с «$»: Unicore ASCII, при необходимости и NMEA по гл. 7)
    try:
        text = data.decode("ascii", errors="ignore")
        nmea_messages = parse_nmea_messages(text)
        if nmea_messages:
            result.nmea_messages = nmea_messages
            result.extra["nmea_count"] = len(nmea_messages)
            if len(data) > len(text.encode("ascii", errors="ignore")):
                result.extra["type"] = "mixed"
            else:
                result.extra["type"] = "ASCII"
    except Exception as e:
        result.extra["decode_error"] = str(e)

    # Попытка парсинга как протокол Unicore binary
    if len(data) >= 24:
        header = parse_unicore_header(data[:24])
        if header is not None:
            result.unicore_header = header

            msg_length = header.message_length
            if len(data) >= msg_length:
                data_start = 24
                data_end = msg_length - 4  # исключаем CRC
                crc_start = msg_length - 4
                crc_end = msg_length

                result.extra["unicore_data"] = {
                    "raw": data[data_start:data_end].hex() if data_end > data_start else "",
                    "length": data_end - data_start if data_end > data_start else 0,
                }

                crc_bytes = data[crc_start:crc_end] if crc_end > crc_start else b""
                crc_value = struct.unpack("<I", crc_bytes)[0] if len(crc_bytes) == 4 else None
                result.extra["unicore_crc"] = {
                    "raw": crc_bytes.hex(),
                    "value": crc_value,
                }

    # Резервный вариант: общий бинарный парсинг
    if "unicore_data" not in result.extra and len(data) >= 2:
        result.extra["binary"] = {
            "header": data[:2].hex(),
            "payload": data[2:].hex() if len(data) > 2 else None,
        }

    return result


def parsed_response_to_legacy_dict(parsed: ParsedResponse) -> Dict[str, Any]:
    """
    Преобразовать `ParsedResponse` в словарь в стиле старого API `parse_binary_response`.
    """
    result: Dict[str, Any] = {
        "raw_bytes": parsed.raw_bytes,
        "hex": parsed.hex,
        "length": parsed.length,
        "parsed": {},
    }

    if parsed.nmea_messages:
        result["parsed"]["type"] = parsed.extra.get("type")
        result["parsed"]["messages"] = [asdict(m) for m in parsed.nmea_messages]
        result["parsed"]["nmea_count"] = parsed.extra.get("nmea_count", len(parsed.nmea_messages))

    if parsed.unicore_header is not None:
        result["parsed"]["unicore_binary"] = {
            "header": asdict(parsed.unicore_header),
        }
        if "unicore_data" in parsed.extra:
            result["parsed"]["unicore_binary"]["data"] = parsed.extra["unicore_data"]
        if "unicore_crc" in parsed.extra:
            result["parsed"]["unicore_binary"]["crc"] = parsed.extra["unicore_crc"]

    # Прочие дополнительные поля переносим как есть
    for key, value in parsed.extra.items():
        if key not in {"type", "nmea_count", "unicore_data", "unicore_crc"}:
            result["parsed"][key] = value

    return result


def parse_log_period_str(value: Union[str, int, float, None]) -> float:
    """
    Период выдачи LOG / query (эпохи): неотрицательное число; в строке допускается «,» как разделитель.
    """
    if value is None:
        raise ValueError("period is None")
    if isinstance(value, (int, float)):
        v = float(value)
        if not math.isfinite(v) or v < 0 or v > 1e6:
            raise ValueError("period out of range")
        return v
    t = str(value).strip().replace(",", ".")
    if not t:
        raise ValueError("period empty")
    v = float(t)
    if not math.isfinite(v) or v < 0 or v > 1e6:
        raise ValueError("period out of range")
    return v


def format_log_period_wire(x: float) -> str:
    """Токен периода для ASCII-команды «MSG … period»."""
    if not math.isfinite(x) or x < 0:
        raise ValueError("invalid period for wire")
    if abs(x - round(x)) < 1e-9:
        return str(int(round(x)))
    return format(x, "g")


def format_log_period_display(x: float) -> str:
    """Отображение периода в полях GUI."""
    if not math.isfinite(x):
        return "1"
    if abs(x - round(x)) < 1e-9:
        return str(int(round(x)))
    return format(x, "g")


