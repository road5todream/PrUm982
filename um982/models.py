from dataclasses import dataclass, field
from typing import List, Optional, Any, Dict


@dataclass
class UnicoreHeader:
    """Заголовок бинарного сообщения Unicore (24 байта)."""

    sync_bytes: str
    cpu_idle: int
    message_id: int
    message_length: int
    time_ref: int
    time_status: int
    week_number: int
    seconds_of_week_ms: int
    reserved: int
    version: int
    leap_second: int
    output_delay_ms: int


@dataclass
class NMEAMessage:
    """NMEA-подобное сообщение, извлечённое из текстового ответа."""

    type: str
    fields: List[str] = field(default_factory=list)
    raw: str = ""
    data: Optional[str] = None
    checksum: Optional[str] = None


@dataclass
class ParsedResponse:
    """
    Унифицированное представление разобранного ответа приёмника.
    Содержит «сырые» байты, и опциональные структуры более высокого уровня.
    """
    raw_bytes: bytes
    hex: str
    length: int
    nmea_messages: List[NMEAMessage] = field(default_factory=list)
    unicore_header: Optional[UnicoreHeader] = None
    extra: Dict[str, Any] = field(default_factory=dict)


