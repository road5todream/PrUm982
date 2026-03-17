import time
from typing import Callable, Dict, Any, Optional, Tuple

from um982.core import Um982Core
from um982.utils import parse_unicore_header


UNICORE_SYNC = bytes((0xAA, 0x44, 0xB5))


def find_unicore_sync(data: bytes, start: int = 0) -> Optional[Tuple[int, Any]]:
    """
    Найти первое вхождение заголовка Unicore (0xAA 0x44 0xB5) в data.
    Возвращает (offset, header) или None. header — результат parse_unicore_header.
    """
    for i in range(start, len(data) - 24):
        if data[i : i + 3] == UNICORE_SYNC:
            header = parse_unicore_header(data[i : i + 24])
            if header is not None:
                return (i, header)
    return None


def find_ascii_marker(data: bytes, marker: bytes) -> int:
    """Позиция первого вхождения marker в data, -1 если не найден."""
    pos = data.find(marker)
    return pos if pos >= 0 else -1


def find_nmea_marker(data: bytes) -> int:
    """Позиция первого символа NMEA ('$') в data, -1 если не найден."""
    pos = data.find(b"$")
    return pos if pos >= 0 else -1


def _run_data_query(
    core: Um982Core,
    *,
    command: str,
    parse_func: Callable[[bytes, bool], Optional[dict]],
    binary: bool = False,
    add_crlf: Optional[bool] = None,
    wait_time: float = 0.5,
    read_attempts: int = 10,
    read_timeout: float = 1.5,
    check_complete: Optional[Callable[[bytes, bool], bool]] = None,
    result_key: str = "data",
) -> Dict[str, Any]:
    """Общий helper для запросов потоковых данных (query_*)."""
    if add_crlf is None:
        add_crlf = core.baudrate >= 460800

    if core.serial_conn and core.serial_conn.in_waiting > 0:
        core.serial_conn.reset_input_buffer()

    if not core.send_ascii_command(command, add_crlf=add_crlf):
        return {"error": f"Failed to send {command} command"}

    time.sleep(wait_time)

    response = b""
    for _ in range(read_attempts):
        time.sleep(0.2)
        chunk = core.read_response(timeout=read_timeout)
        if chunk:
            response += chunk
            if check_complete and check_complete(response, binary):
                break
        elif response:
            if check_complete and check_complete(response, binary):
                break
            if len(response) > 100:
                break
        if len(response) > 50000:
            break

    if not response:
        return {"error": "No response received"}

    parsed = core.parse_binary_response(response)
    command_data = parse_func(response, binary)

    return {
        "command": command,
        "response": parsed,
        result_key: command_data,
        "raw_response_length": len(response),
    }


def _make_unicore_header_checker(
    message_id: int,
    *,
    min_length: int = 0,
    ascii_tag: Optional[bytes] = None,
    ascii_window: int = 2000,
    binary_min_total: int = 1000,
    ascii_min_total: int = 500,
) -> Callable[[bytes, bool], bool]:
    """
    Унифицированный генератор функций check_*_complete для потоковых ответов.
    message_id: ID бинарного сообщения Unicore.
    min_length: минимальная длина полезной нагрузки (header уже учтён).
    ascii_tag: префикс ASCII-сообщения вида b'#TAG,'; если None — ASCII не проверяется по тегу.
    ascii_window: окно символов после префикса, в котором ищется '*' и ';'.
    """
    def _checker(data: bytes, is_binary: bool) -> bool:
        if is_binary:
            search_start = max(0, len(data) - 10000)
            for i in range(search_start, len(data) - 24):
                if data[i : i + 3] == UNICORE_SYNC:
                    header = parse_unicore_header(data[i : i + 24])
                    if header and header.message_id == message_id:
                        msg_len = header.message_length
                        if msg_len > 0 and len(data) >= i + msg_len:
                            if min_length <= 0 or msg_len >= 24 + min_length:
                                return True
            return len(data) > binary_min_total
        if ascii_tag:
            try:
                text = data.decode("ascii", errors="ignore")
                pos = text.find(ascii_tag.decode("ascii"))
                if pos >= 0:
                    end_pos = min(len(text), pos + ascii_window)
                    if "*" in text[pos:end_pos]:
                        semicolon_pos = text.find(";", pos)
                        if semicolon_pos > pos and semicolon_pos < end_pos:
                            return True
            except Exception:
                pass
        return len(data) > ascii_min_total

    return _checker
