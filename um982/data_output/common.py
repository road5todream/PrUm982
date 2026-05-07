"""Общие утилиты data output: _run_data_query, _make_unicore_header_checker, поиск заголовка Unicore и маркеров."""
import time
from typing import Callable, Dict, Any, Optional, Tuple

from um982.core import Um982Core
from um982.utils import parse_unicore_header


# Синхрослово Unicore (3 байта)
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
    """Позиция первого символа «$» (начало ASCII-строки в потоке Unicore; не смешивать с отдельным протоколом NMEA)."""
    pos = data.find(b"$")
    return pos if pos >= 0 else -1


def _run_data_query(
    core: Um982Core,
    *,
    command: str,
    parse_func: Callable[[bytes, bool], Optional[dict]],
    binary: bool = False,
    add_crlf: Optional[bool] = None,
    wait_time: float = 0.0,
    read_attempts: int = 48,
    first_read_timeout: float = 0.32,
    read_timeout: float = 0.14,
    max_wait: float = 10.0,
    check_complete: Optional[Callable[[bytes, bool], bool]] = None,
    result_key: str = "data",
) -> Dict[str, Any]:
    """Общий helper для запросов потоковых данных (query_*).

    Без фиксированной многосекундной паузы до чтения: первое чтение с чуть большим
    окном (first_read_timeout) — чтобы пойти первый байт ответа без лишних циклов;
    дальше короткие read_response до check_complete. Устаревший wait_time>0 —
    совместимость (один sleep перед циклом).
    """
    if add_crlf is None:
        add_crlf = core.baudrate >= 460800

    if core.serial_conn and core.serial_conn.in_waiting > 0:
        core.serial_conn.reset_input_buffer()

    if not core.send_ascii_command(command, add_crlf=add_crlf):
        return {"error": f"Failed to send {command} command"}

    if wait_time > 0:
        time.sleep(wait_time)

    response = b""
    deadline = time.monotonic() + max_wait
    for attempt in range(read_attempts):
        if time.monotonic() >= deadline:
            break
        rt = first_read_timeout if attempt == 0 else read_timeout
        chunk = core.read_response(timeout=rt)
        if chunk:
            response += chunk
            if check_complete and check_complete(response, binary):
                break
            if not check_complete and len(response) > 100:
                break
        elif response:
            if check_complete and check_complete(response, binary):
                break
            if not check_complete and len(response) > 100:
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
                tag = ascii_tag.decode("ascii")
                pos = text.find(tag)
                if pos >= 0 and text.startswith(tag, pos):
                    end_pos = min(len(text), pos + ascii_window)
                    payload_start = pos + len(tag)
                    # Первый '*' в окне может относиться к другой строке (#OTHER;...*).
                    # Берём первую '*' такую, что между концом тега и '*' нет нового '#'.
                    star = -1
                    search_from = pos
                    while search_from < end_pos:
                        cand = text.find("*", search_from, end_pos)
                        if cand < 0:
                            break
                        if text.find("#", payload_start, cand) < 0:
                            star = cand
                            break
                        search_from = cand + 1
                    if star > pos:
                        frame = text[pos:star]
                        if ";" in frame:
                            return True
                        j = payload_start
                        while j < star and text[j] in " \t":
                            j += 1
                        if j < star and text[j] in ",;":
                            return True
            except Exception:
                pass
        return len(data) > ascii_min_total

    return _checker
