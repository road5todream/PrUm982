"""Логи/унилог: query_uniloglist, log, unlog, _parse_uniloglist_message."""
import time
from typing import Any, Dict, Optional, Union

from um982.core import Um982Core
from um982.utils import format_log_period_wire, parse_log_period_str

from .common import _run_data_query


def _send_config_command_via_core(
    core: Um982Core,
    command: str,
    command_name: str = "",
    add_crlf: Optional[bool] = None,
    *,
    accept_empty_response: bool = False,
) -> Dict[str, Any]:
    """Отправка CONFIG-подобной команды через Um982Core (send_ascii + read + parse)."""
    if add_crlf is None:
        add_crlf = getattr(core, "baudrate", 115200) >= 460800
    if core.serial_conn and core.serial_conn.in_waiting > 0:
        core.serial_conn.reset_input_buffer()
    if not core.send_ascii_command(command, add_crlf=add_crlf):
        return {"error": f"Не удалось отправить команду {command}"}
    time.sleep(0.5)
    response = b""
    for _ in range(8):
        time.sleep(0.15)
        chunk = core.read_response(timeout=1.2)
        if chunk:
            response += chunk
        elif response:
            break
    if not response:
        if accept_empty_response:
            note = (
                "Ответ не получен — после UNLOG это ожидаемо: приёмник может отключить вывод на COM."
                if (command_name or "").upper() == "UNLOG"
                else "Ответ не получен после отправки команды."
            )
            return {
                "command": command,
                "response": {"raw_bytes": b"", "parsed": {}},
                "confirmation": None,
                "success": True,
                "empty_response_after_send": True,
                "note": note,
            }
        return {"error": "Ответ не получен"}
    parsed = core.parse_binary_response(response)
    messages = parsed.get("parsed", {}).get("messages", [])
    confirmation = None
    cmd_upper = (command_name or "").upper()
    for msg in messages:
        raw = msg.get("raw", "")
        raw_upper = raw.upper()
        if "COMMAND" in raw_upper and "OK" in raw_upper:
            if not cmd_upper or cmd_upper in raw_upper:
                confirmation = raw
                break
    return {
        "command": command,
        "response": parsed,
        "confirmation": confirmation,
        "success": confirmation is not None or len(messages) > 0,
    }


def _check_uniloglist_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        return len(data) > 100
    try:
        text = data.decode("ascii", errors="ignore")
        if "#UNILOGLIST" in text:
            uniloglist_pos = text.find("#UNILOGLIST")
            if uniloglist_pos >= 0:
                end_pos = min(len(text), uniloglist_pos + 5000)
                if "*" in text[uniloglist_pos:end_pos]:
                    return True
    except Exception:
        pass
    return len(data) > 1000


def _parse_uniloglist_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        uniloglist_pos = text.find("#UNILOGLIST,")
        if uniloglist_pos < 0:
            return None
        semicolon_pos = text.find(";", uniloglist_pos + 12)
        if semicolon_pos <= uniloglist_pos:
            return None

        crc_pos = text.find("*", semicolon_pos)
        if crc_pos < 0:
            next_msg_pos = text.find("#", semicolon_pos + 1)
            if next_msg_pos > semicolon_pos and next_msg_pos < semicolon_pos + 20000:
                crc_pos = next_msg_pos
            else:
                # Следующая ASCII-строка с «$» (не обязательно NMEA по гл. 7)
                dollar_pos = text.find("$", semicolon_pos)
                if dollar_pos > semicolon_pos and dollar_pos < semicolon_pos + 20000:
                    crc_pos = dollar_pos
                else:
                    crc_pos = min(len(text), semicolon_pos + 15000)

        line = text[uniloglist_pos:crc_pos]
        if not line.startswith("#UNILOGLIST,"):
            return None

        parts = line.split(";", 1)
        if len(parts) < 2:
            return None
        data_part = parts[1]

        logs: list[dict] = []
        lines = data_part.split("\n")
        log_count: Optional[int] = None

        for log_line in lines:
            log_line = log_line.strip()
            if not log_line or not log_line.startswith("<"):
                continue

            log_line = log_line[1:].lstrip("\t ")

            if log_line.isdigit():
                try:
                    log_count = int(log_line)
                except ValueError:
                    pass
                continue

            fields = log_line.split()
            if len(fields) >= 3:
                message = fields[0]
                port = fields[1]
                trigger_str = fields[2]

                if "$" in trigger_str:
                    trigger_str = trigger_str.split("$")[0]

                if trigger_str.isdigit():
                    trigger = "ONTIME"
                    try:
                        period = int(trigger_str)
                    except ValueError:
                        period = 1
                elif trigger_str.upper() == "ONCHANGED":
                    trigger = "ONCHANGED"
                    period = None
                else:
                    trigger = trigger_str
                    period = None

                log_entry: dict[str, Any] = {
                    "port": port,
                    "message": message,
                    "trigger": trigger,
                }
                if period is not None:
                    log_entry["period"] = period
                logs.append(log_entry)
            elif len(fields) >= 2:
                message = fields[0]
                port = fields[1]
                log_entry = {
                    "port": port,
                    "message": message,
                    "trigger": "ONTIME",
                }
                logs.append(log_entry)

        if logs or log_count is not None:
            return {
                "format": "ascii",
                "logs": logs,
                "count": log_count if log_count is not None else len(logs),
                "raw": line,
            }
    except Exception:
        pass
    return None


def query_uniloglist(
    core: Um982Core,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = "UNILOGLIST"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_uniloglist_message,
        binary=False,
        add_crlf=add_crlf,
        read_attempts=14,
        max_wait=4.0,
        check_complete=_check_uniloglist_complete,
        result_key="uniloglist",
    )


def unlog(
    core: Um982Core,
    port: Optional[str] = None,
    message: Optional[str] = None,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    valid_ports = ("COM1", "COM2", "COM3")
    if port is not None:
        port_upper = port.upper().strip()
        if port_upper not in valid_ports:
            return {"error": f"Порт должен быть COM1, COM2 или COM3, получено: {port!r}"}
        port = port_upper

    parts = ["UNLOG"]
    if port is not None:
        parts.append(port)
    if message is not None and message.strip():
        parts.append(message.strip())
    command = " ".join(parts)
    return _send_config_command_via_core(
        core, command, "UNLOG", add_crlf=add_crlf, accept_empty_response=True
    )


def log(
    core: Um982Core,
    message: str,
    port: Optional[str] = None,
    rate: Union[int, float] = 1,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Включить вывод сообщения на COM-порт приёмника.

    Формат как в скриптах восстановления и benchmark: «MSG [COMn] period»,
    например BESTNAVA COM1 10 или BESTNAVA 1.5. Период — неотрицательное число (допускается дробь).
    Ключевое слово ONTIME в команду не подставляем — на части прошивок оно ломает ожидаемый поток и фоновый разбор кадров в GUI.
    """
    valid_ports = ("COM1", "COM2", "COM3")
    if port is not None:
        port_upper = port.upper().strip()
        if port_upper not in valid_ports:
            return {"error": f"Порт должен быть COM1, COM2 или COM3, получено: {port!r}"}
        port = port_upper
    msg = message.strip().upper()
    try:
        rf = parse_log_period_str(rate)
    except ValueError as e:
        return {"error": f"Некорректный период (rate): {rate!r} ({e})"}
    parts = [msg, format_log_period_wire(rf)]
    if port is not None:
        parts.insert(1, port)
    command = " ".join(parts)
    return _send_config_command_via_core(core, command, msg, add_crlf=add_crlf)
