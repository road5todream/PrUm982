from typing import Optional, Dict, Any
import time

from .core import Um982Core
from .utils import parse_response, parsed_response_to_legacy_dict


def query_config(core: Um982Core, use_lines: bool = False, add_crlf: Optional[bool] = None) -> Dict[str, Any]:
    """Запрос конфигурации приёмника (CONFIG)."""
    if add_crlf is None:
        add_crlf = core.baudrate >= 460800

    if core.serial_conn and core.serial_conn.in_waiting > 0:
        core.serial_conn.reset_input_buffer()

    if not core.send_ascii_command("CONFIG", add_crlf=add_crlf):
        return {"error": "Не удалось отправить команду CONFIG"}

    time.sleep(0.5)

    if use_lines:
        lines = core.read_lines(timeout=3.0, max_lines=200)
        if not lines:
            return {"error": "Ответ не получен"}
        text = "\n".join(lines)
        response = text.encode("ascii")
    else:
        response = b""
        start_time = time.time()
        max_wait_time = 8.0

        for _ in range(20):
            if time.time() - start_time > max_wait_time:
                break
            time.sleep(0.15)
            chunk = core.read_response(timeout=0.6)
            if chunk:
                response += chunk
                start_time = time.time()
            elif response:
                time.sleep(0.15)
                if core.serial_conn and core.serial_conn.in_waiting == 0:
                    break

        if not response:
            return {"error": "Ответ не получен"}

    parsed_struct = parse_response(response)
    parsed = parsed_response_to_legacy_dict(parsed_struct)

    if "parsed" in parsed:
        parsed["config"] = parsed["parsed"]

    return parsed


