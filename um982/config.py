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
        hard_deadline = time.time() + 1.2
        saw_config_ascii = False
        config_seen_at: Optional[float] = None

        for _ in range(24):
            now = time.time()
            if now >= hard_deadline:
                break
            time.sleep(0.04)
            # При активных бинарных LOG (например BESTNAVB) поток может быть плотным:
            # читаем крупнее, чтобы быстрее «пробиться» до ASCII-ответа CONFIG.
            chunk = core.read_response(timeout=0.25, max_bytes=65536)
            if chunk:
                response += chunk
                if b"$CONFIG," in response:
                    saw_config_ascii = True
                    if config_seen_at is None:
                        config_seen_at = time.time()
                    # Если именно CONFIG уже увидели и очередь почти пуста — можно завершать чтение.
                    if core.serial_conn and core.serial_conn.in_waiting == 0:
                        break
            elif saw_config_ascii:
                # Уже увидели CONFIG и вход пуст — завершаем быстрее.
                if core.serial_conn and core.serial_conn.in_waiting == 0:
                    break

            # Дополнительный мягкий таймаут после появления CONFIG в буфере:
            # не ждём бесконечно хвост в условиях плотного бинарного потока.
            if config_seen_at is not None and (time.time() - config_seen_at) > 0.25:
                break

        if not response:
            return {"error": "Ответ не получен"}
        if not saw_config_ascii and b"$CONFIG," not in response:
            # Оставляем совместимость: пусть дальше parse_response попробует распарсить общий ответ,
            # но явно отмечаем, что ответ CONFIG мог «утонуть» в бинарном потоке.
            pass

    parsed_struct = parse_response(response)
    parsed = parsed_response_to_legacy_dict(parsed_struct)

    if "parsed" in parsed:
        parsed["config"] = parsed["parsed"]

    return parsed


