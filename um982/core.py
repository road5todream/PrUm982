import os
import time
from typing import Optional

import serial
import serial.tools.list_ports

from .utils import parse_response, parsed_response_to_legacy_dict


def _serial_debug_enabled() -> bool:
    return os.environ.get("UM982_DEBUG", "").strip() in ("1", "true", "yes")


def _is_tcp_port_spec(port: str) -> bool:
    """Проверка, задан ли порт как TCP (host:port или tcp://host:port)."""
    if not port or not isinstance(port, str):
        return False
    port = port.strip()
    if port.lower().startswith("tcp://"):
        return True
    if ":" in port and not port.startswith("/") and "COM" not in port.upper()[:4]:
        parts = port.rsplit(":", 1)
        if len(parts) == 2 and parts[1].isdigit():
            return True
    return False


def _tcp_port_to_socket_url(port: str) -> str:
    """Преобразование спецификации TCP в URL для serial_for_url (socket://host:port)."""
    port = port.strip()
    if port.lower().startswith("tcp://"):
        host_port = port[6:].strip()
    else:
        host_port = port
    return "socket://" + host_port


class Um982Core:
    """
    Базовый класс для низкоуровневого обмена с приёмником UM982.

    Отвечает за:
    - установку/закрытие соединения (UART или TCP);
    - отправку ASCII/бинарных команд;
    - чтение ответов в виде байтов или строк;
    - базовый парсинг ответов в структуру `ParsedResponse`.
    """

    def __init__(self, port: str, baudrate: int = 460800, timeout: float = 1.0, debug: Optional[bool] = None):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.serial_conn: Optional[serial.Serial] = None
        self._is_tcp = _is_tcp_port_spec(port)
        self._debug = debug if debug is not None else _serial_debug_enabled()

    def connect(self) -> bool:
        try:
            if self._is_tcp:
                url = _tcp_port_to_socket_url(self.port)
                self.serial_conn = serial.serial_for_url(
                    url,
                    timeout=self.timeout,
                )
            else:
                self.serial_conn = serial.Serial(
                    port=self.port,
                    baudrate=self.baudrate,
                    timeout=self.timeout,
                    bytesize=serial.EIGHTBITS,
                    parity=serial.PARITY_NONE,
                    stopbits=serial.STOPBITS_ONE,
                )
            time.sleep(0.1)
            return True
        except serial.SerialException as e:
            print(f"Ошибка открытия соединения: {e}")
            return False

    def disconnect(self) -> None:
        if self.serial_conn and self.serial_conn.is_open:
            self.serial_conn.close()

    def send_ascii_command(self, command: str, add_crlf: Optional[bool] = True) -> bool:
        if not self.serial_conn or not self.serial_conn.is_open:
            print("Последовательное соединение не открыто")
            return False

        try:
            use_crlf = self.baudrate >= 460800 if add_crlf is None else bool(add_crlf)
            if use_crlf and not command.endswith("\r\n"):
                if not command.endswith("\r"):
                    command = command + "\r\n"
            raw = command.encode("ascii")
            self.serial_conn.write(raw)
            self.serial_conn.flush()
            if self._debug:
                print(f"[UM982 TX] {raw!r}")
            return True
        except Exception as e:
            print(f"Ошибка отправки ASCII команды: {e}")
            return False

    def send_binary_command(self, command_bytes: bytes) -> bool:
        if not self.serial_conn or not self.serial_conn.is_open:
            print("Последовательное соединение не открыто")
            return False

        try:
            self.serial_conn.write(command_bytes)
            self.serial_conn.flush()
            return True
        except Exception as e:
            print(f"Ошибка отправки бинарной команды: {e}")
            return False

    def read_response(self, timeout: Optional[float] = None, max_bytes: int = 4096) -> bytes:
        if not self.serial_conn or not self.serial_conn.is_open:
            return b""

        old_timeout = self.serial_conn.timeout
        if timeout is not None:
            self.serial_conn.timeout = timeout

        try:
            response = b""
            start_time = time.time()
            current_timeout = timeout if timeout is not None else self.timeout

            while True:
                if self.serial_conn.in_waiting > 0:
                    chunk = self.serial_conn.read(min(self.serial_conn.in_waiting, max_bytes - len(response)))
                    response += chunk

                    if len(response) >= max_bytes:
                        break

                if current_timeout and (time.time() - start_time) > current_timeout:
                    break

                if not response:
                    time.sleep(0.01)
                else:
                    time.sleep(0.02)
                    if self.serial_conn.in_waiting == 0:
                        break

            if self._debug and response:
                print(f"[UM982 RX] {len(response)} байт: {response[:300]!r}")
            return response
        finally:
            self.serial_conn.timeout = old_timeout

    def read_ascii_response(self, timeout: Optional[float] = None) -> str:
        response = self.read_response(timeout)
        return response.decode("ascii", errors="ignore")

    def read_lines(self, timeout: Optional[float] = None, max_lines: int = 100):
        if not self.serial_conn or not self.serial_conn.is_open:
            return []

        old_timeout = self.serial_conn.timeout
        if timeout is not None:
            self.serial_conn.timeout = min(timeout, 0.5)

        lines = []
        try:
            start_time = time.time()
            current_timeout = timeout if timeout is not None else self.timeout
            last_data_time = start_time
            no_data_timeout = 1.0

            while len(lines) < max_lines:
                if current_timeout and (time.time() - start_time) > current_timeout:
                    break

                if time.time() - last_data_time > no_data_timeout:
                    break

                line = self.serial_conn.readline()
                if line:
                    lines.append(line.decode("ascii", errors="ignore").strip())
                    last_data_time = time.time()
                    start_time = time.time()
                else:
                    if self.serial_conn.in_waiting == 0:
                        time.sleep(0.05)
                    else:
                        time.sleep(0.01)

            return lines
        finally:
            self.serial_conn.timeout = old_timeout

    # --- Высокоуровневый парсинг ---

    def parse_binary_response(self, data: bytes):
        """Совместимая с прежним API обёртка вокруг нового парсера."""
        parsed = parse_response(data)
        return parsed_response_to_legacy_dict(parsed)


