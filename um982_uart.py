import time
import struct
import re
import serial
import serial.tools.list_ports
from typing import Optional, List, Tuple, Callable, Union, Dict, Any
from enum import Enum

from um982.core import Um982Core
from um982.utils import parse_response, parsed_response_to_legacy_dict
from um982.config import query_config as _query_config
from um982.mask import query_mask as _query_mask
from um982.data_output import (
    _run_data_query as _do_run_data_query,  # для старых специальных логов
    query_obsvm as _do_query_obsvm,
    query_obsvh as _do_query_obsvh,
    query_obsvmcmp as _do_query_obsvmcmp,
    query_obsvbase as _do_query_obsvbase,
    query_baseinfo as _do_query_baseinfo,
    query_bestnav as _do_query_bestnav,
    query_adrnav as _do_query_adrnav,
    query_pppnav as _do_query_pppnav,
    query_sppnav as _do_query_sppnav,
    query_adrnavh as _do_query_adrnavh,
    query_sppnavh as _do_query_sppnavh,
    query_stadop as _do_query_stadop,
    query_adrdop as _do_query_adrdop,
    query_bestnavxyz as _do_query_bestnavxyz,
    query_pvtsln as _do_query_pvtsln,
    query_uniloglist as _do_query_uniloglist,
    query_gpsion as _do_query_gpsion,
    query_galion as _do_query_galion,
    query_gpsutc as _do_query_gpsutc,
    query_bd3utc as _do_query_bd3utc,
    query_bdsion as _do_query_bdsion,
    query_bd3ion as _do_query_bd3ion,
    query_agric as _do_query_agric,
    query_hwstatus as _do_query_hwstatus,
    query_agc as _do_query_agc,
    query_adrdoph as _do_query_adrdoph,
    query_mode as _do_query_mode,
    log as _do_log,
    unlog as _do_unlog,
)
from um982.data_output.common import find_unicore_sync
from um982.data_output.observation import (
    send_obsv_stream_command as _send_obsv_stream_command,
    extract_one_obsv_message as _extract_one_obsv_message,
)
from um982.data_output.version_rx import parse_version_rx


def _version_query_complete(data: bytes, is_binary: bool) -> bool:
    """Достаточно данных для парсинга VERSIONA/VERSIONB (без многосекундного ожидания)."""
    if b"#VERSIONA" in data:
        return True
    found = find_unicore_sync(data)
    if found:
        off, hdr = found
        if hdr.message_id == 37:
            ml = hdr.message_length or 336
            return len(data) >= off + ml
    return False


class CommandFormat(Enum):
    ASCII = "ascii"
    BINARY = "binary"


# Команды, на которые устройство не присылает подтверждение (считаем отправку успешной)
NO_ACK_COMMANDS = frozenset(("FRESET", "RESET", "ROVER", "HEADING2"))


class UM982UART:
    """
    Фасадный класс, сохраняющий старый API, но использующий под капотом `Um982Core`
    и новые утилиты парсинга.
    """

    def __init__(self, port: str, baudrate: int = 460800, timeout: float = 1.0, debug: Optional[bool] = None):
        self._core = Um982Core(port=port, baudrate=baudrate, timeout=timeout, debug=debug)

    # --- Базовые методы работы с соединением и I/O ---

    @property
    def _is_tcp(self) -> bool:
        """
        Совместимость со старым кодом/GUI: флаг, указывающий, что подключение по TCP, а не по последовательному порту.
        Реальное значение хранится внутри Um982Core.
        """
        return getattr(self._core, "_is_tcp", False)

    @property
    def port(self) -> str:
        return self._core.port

    @property
    def baudrate(self) -> int:
        return self._core.baudrate

    @property
    def timeout(self) -> float:
        return self._core.timeout

    @timeout.setter
    def timeout(self, value: float) -> None:
        self._core.timeout = value

    @property
    def serial_conn(self):
        # Для обратной совместимости — прямой доступ к низкоуровневому соединению.
        return self._core.serial_conn

    def connect(self) -> bool:
        return self._core.connect()

    def disconnect(self) -> None:
        self._core.disconnect()

    def send_ascii_command(self, command: str, add_crlf: Optional[bool] = True) -> bool:
        return self._core.send_ascii_command(command, add_crlf=add_crlf)

    def send_binary_command(self, command_bytes: bytes) -> bool:
        return self._core.send_binary_command(command_bytes)

    def read_response(self, timeout: Optional[float] = None, max_bytes: int = 4096) -> bytes:
        return self._core.read_response(timeout=timeout, max_bytes=max_bytes)

    def read_ascii_response(self, timeout: Optional[float] = None) -> str:
        return self._core.read_ascii_response(timeout=timeout)

    def read_lines(self, timeout: Optional[float] = None, max_lines: int = 100) -> List[str]:
        return self._core.read_lines(timeout=timeout, max_lines=max_lines)

    # --- Парсинг ---

    def parse_binary_response(self, data: bytes) -> dict:
        """Совместимая с прежним API обёртка вокруг нового парсера."""
        if not data:
            return {"error": "Пустой ответ"}
        parsed = parse_response(data)
        return parsed_response_to_legacy_dict(parsed)

    # --- Доменные команды: CONFIG / MASK ---

    def query_config(self, use_lines: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Тонкая обёртка над `um982.config.query_config`."""
        return _query_config(self._core, use_lines=use_lines, add_crlf=add_crlf)

    def query_mask(self, add_crlf: Optional[bool] = None) -> dict:
        """Тонкая обёртка над `um982.mask.query_mask`."""
        return _query_mask(self._core, add_crlf=add_crlf)

    def query_com_ports(self, add_crlf: Optional[bool] = None) -> dict:
        """
        Запрос информации о COM портах устройства

        Args:
            add_crlf: Если True, отправить команду с \r\n, если False без, если None автоопределение

        Returns:
            Словарь с информацией о портах:
            {
                'com1': {'baudrate': 115200, 'data_bits': 8, 'parity': 'N', 'stop_bits': 1, 'raw': '...'},
                'com2': {...},
                'com3': {...}
            }
        """
        config_result = self.query_config(use_lines=False, add_crlf=add_crlf)

        if 'error' in config_result:
            return config_result

        # Извлекаем CONFIG сообщения о портах
        messages = config_result.get('parsed', {}).get('messages', [])
        ports_info = {}

        for msg in messages:
            if isinstance(msg, dict):
                raw = msg.get('raw', '')
                msg_type = msg.get('type', '')

                # Ищем сообщения о COM портах
                if 'CONFIG,COM' in raw or 'CONFIG COM' in raw:
                    # Парсим сообщение типа: $CONFIG,COM1,CONFIG COM1 115200*23
                    # или: $CONFIG,COM1,CONFIG COM1 115200 8 N 1*XX
                    parts = raw.split(',')
                    if len(parts) >= 3:
                        # parts[1] = "COM1", parts[2] = "CONFIG COM1 115200*23" или "CONFIG COM1 115200 8 N 1*XX"
                        port_name = parts[1].strip()
                        config_part = parts[2].strip()

                        # Удаляем checksum если есть
                        if '*' in config_part:
                            config_part = config_part[:config_part.rfind('*')]

                        # Парсим параметры: "CONFIG COM1 115200" или "CONFIG COM1 115200 8 N 1"
                        config_fields = config_part.split()
                        if len(config_fields) >= 3:
                            port_key = port_name.lower()  # com1, com2, com3
                            port_info = {
                                'port': port_name,
                                'raw': raw
                            }

                            # Базовый формат: CONFIG COM1 115200
                            if len(config_fields) >= 3:
                                try:
                                    port_info['baudrate'] = int(config_fields[2])
                                except ValueError:
                                    port_info['baudrate'] = None

                            # Расширенный формат: CONFIG COM1 115200 8 N 1
                            if len(config_fields) >= 4:
                                try:
                                    port_info['data_bits'] = int(config_fields[3])
                                except ValueError:
                                    port_info['data_bits'] = 8  # значение по умолчанию

                            if len(config_fields) >= 5:
                                port_info['parity'] = config_fields[4].upper()
                            else:
                                port_info['parity'] = 'N'  # значение по умолчанию

                            if len(config_fields) >= 6:
                                try:
                                    port_info['stop_bits'] = int(config_fields[5])
                                except ValueError:
                                    port_info['stop_bits'] = 1  # значение по умолчанию
                            else:
                                port_info['stop_bits'] = 1

                            ports_info[port_key] = port_info

        result = {
            'ports': ports_info,
            'found_ports': list(ports_info.keys())
        }

        # Добавляем исходный ответ для отладки
        result['source'] = config_result

        return result

    def query_version(self, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """
        Query device version information (VERSION command)

        Args:
            binary: If True, send VERSIONB (binary), if False send VERSIONA (ASCII)
            add_crlf: If True, send command with \r\n, if False without, if None auto-detect

        Returns:
            Dictionary with parsed version information
        """
        command = "VERSIONB" if binary else "VERSIONA"
        return _do_run_data_query(
            self._core,
            command=command,
            parse_func=lambda d, b: self._parse_version_message(d, b),
            binary=binary,
            add_crlf=add_crlf,
            wait_time=0.0,
            read_attempts=48,
            max_wait=5.0,
            check_complete=_version_query_complete,
            result_key="version",
        )

    def query_obsvm(self, port: Optional[str] = None, rate: Union[int, float] = 1, binary: bool = False,
                    add_crlf: Optional[bool] = None) -> dict:
        """Query OBSVM (Observation of the Master Antenna) data. Порт None/пусто — команда без COM (текущий порт сессии)."""
        return _do_query_obsvm(self._core, port=port, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_obsvh(self, port: Optional[str] = None, rate: Union[int, float] = 1, binary: bool = False,
                    add_crlf: Optional[bool] = None) -> dict:
        """Query OBSVH (Observation of the Slave Antenna) data. Порт None/пусто — команда без COM."""
        return _do_query_obsvh(self._core, port=port, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_obsvmcmp(self, port: Optional[str] = None, rate: Union[int, float] = 1, binary: bool = False,
                       add_crlf: Optional[bool] = None) -> dict:
        """Query OBSVMCMP (Compressed Observation of the Master Antenna) data. Порт None/пусто — команда без COM."""
        return _do_query_obsvmcmp(self._core, port=port, rate=rate, binary=binary, add_crlf=add_crlf)

    def send_obsv_stream_command(
            self,
            stream_type: str,
            port: Optional[str] = None,
            rate: Union[int, float] = 1,
            binary: bool = False,
            add_crlf: Optional[bool] = None,
    ) -> bool:
        """Отправить команду вывода OBSVM/OBSVH/OBSVMCMP один раз. Дальше только читать с порта."""
        return _send_obsv_stream_command(
            self._core, stream_type=stream_type, port=port, rate=rate, binary=binary, add_crlf=add_crlf
        )

    def query_obsvbase(self, port: Optional[str] = None, trigger: str = "ONCHANGED", binary: bool = False,
                       add_crlf: Optional[bool] = None) -> dict:
        """Query OBSVBASE (Observation of the Base Station) data. Порт None/пусто — команда без COM."""
        return _do_query_obsvbase(self._core, port=port, trigger=trigger, binary=binary, add_crlf=add_crlf)

    def query_baseinfo(self, rate: int = 1, trigger: Optional[str] = None, binary: bool = False,
                       add_crlf: Optional[bool] = None) -> dict:
        """Query BASEINFO (Base Station Information) data."""
        return _do_query_baseinfo(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)

    def query_gpsion(
            self,
            rate: Optional[Union[int, float]] = None,
            trigger: Optional[str] = None,
            binary: bool = False,
            add_crlf: Optional[bool] = None,
    ) -> dict:
        """Фасад для запроса GPSION через новый модуль данных. rate=None — без периода на проводе (одиночный запрос)."""
        return _do_query_gpsion(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)

    def query_galion(
            self,
            rate: Optional[Union[int, float]] = None,
            trigger: Optional[str] = None,
            binary: bool = False,
            add_crlf: Optional[bool] = None,
    ) -> dict:
        """Фасад для запроса GALION. rate=None — без периода на проводе."""
        return _do_query_galion(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)

    def query_gpsutc(self, rate: int = 1, trigger: Optional[str] = None, binary: bool = False,
                     add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса GPSUTC через новый модуль данных."""
        return _do_query_gpsutc(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)

    def query_bd3utc(self, rate: int = 1, trigger: Optional[str] = None, binary: bool = False,
                     add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса BD3UTC через новый модуль данных."""
        return _do_query_bd3utc(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)

    def query_bdsion(
            self,
            rate: Optional[Union[int, float]] = None,
            trigger: Optional[str] = None,
            binary: bool = False,
            add_crlf: Optional[bool] = None,
    ) -> dict:
        """Фасад для запроса BDSION. rate=None — без периода на проводе."""
        return _do_query_bdsion(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)

    def query_bd3ion(
            self,
            rate: Optional[Union[int, float]] = None,
            trigger: Optional[str] = None,
            binary: bool = False,
            add_crlf: Optional[bool] = None,
    ) -> dict:
        """Фасад для запроса BD3ION (§7.3.9). rate=None — BD3IONA/BD3IONB без периода; иначе «… 1» и т.д.; ONCHANGED отдельно."""
        return _do_query_bd3ion(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)

    def query_agric(self, port: Optional[str] = None, rate: int = 1, binary: bool = False,
                    add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса AGRIC через новый модуль данных."""
        return _do_query_agric(self._core, port=port, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_pvtsln(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса PVTSLN через новый модуль данных."""
        return _do_query_pvtsln(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_uniloglist(self, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса UNILOGLIST через новый модуль данных."""
        return _do_query_uniloglist(self._core, add_crlf=add_crlf)

    def unlog(self, port: Optional[str] = None, message: Optional[str] = None,
              add_crlf: Optional[bool] = None) -> dict:
        """Остановить вывод сообщений (UNLOG) через новый модуль данных."""
        return _do_unlog(self._core, port=port, message=message, add_crlf=add_crlf)

    def log(self, message: str, port: Optional[str] = None, rate: int = 1,
            add_crlf: Optional[bool] = None) -> dict:
        """Включить вывод сообщения (LOG) через новый модуль данных."""
        return _do_log(self._core, message=message, port=port, rate=rate, add_crlf=add_crlf)

    def freset(self, add_crlf: Optional[bool] = None) -> dict:
        """Полный сброс и очистка NVM (FRESET)."""
        return self._send_config_command("FRESET", "FRESET", add_crlf=add_crlf)

    def reset(self, parameters: Optional[Union[str, List[str]]] = None,
              add_crlf: Optional[bool] = None) -> dict:
        """Перезапуск приёмника с опциональной очисткой данных (RESET)."""
        valid_params = frozenset(("EPHEM", "ALMANAC", "IONUTC", "POSITION", "XOPARAM", "CLOCKDRIFT", "ALL"))
        parts = ["RESET"]
        if parameters is not None:
            if isinstance(parameters, str):
                parameters = [parameters]
            for p in parameters:
                p_upper = p.strip().upper() if p else ""
                if not p_upper:
                    continue
                if p_upper not in valid_params:
                    return {
                        "error": f"Недопустимый параметр RESET: {p!r}. Допустимы: {', '.join(sorted(valid_params))}"}
                # CLOCKDRIFT и XOPARAM — синонимы, в команде обычно XOPARAM
                parts.append("XOPARAM" if p_upper == "CLOCKDRIFT" else p_upper)
        command = " ".join(parts)
        return self._send_config_command(command, "RESET", add_crlf=add_crlf)

    def saveconfig(self, add_crlf: Optional[bool] = None) -> dict:
        """Сохранить текущую конфигурацию в NVM (SAVECONFIG)."""
        return self._send_config_command("SAVECONFIG", "SAVECONFIG", add_crlf=add_crlf)

    def restore_output(self, port: Optional[str] = None, add_crlf: Optional[bool] = None) -> dict:
        """Включить вывод сообщений после UNLOG (отправка BESTNAVA 1)."""
        cmd = f"BESTNAVA {port} 1".strip() if port else "BESTNAVA 1"
        if add_crlf is None:
            add_crlf = (self.baudrate >= 460800)
        if self._core.serial_conn and self._core.serial_conn.in_waiting > 0:
            self._core.serial_conn.reset_input_buffer()
        if not self.send_ascii_command(cmd, add_crlf=add_crlf):
            return {"error": f"Не удалось отправить команду {cmd}"}
        time.sleep(0.5)
        response = b''
        for _ in range(6):
            time.sleep(0.2)
            chunk = self.read_response(timeout=1.0)
            if chunk:
                response += chunk
            elif response:
                break
        return {
            "command": cmd,
            "success": True,
            "response_received": len(response) > 0,
            "note": "Вывод BESTNAV включён; при отсутствии ответа проверьте скорость (например 115200 после FRESET)."
        }

    def query_bestnav(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса BESTNAV через новый модуль данных."""
        return _do_query_bestnav(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_adrnav(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса ADRNAV через новый модуль данных."""
        return _do_query_adrnav(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_adrnavh(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса ADRNAVH через новый модуль данных."""
        return _do_query_adrnavh(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_pppnav(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса PPPNAV через новый модуль данных."""
        return _do_query_pppnav(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_sppnav(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса SPPNAV через новый модуль данных."""
        return _do_query_sppnav(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_sppnavh(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса SPPNAVH через новый модуль данных."""
        return _do_query_sppnavh(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_stadop(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса STADOP через новый модуль данных."""
        return _do_query_stadop(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_adrdop(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса ADRDOP (раздел 7.3.36, id 953) через новый модуль данных."""
        return _do_query_adrdop(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_bestnavxyz(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса BESTNAVXYZ через новый модуль данных."""
        return _do_query_bestnavxyz(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_adrdoph(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса DOP по ADRNAVH (лог 2121) через новый модуль данных."""
        return _do_query_adrdoph(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    # (Подробные реализации STADOP, ADRDOP и BESTNAVXYZ вынесены в модуль `um982.data_output`;
    #  здесь остаются только фасадные методы выше.)

    def query_hwstatus(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса HWSTATUS через новый модуль данных."""
        return _do_query_hwstatus(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_agc(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса AGC через новый модуль данных."""
        return _do_query_agc(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_mode(self, add_crlf: Optional[bool] = None) -> dict:
        """
        Query the receiver's operating mode (MODE command without parameters).

        Args:
            add_crlf: If True, send command with \r\n, if False without, if None auto-detect

        Returns:
            Dictionary with parsed MODE data
        """
        # MODE команда поддерживает только ASCII формат; делегируем в модуль данных.
        return _do_query_mode(self._core, mode_arg="", add_crlf=add_crlf)

    def set_mode_base(self,
                      station_id: Optional[int] = None,
                      param1: Optional[float] = None,
                      param2: Optional[float] = None,
                      param3: Optional[float] = None,
                      lat: Optional[float] = None,
                      lon: Optional[float] = None,
                      hgt: Optional[float] = None,
                      x: Optional[float] = None,
                      y: Optional[float] = None,
                      z: Optional[float] = None,
                      coordinate_system: Optional[str] = None,
                      add_crlf: Optional[bool] = None) -> dict:
        """Set receiver to BASE mode (default, ID-only or fixed coordinates in geodetic/ECEF).

        Для param1/param2/param3 без имён lat/lon/hgt или x/y/z укажите coordinate_system:
        \"GEODETIC\" или \"ECEF\", иначе тип выводится по диапазону param1 (см. мануал §3.2).
        """
        # Определяем, какие координаты использовать
        use_geodetic = False
        use_ecef = False

        if lat is not None and lon is not None and hgt is not None:
            use_geodetic = True
            coord1, coord2, coord3 = lat, lon, hgt
        elif x is not None and y is not None and z is not None:
            use_ecef = True
            coord1, coord2, coord3 = x, y, z
        elif param1 is not None and param2 is not None and param3 is not None:
            cs = (coordinate_system or "").strip().upper()
            if cs == "GEODETIC":
                use_geodetic = True
            elif cs == "ECEF":
                use_ecef = True
            elif cs:
                return {
                    "error": (
                        f"Недопустимый coordinate_system: {coordinate_system!r}. "
                        "Для param1..3 укажите \"GEODETIC\", \"ECEF\" или оставьте пусто (авто по диапазону param1)."
                    ),
                }
            else:
                # Автоматическое определение (как в мануале: широта в [-90,90] — геодезия, иначе ECEF)
                if -90 <= param1 <= 90:
                    use_geodetic = True
                else:
                    use_ecef = True
            coord1, coord2, coord3 = param1, param2, param3
        elif lat is not None or lon is not None or hgt is not None:
            return {"error": "Must provide all three coordinates (lat, lon, hgt) for Geodetic system"}
        elif x is not None or y is not None or z is not None:
            return {"error": "Must provide all three coordinates (x, y, z) for ECEF system"}
        elif param1 is not None or param2 is not None or param3 is not None:
            return {"error": "Must provide all three parameters (param1, param2, param3)"}

        # Валидация координат
        if use_geodetic:
            if not (-90 <= coord1 <= 90):
                return {"error": f"Invalid latitude: {coord1}. Must be between -90 and 90 degrees"}
            if not (-180 <= coord2 <= 180):
                return {"error": f"Invalid longitude: {coord2}. Must be between -180 and 180 degrees"}
            if not (-30000 <= coord3 <= 30000):
                return {"error": f"Invalid height: {coord3}. Must be between -30000 and 30000 meters"}
        elif use_ecef:
            # Для ECEF координаты могут быть любыми, но обычно большие значения
            pass

        # Валидация station_id
        if station_id is not None:
            if not (0 <= station_id <= 4095):
                return {"error": f"Invalid station ID: {station_id}. Must be between 0 and 4095"}

        # Построение команды
        if use_geodetic or use_ecef:
            if station_id is not None:
                command = f"MODE BASE {station_id} {coord1} {coord2} {coord3}"
            else:
                command = f"MODE BASE {coord1} {coord2} {coord3}"
        else:
            # Без координат
            if station_id is not None:
                command = f"MODE BASE {station_id}"
            else:
                command = "MODE BASE"

        return self._send_config_command(command, add_crlf=add_crlf)

    def set_mode_rover(
            self,
            rover_param1: Optional[str] = None,
            rover_param2: Optional[str] = None,
            *,
            add_crlf: Optional[bool] = None,
    ) -> dict:
        """
        Режим ROVER (§3.6): MODE ROVER [сценарий] [подрежим].

        rover_param1 / rover_param2 — имена аргументов в API; в интерфейсе — «Сценарий» и «Подрежим».
        Без значений — MODE ROVER (режим по умолчанию для модели). Примеры: MODE ROVER UAV, MODE ROVER SURVEY MOW.
        """
        p1 = (rover_param1 or "").strip().upper()
        raw2 = (rover_param2 or "").strip().upper()
        if not p1 and raw2:
            return {"error": "Сначала выберите сценарий, если задан подрежим."}
        valid1 = frozenset({"UAV", "SURVEY", "AUTOMOTIVE"})
        if p1 and p1 not in valid1:
            return {
                "error": f"Недопустимый сценарий: {p1!r}. Допустимо: {', '.join(sorted(valid1))} или пусто.",
            }
        if p1 == "UAV" and raw2 not in ("", "DEFAULT", "FORMATION"):
            return {"error": "Для сценария UAV подрежим: пусто, DEFAULT или FORMATION."}
        if p1 == "SURVEY" and raw2 not in ("", "DEFAULT", "MOW"):
            return {"error": "Для сценария SURVEY подрежим: пусто, DEFAULT или MOW."}
        if p1 == "AUTOMOTIVE" and raw2 not in ("", "DEFAULT"):
            return {"error": "Для сценария AUTOMOTIVE подрежим: только пусто или DEFAULT."}
        p2 = "" if raw2 in ("", "DEFAULT") else raw2
        if not p1:
            command = "MODE ROVER"
        elif not p2:
            command = f"MODE ROVER {p1}"
        else:
            command = f"MODE ROVER {p1} {p2}"
        return self._send_config_command(command, "ROVER", add_crlf=add_crlf)

    def set_mode_base_time(self,
                           station_id: Optional[int] = None,
                           time: int = 60,
                           distance: Optional[float] = None,
                           add_crlf: Optional[bool] = None) -> dict:
        """Установить режим BASE TIME (самооптимизирующаяся база)."""
        # Валидация station_id
        if station_id is not None:
            if not (0 <= station_id <= 4095):
                return {"error": f"Invalid station ID: {station_id}. Must be between 0 and 4095"}

        # Валидация time
        if time < 0:
            return {"error": f"Invalid time: {time}. Cannot be negative"}

        # Валидация distance
        if distance is not None:
            if not (0 <= distance <= 10):
                return {"error": f"Invalid distance: {distance}. Must be between 0 and 10 meters"}

        # Построение команды
        if station_id is not None:
            if distance is not None:
                command = f"MODE BASE {station_id} TIME {time} {distance}"
            else:
                command = f"MODE BASE {station_id} TIME {time}"
        else:
            if distance is not None:
                command = f"MODE BASE TIME {time} {distance}"
            else:
                command = f"MODE BASE TIME {time}"

        return self._send_config_command(command, add_crlf=add_crlf)

    def set_mode_heading2(
            self,
            heading2_variant: Optional[str] = None,
            *,
            add_crlf: Optional[bool] = None,
    ) -> dict:
        """
        Режим HEADING2 (§3.7): MODE HEADING2 [вариант].

        heading2_variant — в API; в GUI подпись «Вариант heading». Пусто — как в мануале без суффикса
        (типично эквивалентно фиксированной базе FIXLENGTH). Иначе: FIXLENGTH, VARIABLELENGTH, STATIC,
        LOWDYNAMIC, TRACTOR.
        """
        v = (heading2_variant or "").strip().upper()
        valid = frozenset(
            {"", "FIXLENGTH", "VARIABLELENGTH", "STATIC", "LOWDYNAMIC", "TRACTOR"}
        )
        if v and v not in valid:
            return {
                "error": (
                    f"Недопустимый вариант HEADING2: {v!r}. "
                    f"Допустимо: пусто или {', '.join(sorted(x for x in valid if x))}."
                ),
            }
        command = "MODE HEADING2" if not v else f"MODE HEADING2 {v}"
        return self._send_config_command(command, "HEADING2", add_crlf=add_crlf)

    def _parse_version_message(self, data: bytes, binary: bool = False) -> Optional[dict]:
        return parse_version_rx(data, binary)

    def _send_config_command(self, command: str, command_name: str = "",
                             add_crlf: Optional[bool] = None) -> dict:
        """
        Универсальный метод для отправки CONFIG команд

        Args:
            command: Полная команда (например, "CONFIG COM1 115200")
            command_name: Имя команды для поиска подтверждения (например, "COM1", "PPS")
            add_crlf: Если True, отправить команду с \r\n, если False без, если None автоопределение

        Returns:
            Словарь с информацией об ответе
        """
        # Автоопределение CRLF на основе скорости передачи
        if add_crlf is None:
            add_crlf = (self.baudrate >= 460800)

        # Очистка входного буфера
        if self._core.serial_conn and self._core.serial_conn.in_waiting > 0:
            self._core.serial_conn.reset_input_buffer()

        # Отправка команды
        if not self.send_ascii_command(command, add_crlf=add_crlf):
            return {"error": f"Не удалось отправить команду {command}"}

        # Ожидание ответа (устройству нужно время на обработку, особенно для UNLOG и др.)
        time.sleep(0.5)

        # Чтение ответа (увеличены попытки и таймаут для медленного ответа)
        response = b''
        for attempt in range(8):
            time.sleep(0.15)
            chunk = self.read_response(timeout=1.2)
            if chunk:
                response += chunk
            elif response:
                break

        if not response:
            # Команды перезагрузки: устройство не успевает ответить — считаем отправку успешной
            if command_name and command_name.upper() in NO_ACK_COMMANDS:
                print("Команда отправлена")
                return {
                    "command": command,
                    "response": {},
                    "confirmation": None,
                    "success": True,
                    "sent_no_response": True,
                }
            return {"error": "Ответ не получен"}

        # Парсинг ответа
        parsed = self.parse_binary_response(response)

        # Проверка сообщения подтверждения
        messages = parsed.get('parsed', {}).get('messages', [])
        confirmation = None

        # Подтверждение ищем в ASCII-строках ответа ($COMMAND, $CONFIG, … — формат Unicore ASCII)
        for msg in messages:
            raw = msg.get('raw', '')
            raw_upper = raw.upper()
            # Поиск формата ответа команды: $command,CONFIG...,response: OK*XX
            if 'COMMAND' in raw_upper and 'CONFIG' in raw_upper and 'OK' in raw_upper:
                # Проверка правильности команды (если указано имя команды)
                if not command_name or command_name.upper() in raw_upper:
                    confirmation = raw
                    break
            # Поиск формата ответа для команд MASK/UNMASK: $command,MASK...,response: OK*XX
            elif 'COMMAND' in raw_upper and 'OK' in raw_upper:
                # Проверяем, что это подтверждение для нашей команды
                if command_name and command_name.upper() in raw_upper:
                    confirmation = raw
                    break
                # Или если команда содержит MASK или UNMASK
                elif 'MASK' in raw_upper or 'UNMASK' in raw_upper:
                    confirmation = raw
                    break
            # Поиск формата ответа CONFIG (только для команд, которые возвращают подтверждение)
            # Примечание: команда настройки CONFIG COM не возвращает ответ в формате $CONFIG,COM1,...
            # Этот формат используется только для запроса конфигурации (query_config)
            elif 'CONFIG' in raw_upper:
                # Для команды COM (настройка) не ищем подтверждение в формате $CONFIG,COM1,...
                # так как команда настройки не возвращает такой ответ
                if command_name and command_name.upper() == 'COM':
                    # Команда настройки COM не возвращает подтверждение в этом формате
                    pass
                elif not command_name or command_name.upper() in raw_upper:
                    confirmation = raw
                    break

        # Также проверяем в сыром ASCII тексте, если не найдено в распарсенных сообщениях
        message_sent_style = False  # ответ формата "сообщение отправлено" / "message sent"
        if not confirmation:
            try:
                ascii_text = response.decode('ascii', errors='ignore')
                # Look for confirmation patterns
                patterns = [
                    rf'\$command,CONFIG[^\r\n]*response: OK[^\r\n]*',
                    rf'\$command,{re.escape(command_name.upper())}[^\r\n]*response: OK[^\r\n]*',  # Для MASK/UNMASK
                ]

                # Для команды настройки CONFIG COM не ищем подтверждение в формате $CONFIG,COM1,...
                # так как команда настройки не возвращает такой ответ (в документации нет примера)
                # Формат $CONFIG,COM1,... используется только для запроса конфигурации (query_config)
                if command_name and command_name.upper() != 'COM':
                    patterns.append(rf'\$CONFIG[^\r\n]*')
                for pattern in patterns:
                    match = re.search(pattern, ascii_text, re.IGNORECASE)
                    if match:
                        confirmation = match.group(0).strip()
                        break
                # Ответ формата "message sent" / "сообщение отправлено" — считаем успешной отправкой
                if not confirmation:
                    ascii_upper = ascii_text.upper()
                    for sent_marker in ('SENT', 'ОТПРАВЛЕНО', 'MESSAGE SENT', 'COMMAND SENT'):
                        if sent_marker in ascii_upper:
                            for line in ascii_text.splitlines():
                                if sent_marker in line.upper():
                                    confirmation = line.strip()
                                    message_sent_style = True
                                    break
                            if message_sent_style:
                                break
            except Exception:
                pass

        result = {
            "command": command,
            "response": parsed,
            "confirmation": confirmation,
            "success": confirmation is not None
        }
        if message_sent_style:
            result["sent_no_response"] = False  # ответ был, но в формате "отправлено"
            result["message_sent_style"] = True  # для GUI: показать уведомление "Команда отправлена"
            print("Команда отправлена")

        return result

    def send_ascii_configuration_line(self, command: str, add_crlf: Optional[bool] = None) -> dict:
        """
        Произвольная ASCII-строка настройки (CONFIG …, MASK …, LOG …, UNLOG …).
        Для импорта профиля приёмника из JSON (см. um982.receiver_profile).
        """
        parts = str(command).strip().split()
        token = parts[0].upper() if parts else "CMD"
        return self._send_config_command(str(command).strip(), token, add_crlf=add_crlf)

    def config_command(self, command_type: str, **params) -> dict:
        """
        Универсальный метод для отправки CONFIG команд через систему регистрации

        Args:
            command_type: Тип команды ('COM', 'PPS', 'DGPS', и т.д.)
            **params: Параметры команды

        Returns:
            Dictionary with response information
        """
        try:
            from um982_commands import get_command_definition, get_command_names

            cmd_def = get_command_definition(command_type)
            if not cmd_def:
                available = get_command_names()
                return {"error": f"Unknown command type: {command_type}. Available: {available}"}

            # Валидация параметров
            if cmd_def.validator:
                error = cmd_def.validator(params)
                if error:
                    return {"error": error}

            # Построение команды
            command, warning = cmd_def.command_builder(params)

            # Отправка команды
            result = self._send_config_command(command, command_type)

            if warning:
                result["warning"] = warning

            return result

        except ImportError:
            return {"error": "Command system not available. Install um982_commands module"}
        except Exception as e:
            return {"error": f"Error executing command: {str(e)}"}

    def config_com_port(self, port: str, baudrate: int, data_bits: int = 8,
                        parity: str = 'N', stop_bits: int = 1,
                        add_crlf: Optional[bool] = None) -> dict:
        """
        Configure serial port (COM1, COM2, or COM3)

        Args:
            port: Port name ('COM1', 'COM2', or 'COM3')
            baudrate: Baud rate (9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600)
            data_bits: Data bits (currently only 8 is supported)
            parity: Parity check ('N', 'E', or 'O', currently only 'N' is supported)
            stop_bits: Stop bits (1 or 2, currently only 1 is supported)
            add_crlf: If True, send command with \r\n, if False without, if None auto-detect

        Returns:
            Dictionary with response information
        """
        # Используем универсальную систему команд
        return self.config_command('COM',
                                   port=port,
                                   baudrate=baudrate,
                                   data_bits=data_bits,
                                   parity=parity,
                                   stop_bits=stop_bits
                                   )

    def config_pps(self, enable: str = 'ENABLE', timeref: Optional[str] = None,
                   polarity: Optional[str] = None, width: Optional[int] = None,
                   period: Optional[int] = None, rf_delay: Optional[int] = None,
                   user_delay: Optional[int] = None, add_crlf: Optional[bool] = None) -> dict:
        """
        Configure PPS (Pulse Per Second) signal parameters

        Args:
            enable: PPS mode - 'DISABLE', 'ENABLE' (default), 'ENABLE2', or 'ENABLE3'
            timeref: Time reference for ENABLE/ENABLE2/ENABLE3 - 'GPS', 'BDS', 'GAL', or 'GLO'
            polarity: Pulse polarity - 'POSITIVE' or 'NEGATIVE'
            width: Pulse width in microseconds (must be smaller than period)
            period: PPS output period in milliseconds (valid: 50, 100, 200, ..., 20000)
            rf_delay: RF delay in nanoseconds (range: -32768 to 32767)
            user_delay: User-set delay in nanoseconds (range: -32768 to 32767)
            add_crlf: If True, send command with \r\n, if False without, if None auto-detect

        Returns:
            Dictionary with response information
        """
        # Используем универсальную систему команд
        params = {'enable': enable}

        # Добавляем параметры только если не DISABLE
        if enable.upper() != 'DISABLE':
            if timeref is not None:
                params['timeref'] = timeref
            if polarity is not None:
                params['polarity'] = polarity
            if width is not None:
                params['width'] = width
            if period is not None:
                params['period'] = period
            if rf_delay is not None:
                params['rf_delay'] = rf_delay
            if user_delay is not None:
                params['user_delay'] = user_delay

        return self.config_command('PPS', **params)

    def config_dgps(self, timeout: int, add_crlf: Optional[bool] = None) -> dict:
        """
        Configure DGPS (Differential GPS) timeout

        Args:
            timeout: Maximum age of differential data in seconds
                    - 0: Disable DGPS positioning
                    - 1-1800: Maximum age of differential data (default = 300)
            add_crlf: If True, send command with \r\n, if False without, if None auto-detect

        Returns:
            Dictionary with response information
        """
        # Используем универсальную систему команд
        return self.config_command('DGPS', timeout=timeout)

    def config_rtk(self, subcommand: str, timeout: Optional[int] = None,
                   param1: Optional[int] = None, param2: Optional[int] = None,
                   add_crlf: Optional[bool] = None) -> dict:
        """
        Configure RTK (Real-Time Kinematic) parameters

        Args:
            subcommand: RTK subcommand - 'TIMEOUT', 'RELIABILITY', 'USER_DEFAULTS', 'RESET', or 'DISABLE'
            timeout: For TIMEOUT subcommand - 0 to disable, 1-1800 seconds for max age (max 600 for some versions)
            param1: For RELIABILITY subcommand - RTK reliability threshold (1-4)
            param2: For RELIABILITY subcommand - ADR reliability threshold (1 or 4, optional)
            add_crlf: If True, send command with \r\n, if False without, if None auto-detect

        Returns:
            Dictionary with response information

        Примеры:
            # Отключение RTK
            device.config_rtk('TIMEOUT', timeout=0)

            # Установка таймаута RTK на 600 секунд
            device.config_rtk('TIMEOUT', timeout=600)

            # Установка надежности RTK (один параметр)
            device.config_rtk('RELIABILITY', param1=3)

            # Установка надежности RTK (два параметра)
            device.config_rtk('RELIABILITY', param1=3, param2=1)

            # Сброс решения RTK
            device.config_rtk('RESET')

            # Отключение расчета RTK
            device.config_rtk('DISABLE')
        """
        # Используем универсальную систему команд
        params = {'subcommand': subcommand}

        if subcommand.upper() == 'TIMEOUT':
            if timeout is not None:
                params['timeout'] = timeout
        elif subcommand.upper() == 'RELIABILITY':
            if param1 is not None:
                params['param1'] = param1
            if param2 is not None:
                params['param2'] = param2

        return self.config_command('RTK', **params)

    def config_standalone(self, subcommand: str = 'ENABLE',
                          latitude: Optional[float] = None,
                          longitude: Optional[float] = None,
                          altitude: Optional[float] = None,
                          time: Optional[int] = None,
                          add_crlf: Optional[bool] = None) -> dict:
        """
        Configure STANDALONE mode

        Args:
            subcommand: 'ENABLE' (default) or 'DISABLE'
            latitude: Latitude in degrees (-90 to 90) for coordinate mode
            longitude: Longitude in degrees (-180 to 180) for coordinate mode
            altitude: Altitude in meters (-30000 to 18000) for coordinate mode
            time: Time parameter in seconds (3-100) for time mode
            add_crlf: If True, send command with \r\n, if False without, if None auto-detect

        Returns:
            Dictionary with response information

        Examples:
            # Enable with default settings (auto-calculated position, 100s timeout)
            device.config_standalone('ENABLE')

            # Enable with coordinates
            device.config_standalone('ENABLE', latitude=55.7558, longitude=37.6173, altitude=150.0)

            # Enable with time parameter
            device.config_standalone('ENABLE', time=100)

            # Disable
            device.config_standalone('DISABLE')
        """
        # Используем универсальную систему команд
        params = {'subcommand': subcommand}

        if latitude is not None:
            params['latitude'] = latitude
        if longitude is not None:
            params['longitude'] = longitude
        if altitude is not None:
            params['altitude'] = altitude
        if time is not None:
            params['time'] = time

        return self.config_command('STANDALONE', **params)

    def config_heading(self, subcommand: str,
                       param1: Optional[float] = None,
                       param2: Optional[float] = None,
                       heading_offset: Optional[float] = None,
                       pitch_offset: Optional[float] = None,
                       add_crlf: Optional[bool] = None) -> dict:
        """
        Configure HEADING parameters

        Args:
            subcommand: HEADING subcommand - 'FIXLENGTH', 'VARIABLELENGTH', 'STATIC',
                       'LOWDYNAMIC', 'TRACTOR', 'LENGTH', 'RELIABILITY', or 'OFFSET'
            param1: For LENGTH - baseline length in centimeters
                   For RELIABILITY - reliability threshold (1-4)
            param2: For LENGTH - error tolerance in centimeters (optional)
            heading_offset: For OFFSET - heading offset correction in degrees (-180.0 to 180.0)
            pitch_offset: For OFFSET - pitch offset correction in degrees (-90.0 to 90.0)
            add_crlf: If True, send command with \r\n, if False without, if None auto-detect

        Returns:
            Dictionary with response information

        Examples:
            # Mode commands (no parameters)
            device.config_heading('FIXLENGTH')
            device.config_heading('VARIABLELENGTH')
            device.config_heading('STATIC')
            device.config_heading('LOWDYNAMIC')
            device.config_heading('TRACTOR')

            # LENGTH command
            device.config_heading('LENGTH')  # Use defaults
            device.config_heading('LENGTH', param1=20)  # 20 cm baseline
            device.config_heading('LENGTH', param1=20, param2=3)  # 20 cm baseline, 3 cm tolerance

            # RELIABILITY command
            device.config_heading('RELIABILITY', param1=3)  # Relatively high reliability

            # OFFSET command
            device.config_heading('OFFSET', heading_offset=90.0, pitch_offset=45.0)
        """
        # Используем универсальную систему команд
        params = {'subcommand': subcommand}

        if param1 is not None:
            params['param1'] = param1
        if param2 is not None:
            params['param2'] = param2
        if heading_offset is not None:
            params['heading_offset'] = heading_offset
        if pitch_offset is not None:
            params['pitch_offset'] = pitch_offset

        return self.config_command('HEADING', **params)

    def config_sbas(self, subcommand: str = 'ENABLE',
                    mode: Optional[str] = None,
                    timeout: Optional[int] = None,
                    add_crlf: Optional[bool] = None) -> dict:
        """
        Configure SBAS (Satellite-Based Augmentation System)

        Args:
            subcommand: SBAS subcommand - 'ENABLE' (default), 'DISABLE', or 'TIMEOUT'
            mode: For ENABLE - SBAS mode: 'AUTO' (default), 'WAAS', 'GAGAN', 'MSAS', 'EGNOS', 'SDCM', or 'BDS'
            timeout: For TIMEOUT - SBAS timeout in seconds (120-1800, default=1200)
            add_crlf: If True, send command with \r\n, if False without, if None auto-detect

        Returns:
            Dictionary with response information

        Examples:
            # Enable SBAS with AUTO mode (default)
            device.config_sbas('ENABLE')

            # Enable SBAS with specific mode
            device.config_sbas('ENABLE', mode='WAAS')
            device.config_sbas('ENABLE', mode='EGNOS')

            # Disable SBAS
            device.config_sbas('DISABLE')

            # Set SBAS timeout
            device.config_sbas('TIMEOUT', timeout=600)
        """
        # Используем универсальную систему команд
        params = {'subcommand': subcommand}

        if subcommand.upper() == 'ENABLE':
            if mode is not None:
                params['mode'] = mode
        elif subcommand.upper() == 'TIMEOUT':
            if timeout is not None:
                params['timeout'] = timeout

        return self.config_command('SBAS', **params)

    def config_undulation(self,
                          mode: str = 'AUTO',
                          separation: Optional[float] = None,
                          add_crlf: Optional[bool] = None) -> dict:
        """
        Configure geoid undulation for the receiver (CONFIG UNDULATION)

        Args:
            mode: 'AUTO' (default) to use built-in geoid undulation grid
            separation: User-defined geoid undulation value in meters
                        Range: -1000.0000 to +1000.0000 (4 decimal places)
            add_crlf: Reserved for future use (handled by low-level sender)

        Notes:
            When configuring the receiver to operate in Base Station mode,
            UNDULATION should be configured first.

        Examples:
            # Use built-in geoid undulation grid
            device.config_undulation()
            device.config_undulation(mode='AUTO')

            # Use fixed geoid separation value (meters)
            device.config_undulation(separation=9.7)
        """
        params: Dict[str, Any] = {}

        # Если задано значение separation, используем его независимо от режима
        if separation is not None:
            params['separation'] = separation
        else:
            params['mode'] = mode

        return self.config_command('UNDULATION', **params)

    def config_ppp(self,
                   subcommand: str,
                   service: Optional[str] = None,
                   datum: Optional[str] = None,
                   hor_std: Optional[int] = None,
                   ver_std: Optional[int] = None,
                   add_crlf: Optional[bool] = None) -> dict:
        """
        Configure PPP (Precise Point Positioning) function (CONFIG PPP)

        Args:
            subcommand:
                - 'ENABLE'   – enable PPP service (service: 'B2B-PPP' or 'SSR-RX')
                - 'DATUM'    – set PPP coordinate system ('WGS84' or 'PPPORIGINAL')
                - 'CONVERGE' – set convergence thresholds (hor_std, ver_std in cm)
                - 'DISABLE'  – disable PPP
            service: For ENABLE – PPP service: 'B2B-PPP' (default) or 'SSR-RX'
            datum: For DATUM – PPP datum: 'WGS84' or 'PPPORIGINAL' (default)
            hor_std: For CONVERGE – horizontal std threshold in cm (int, >= 0)
            ver_std: For CONVERGE – vertical std threshold in cm (int, >= 0)
            add_crlf: Reserved for future use (handled by low-level sender)

        Examples:
            device.config_ppp('ENABLE', service='B2B-PPP')
            device.config_ppp('ENABLE', service='SSR-RX')
            device.config_ppp('DATUM', datum='WGS84')
            device.config_ppp('DATUM', datum='PPPORIGINAL')
            device.config_ppp('CONVERGE', hor_std=10, ver_std=20)
            device.config_ppp('DISABLE')
        """
        params: Dict[str, Any] = {'subcommand': subcommand}

        if subcommand.upper() == 'ENABLE':
            if service is not None:
                params['service'] = service
        elif subcommand.upper() == 'DATUM':
            if datum is not None:
                params['datum'] = datum
        elif subcommand.upper() == 'CONVERGE':
            if hor_std is not None:
                params['hor_std'] = hor_std
            if ver_std is not None:
                params['ver_std'] = ver_std

        return self.config_command('PPP', **params)

    def config_smooth(self,
                      computing_engine: str,
                      parameter: Union[int, str],
                      add_crlf: Optional[bool] = None) -> dict:
        """
        Configure SMOOTH function (CONFIG SMOOTH)

        Args:
            computing_engine:
                - 'RTKHEIGHT' – smoothing of RTK height results
                - 'HEADING' – smoothing of heading results
                - 'PSRVEL' – control smoothing of Doppler velocity in SPPNAV
            parameter:
                - For 'RTKHEIGHT' / 'HEADING': integer time length in epochs, 0–100
                - For 'PSRVEL': 'enable' or 'disable'
            add_crlf: Reserved for future use (handled by low-level sender)

        Examples:
            # RTK height smoothing (10 epochs)
            device.config_smooth('RTKHEIGHT', 10)

            # Heading smoothing (10 epochs)
            device.config_smooth('HEADING', 10)

            # Enable Doppler velocity smoothing in SPPNAV
            device.config_smooth('PSRVEL', 'enable')

            # Disable Doppler velocity smoothing in SPPNAV
            device.config_smooth('PSRVEL', 'disable')
        """
        params: Dict[str, Any] = {
            'computing_engine': computing_engine,
            'parameter': parameter,
        }

        return self.config_command('SMOOTH', **params)

    def config_mmp(self,
                   state: Union[str, bool] = 'DISABLE',
                   add_crlf: Optional[bool] = None) -> dict:
        """
        Configure multi-path mitigation (CONFIG MMP)

        Args:
            state: 'ENABLE' or 'DISABLE' (default), or bool:
                   True  -> ENABLE
                   False -> DISABLE
            add_crlf: Reserved for future use (handled by low-level sender)

        Examples:
            device.config_mmp('ENABLE')
            device.config_mmp('DISABLE')
            device.config_mmp(True)   # ENABLE
            device.config_mmp(False)  # DISABLE
        """
        if isinstance(state, bool):
            state_str = 'ENABLE' if state else 'DISABLE'
        else:
            state_str = str(state).upper()

        params: Dict[str, Any] = {'state': state_str}
        return self.config_command('MMP', **params)

    def config_agnss(self,
                     state: Union[str, bool] = 'DISABLE',
                     add_crlf: Optional[bool] = None) -> dict:
        """
        Configure Assisted GNSS (CONFIG AGNSS)

        Args:
            state: 'ENABLE' or 'DISABLE' (default), or bool:
                   True  -> ENABLE
                   False -> DISABLE
            add_crlf: Reserved for future use (handled by low-level sender)

        Examples:
            device.config_agnss('ENABLE')
            device.config_agnss('DISABLE')
            device.config_agnss(True)   # ENABLE
            device.config_agnss(False)  # DISABLE
        """
        if isinstance(state, bool):
            state_str = 'ENABLE' if state else 'DISABLE'
        else:
            state_str = str(state).upper()

        params: Dict[str, Any] = {'state': state_str}
        return self.config_command('AGNSS', **params)

    def config_event(self, subcommand: str = 'DISABLE',
                     polarity: Optional[str] = None,
                     tguard: Optional[int] = None,
                     add_crlf: Optional[bool] = None) -> dict:
        """
        Configure EVENT function

        Args:
            subcommand: EVENT subcommand - 'ENABLE' or 'DISABLE' (default)
            polarity: For ENABLE - trigger polarity: 'POSITIVE' (rising edge) or 'NEGATIVE' (falling edge)
            tguard: For ENABLE - minimum time between two valid pulses in milliseconds
                   (default=4, min=2, max=3599999)
            add_crlf: If True, send command with \r\n, if False without, if None auto-detect

        Returns:
            Dictionary with response information

        Examples:
            # Disable EVENT (default)
            device.config_event('DISABLE')

            # Enable EVENT with POSITIVE polarity and tguard=10 (example from documentation)
            device.config_event('ENABLE', polarity='POSITIVE', tguard=10)

            # Enable EVENT with NEGATIVE polarity and default tguard=4
            device.config_event('ENABLE', polarity='NEGATIVE')
        """
        # Используем универсальную систему команд
        params = {'subcommand': subcommand}

        if subcommand.upper() == 'ENABLE':
            if polarity is not None:
                params['polarity'] = polarity
            if tguard is not None:
                params['tguard'] = tguard

        return self.config_command('EVENT', **params)

    def __enter__(self):
        """Вход в контекстный менеджер"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Выход из контекстного менеджера"""
        self.disconnect()


def list_serial_ports():
    """Список доступных последовательных портов"""
    ports = serial.tools.list_ports.comports()
    print("Доступные последовательные порты:")
    for port in ports:
        print(f"  {port.device} - {port.description}")
    print("\nДля TCP используйте: --port host:port или --port tcp://host:port")
    return [port.device for port in ports]

