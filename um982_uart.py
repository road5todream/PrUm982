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
    query_arddop as _do_query_arddop,
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
    query_arddoph as _do_query_arddoph,
    query_mode as _do_query_mode,
    log as _do_log,
    unlog as _do_unlog,
)
from um982.data_output.observation import (
    send_obsv_stream_command as _send_obsv_stream_command,
    extract_one_obsv_message as _extract_one_obsv_message,
)


class CommandFormat(Enum):
    ASCII = "ascii"
    BINARY = "binary"


NO_ACK_COMMANDS = frozenset(("FRESET", "RESET", "ROVER", "HEADING2"))


class UM982UART:

    def __init__(self, port: str, baudrate: int = 460800, timeout: float = 1.0, debug: Optional[bool] = None):
        self._core = Um982Core(port=port, baudrate=baudrate, timeout=timeout, debug=debug)

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

    def send_ascii_command(self, command: str, add_crlf: bool = True) -> bool:
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
        """Запрос информации о COM портах устройства"""
        config_result = self.query_config(use_lines=False, add_crlf=add_crlf)
        
        if 'error' in config_result:
            return config_result
        
        messages = config_result.get('parsed', {}).get('messages', [])
        ports_info = {}
        
        for msg in messages:
            if isinstance(msg, dict):
                raw = msg.get('raw', '')

                if 'CONFIG,COM' in raw or 'CONFIG COM' in raw:
                    parts = raw.split(',')
                    if len(parts) >= 3:
                        port_name = parts[1].strip()
                        config_part = parts[2].strip()
                        
                        if '*' in config_part:
                            config_part = config_part[:config_part.rfind('*')]
                        
                        config_fields = config_part.split()
                        if len(config_fields) >= 3:
                            port_key = port_name.lower()
                            port_info = {
                                'port': port_name,
                                'raw': raw
                            }
                            
                            if len(config_fields) >= 3:
                                try:
                                    port_info['baudrate'] = int(config_fields[2])
                                except ValueError:
                                    port_info['baudrate'] = None
                            
                            if len(config_fields) >= 4:
                                try:
                                    port_info['data_bits'] = int(config_fields[3])
                                except ValueError:
                                    port_info['data_bits'] = 8
                            
                            if len(config_fields) >= 5:
                                port_info['parity'] = config_fields[4].upper()
                            else:
                                port_info['parity'] = 'N'
                            
                            if len(config_fields) >= 6:
                                try:
                                    port_info['stop_bits'] = int(config_fields[5])
                                except ValueError:
                                    port_info['stop_bits'] = 1
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
        command = "VERSIONB" if binary else "VERSIONA"
        return _do_run_data_query(
            self._core,
            command=command,
            parse_func=lambda d, b: self._parse_version_message(d, b),
            binary=binary,
            add_crlf=add_crlf,
            wait_time=0.8,
            read_attempts=15,
            read_timeout=2.0,
            result_key="version",
        )
    
    def query_obsvm(self, port: str = "COM1", rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        return _do_query_obsvm(self._core, port=port, rate=rate, binary=binary, add_crlf=add_crlf)
    
    def query_obsvh(self, port: str = "COM1", rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        return _do_query_obsvh(self._core, port=port, rate=rate, binary=binary, add_crlf=add_crlf)
    
    def query_obsvmcmp(self, port: str = "COM1", rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        return _do_query_obsvmcmp(self._core, port=port, rate=rate, binary=binary, add_crlf=add_crlf)

    def send_obsv_stream_command(
        self,
        stream_type: str,
        port: str = "COM1",
        rate: int = 1,
        binary: bool = False,
        add_crlf: Optional[bool] = None,
    ) -> bool:
        """Отправить команду вывода OBSVM/OBSVH/OBSVMCMP один раз. Дальше только читать с порта."""
        return _send_obsv_stream_command(
            self._core, stream_type=stream_type, port=port, rate=rate, binary=binary, add_crlf=add_crlf
        )

    def query_obsvbase(self, port: str = "COM1", trigger: str = "ONCHANGED", binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        return _do_query_obsvbase(self._core, port=port, trigger=trigger, binary=binary, add_crlf=add_crlf)

    def query_baseinfo(self, rate: int = 1, trigger: Optional[str] = None, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        return _do_query_baseinfo(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)
    
    def query_gpsion(self, rate: int = 1, trigger: Optional[str] = None, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        return _do_query_gpsion(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)

    def query_galion(self, rate: int = 1, trigger: Optional[str] = None, binary: bool = False,
                     add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса GALION через новый модуль данных."""
        return _do_query_galion(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)
    
    def query_gpsutc(self, rate: int = 1, trigger: Optional[str] = None, binary: bool = False,
                     add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса GPSUTC через новый модуль данных."""
        return _do_query_gpsutc(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)
    
    def query_bd3utc(self, rate: int = 1, trigger: Optional[str] = None, binary: bool = False,
                     add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса BD3UTC через новый модуль данных."""
        return _do_query_bd3utc(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)
    
    def query_bdsion(self, rate: int = 1, trigger: Optional[str] = None, binary: bool = False,
                     add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса BDSION через новый модуль данных."""
        return _do_query_bdsion(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)
    
    def query_bd3ion(self, rate: int = 1, trigger: Optional[str] = None, binary: bool = False,
                     add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса BD3ION через новый модуль данных."""
        return _do_query_bd3ion(self._core, rate=rate, trigger=trigger, binary=binary, add_crlf=add_crlf)
    
    def query_agric(self, port: Optional[str] = None, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
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
                    return {"error": f"Недопустимый параметр RESET: {p!r}. Допустимы: {', '.join(sorted(valid_params))}"}
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

    def query_arddop(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса ARDDOP через новый модуль данных."""
        return _do_query_arddop(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_bestnavxyz(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса BESTNAVXYZ через новый модуль данных."""
        return _do_query_bestnavxyz(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_arddoph(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса ARDDOPH через новый модуль данных."""
        return _do_query_arddoph(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    def query_hwstatus(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса HWSTATUS через новый модуль данных."""
        return _do_query_hwstatus(self._core, rate=rate, binary=binary, add_crlf=add_crlf)
    
    def query_agc(self, rate: int = 1, binary: bool = False, add_crlf: Optional[bool] = None) -> dict:
        """Фасад для запроса AGC через новый модуль данных."""
        return _do_query_agc(self._core, rate=rate, binary=binary, add_crlf=add_crlf)

    
    def query_mode(self, add_crlf: Optional[bool] = None) -> dict:
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
                    add_crlf: Optional[bool] = None) -> dict:

        use_geodetic = False
        use_ecef = False
        
        if lat is not None and lon is not None and hgt is not None:
            use_geodetic = True
            coord1, coord2, coord3 = lat, lon, hgt
        elif x is not None and y is not None and z is not None:
            use_ecef = True
            coord1, coord2, coord3 = x, y, z
        elif param1 is not None and param2 is not None and param3 is not None:
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
        
        if use_geodetic:
            if not (-90 <= coord1 <= 90):
                return {"error": f"Invalid latitude: {coord1}. Must be between -90 and 90 degrees"}
            if not (-180 <= coord2 <= 180):
                return {"error": f"Invalid longitude: {coord2}. Must be between -180 and 180 degrees"}
            if not (-30000 <= coord3 <= 30000):
                return {"error": f"Invalid height: {coord3}. Must be between -30000 and 30000 meters"}
        elif use_ecef:
            pass
        
        if station_id is not None:
            if not (0 <= station_id <= 4095):
                return {"error": f"Invalid station ID: {station_id}. Must be between 0 and 4095"}
        
        if use_geodetic or use_ecef:
            if station_id is not None:
                command = f"MODE BASE {station_id} {coord1} {coord2} {coord3}"
            else:
                command = f"MODE BASE {coord1} {coord2} {coord3}"
        else:
            if station_id is not None:
                command = f"MODE BASE {station_id}"
            else:
                command = "MODE BASE"
        
        return self._send_config_command(command, add_crlf=add_crlf)
    
    def set_mode_rover(self, add_crlf: Optional[bool] = None) -> dict:
        """Установить приёмник в режим ROVER (устройство может не присылать подтверждение)."""
        command = "MODE ROVER"
        return self._send_config_command(command, "ROVER", add_crlf=add_crlf)
    
    def set_mode_base_time(self, 
                          station_id: Optional[int] = None,
                          time: int = 60,
                          distance: Optional[float] = None,
                          add_crlf: Optional[bool] = None) -> dict:
        """Установить режим BASE TIME (самооптимизирующаяся база)."""
        if station_id is not None:
            if not (0 <= station_id <= 4095):
                return {"error": f"Invalid station ID: {station_id}. Must be between 0 and 4095"}
        
        if time < 0:
            return {"error": f"Invalid time: {time}. Cannot be negative"}
        
        if distance is not None:
            if not (0 <= distance <= 10):
                return {"error": f"Invalid distance: {distance}. Must be between 0 and 10 meters"}
        
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
    
    def set_mode_heading2(self, add_crlf: Optional[bool] = None) -> dict:
        """Установить приёмник в режим HEADING2 (устройство может не присылать подтверждение)."""
        command = "MODE HEADING2"
        return self._send_config_command(command, "HEADING2", add_crlf=add_crlf)


    def _parse_version_message(self, data: bytes, binary: bool = False) -> Optional[dict]:
        if binary:

            if len(data) < 24:
                return None
            
            # Ищем sync bytes (0xAA 0x44 0xB5) во всем потоке данных
            for i in range(len(data) - 24):
                if data[i] == 0xAA and data[i+1] == 0x44 and data[i+2] == 0xB5:
                    header = self._parse_unicore_header(data[i:i+24])
                    if header and header.get('message_id') == 37:
                        msg_length = header.get('message_length', 0)
                        if msg_length == 0:
                            # Если длина не указана, используем стандартную: 24 + 308 + 4 = 336
                            msg_length = 336
                        
                        if len(data) < i + msg_length:
                            continue
                        
                        offset = i + 24
                        
                        if len(data) >= offset + 4:
                            product_type = struct.unpack('<I', data[offset:offset+4])[0]
                            offset += 4
                            

                            if len(data) >= offset + 33:
                                sw_version_build = data[offset:offset+33].decode('ascii', errors='ignore').rstrip('\x00').rstrip()
                                offset += 33
                                
                                if len(data) >= offset + 129:
                                    psn = data[offset:offset+129].decode('ascii', errors='ignore').rstrip('\x00').rstrip()
                                    offset += 129
                                    
                                    if len(data) >= offset + 66:
                                        auth = data[offset:offset+66].decode('ascii', errors='ignore').rstrip('\x00').rstrip()
                                        offset += 66
                                        
                                        if len(data) >= offset + 33:
                                            efuse_id = data[offset:offset+33].decode('ascii', errors='ignore').rstrip('\x00').rstrip()
                                            offset += 33
                                            
                                            if len(data) >= offset + 43:
                                                comp_time = data[offset:offset+43].decode('ascii', errors='ignore').rstrip('\x00').rstrip()
                                                
                                                sw_version = sw_version_build
                                                
                                                crc_offset = i + msg_length - 4
                                                crc_value = None
                                                if len(data) >= crc_offset + 4:
                                                    crc_value = struct.unpack('<I', data[crc_offset:crc_offset+4])[0]
                                                
                                                return {
                                                    "format": "binary",
                                                    "product_type": product_type,
                                                    "product_name": self._get_product_name(product_type),
                                                    "sw_version": sw_version,
                                                    "auth": auth,
                                                    "psn": psn,
                                                    "efuse_id": efuse_id,
                                                    "comp_time": comp_time,
                                                    "header": header,
                                                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                                                    "message_offset": i
                                                }

            # Если не нашли в бинарном формате, возвращаем None
            return None
        else:
            try:
                version_marker = b'#VERSIONA'
                version_pos = data.find(version_marker)
                
                if version_pos >= 0:
                    text = data[version_pos:].decode('ascii', errors='ignore')
                else:
                    text = data.decode('ascii', errors='ignore')
                
                version_pattern = r'#VERSIONA[^\r\n]*'
                matches = re.finditer(version_pattern, text)
                
                for match in matches:
                    line = match.group(0).strip()
                    if not line:
                        continue
                    
                    parts = line.split(';')
                    if len(parts) >= 2:
                        header_part = parts[0].replace('#VERSIONA,', '')
                        data_part = parts[1]
                        

                        data_fields = []
                        current_field = ""
                        in_quotes = False
                        
                        for char in data_part:
                            if char == '"':
                                in_quotes = not in_quotes
                            elif char == ',' and not in_quotes:
                                if current_field:
                                    data_fields.append(current_field)
                                    current_field = ""
                            elif char == '*' and not in_quotes:
                                if current_field:
                                    data_fields.append(current_field)
                                break
                            else:
                                current_field += char
                        
                        if len(data_fields) >= 6:
                            return {
                                "format": "ascii",
                                "product_name": data_fields[0] if len(data_fields) > 0 else "",
                                "sw_version": data_fields[1] if len(data_fields) > 1 else "",
                                "psn": data_fields[2] if len(data_fields) > 2 else "",
                                "auth": data_fields[3] if len(data_fields) > 3 else "",
                                "efuse_id": data_fields[4] if len(data_fields) > 4 else "",
                                "comp_time": data_fields[5] if len(data_fields) > 5 else "",
                                "raw": line
                            }
            except Exception as e:
                pass
        
        if not binary:
            for i in range(len(data) - 24):
                if data[i] == 0xAA and data[i+1] == 0x44 and data[i+2] == 0xB5:
                    header = self._parse_unicore_header(data[i:i+24])
                    if header and header.get('message_id') == 37:
                        return self._parse_version_message(data[i:], binary=True)
        
        return None
    
    def _get_product_name(self, product_type: int) -> str:
        product_names = {
            0: "UNKNOWN",
            1: "UB4B0",
            2: "UM4B0",
            3: "UM480",
            4: "UM440",
            5: "UM482",
            6: "UM442",
            7: "UB482",
            8: "UT4B0",
            9: "UT900",
            10: "UB362L",
            11: "UB4B0M",
            12: "UB4B0J",
            13: "UM482L",
            14: "UM4B0L",
            15: "UT910",
            16: "CLAP-B",
            17: "UM982",
            18: "UM980",
            19: "UM960",
            20: "UM980i",
            21: "UM980A",
            22: "UM960A",
            23: "CLAP-C",
            24: "UM960L"
        }
        return product_names.get(product_type, f"UNKNOWN({product_type})")
    
    def _send_config_command(self, command: str, command_name: str = "", 
                            add_crlf: Optional[bool] = None) -> dict:
        """Универсальный метод для отправки CONFIG команд"""
        if add_crlf is None:
            add_crlf = (self.baudrate >= 460800)
        
        if self._core.serial_conn and self._core.serial_conn.in_waiting > 0:
            self._core.serial_conn.reset_input_buffer()
        
        if not self.send_ascii_command(command, add_crlf=add_crlf):
            return {"error": f"Не удалось отправить команду {command}"}
        
        time.sleep(0.5)
        
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
        
        parsed = self.parse_binary_response(response)
        
        messages = parsed.get('parsed', {}).get('messages', [])
        confirmation = None
        
        # Попытка найти подтверждение в NMEA сообщениях
        for msg in messages:
            raw = msg.get('raw', '')
            raw_upper = raw.upper()
            if 'COMMAND' in raw_upper and 'CONFIG' in raw_upper and 'OK' in raw_upper:
                if not command_name or command_name.upper() in raw_upper:
                    confirmation = raw
                    break
            elif 'COMMAND' in raw_upper and 'OK' in raw_upper:
                if command_name and command_name.upper() in raw_upper:
                    confirmation = raw
                    break
                elif 'MASK' in raw_upper or 'UNMASK' in raw_upper:
                    confirmation = raw
                    break
            # Поиск формата ответа CONFIG (только для команд, которые возвращают подтверждение)
            elif 'CONFIG' in raw_upper:
                if command_name and command_name.upper() == 'COM':
                    pass
                elif not command_name or command_name.upper() in raw_upper:
                    confirmation = raw
                    break
        
        message_sent_style = False
        if not confirmation:
            try:
                ascii_text = response.decode('ascii', errors='ignore')
                patterns = [
                    rf'\$command,CONFIG[^\r\n]*response: OK[^\r\n]*',
                    rf'\$command,{re.escape(command_name.upper())}[^\r\n]*response: OK[^\r\n]*',  # Для MASK/UNMASK
                ]
                
                if command_name and command_name.upper() != 'COM':
                    patterns.append(rf'\$CONFIG[^\r\n]*')
                for pattern in patterns:
                    match = re.search(pattern, ascii_text, re.IGNORECASE)
                    if match:
                        confirmation = match.group(0).strip()
                        break
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
            result["sent_no_response"] = False
            result["message_sent_style"] = True
            print("Команда отправлена")
        
        return result
    
    def config_command(self, command_type: str, **params) -> dict:
        """Универсальный метод для отправки CONFIG команд через систему регистрации"""
        try:
            from um982_commands import get_command_definition, get_command_names
            
            cmd_def = get_command_definition(command_type)
            if not cmd_def:
                available = get_command_names()
                return {"error": f"Unknown command type: {command_type}. Available: {available}"}
            
            if cmd_def.validator:
                error = cmd_def.validator(params)
                if error:
                    return {"error": error}
            
            command, warning = cmd_def.command_builder(params)
            
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
        params = {'enable': enable}
        
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
        return self.config_command('DGPS', timeout=timeout)
    
    def config_rtk(self, subcommand: str, timeout: Optional[int] = None,
                   param1: Optional[int] = None, param2: Optional[int] = None,
                   add_crlf: Optional[bool] = None) -> dict:
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
        params: Dict[str, Any] = {}

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
        params: Dict[str, Any] = {
            'computing_engine': computing_engine,
            'parameter': parameter,
        }

        return self.config_command('SMOOTH', **params)
    
    def config_mmp(self,
                   state: Union[str, bool] = 'DISABLE',
                   add_crlf: Optional[bool] = None) -> dict:
        if isinstance(state, bool):
            state_str = 'ENABLE' if state else 'DISABLE'
        else:
            state_str = str(state).upper()

        params: Dict[str, Any] = {'state': state_str}
        return self.config_command('MMP', **params)
    
    def config_agnss(self,
                     state: Union[str, bool] = 'DISABLE',
                     add_crlf: Optional[bool] = None) -> dict:
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

