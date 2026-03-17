from typing import Dict, Callable, Any, Optional, List, Tuple
from dataclasses import dataclass


@dataclass
class CommandDefinition:
    name: str
    command_builder: Callable[[Dict[str, Any]], Tuple[str, Optional[str]]]
    validator: Optional[Callable[[Dict[str, Any]], Optional[str]]] = None


def validate_choice(value: Any, choices: List[Any], param_name: str) -> Optional[str]:
    if value not in choices:
        return f"Invalid {param_name}: {value}. Must be one of {choices}"
    return None


def validate_range(value: Any, min_val: int, max_val: int, param_name: str) -> Optional[str]:
    if not isinstance(value, int):
        return f"Invalid {param_name}: {value}. Must be an integer"
    if value < min_val or value > max_val:
        return f"Invalid {param_name}: {value}. Must be between {min_val} and {max_val}"
    return None


def validate_multiple_of(value: int, multiple: int, param_name: str) -> Optional[str]:
    if value % multiple != 0:
        return f"Invalid {param_name}: {value}. Must be a multiple of {multiple}"
    return None


COMMANDS: Dict[str, CommandDefinition] = {}


def register_command(name: str, builder: Callable, validator: Optional[Callable] = None):
    COMMANDS[name] = CommandDefinition(name=name, command_builder=builder, validator=validator)


def build_com_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    port = params['port'].upper()
    baudrate = params['baudrate']
    data_bits = params.get('data_bits', 8)
    parity = params.get('parity', 'N').upper()
    stop_bits = params.get('stop_bits', 1)
    return f"CONFIG {port} {baudrate} {data_bits} {parity} {stop_bits}", None


def validate_com_command(params: Dict[str, Any]) -> Optional[str]:
    port = params.get('port', '').upper()
    if port not in ['COM1', 'COM2', 'COM3']:
        return f"Invalid port name: {port}. Must be COM1, COM2, or COM3"
    
    valid_baudrates = [9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600]
    if params.get('baudrate') not in valid_baudrates:
        return f"Invalid baudrate: {params.get('baudrate')}. Must be one of {valid_baudrates}"
    
    if params.get('data_bits', 8) != 8:
        return "Invalid data_bits: Currently only 8 is supported"
    
    parity = params.get('parity', 'N').upper()
    if parity not in ['N', 'E', 'O']:
        return f"Invalid parity: {parity}. Must be 'N', 'E', or 'O'"
    
    if params.get('stop_bits', 1) not in [1, 2]:
        return f"Invalid stop_bits: Must be 1 or 2"
    
    return None


register_command('COM', build_com_command, validate_com_command)


def build_pps_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    enable = params['enable'].upper()
    
    if enable == 'DISABLE':
        return "CONFIG PPS DISABLE", None
    
    timeref = params['timeref'].upper()
    polarity = params['polarity'].upper()
    width = params['width']
    period = params['period']
    rf_delay = params['rf_delay']
    user_delay = params['user_delay']
    
    return f"CONFIG PPS {enable} {timeref} {polarity} {width} {period} {rf_delay} {user_delay}", None


def validate_pps_command(params: Dict[str, Any]) -> Optional[str]:
    enable = params.get('enable', '').upper()
    if enable not in ['DISABLE', 'ENABLE', 'ENABLE2', 'ENABLE3']:
        return f"Invalid enable mode: {enable}. Must be DISABLE, ENABLE, ENABLE2, or ENABLE3"
    
    if enable == 'DISABLE':
        return None
    
    required = ['timeref', 'polarity', 'width', 'period', 'rf_delay', 'user_delay']
    for param in required:
        if param not in params:
            return f"Parameter '{param}' is required for {enable} mode"
    
    timeref = params['timeref'].upper()
    if timeref not in ['GPS', 'BDS', 'GAL', 'GLO']:
        return f"Invalid timeref: {timeref}. Must be GPS, BDS, GAL, or GLO"
    
    polarity = params['polarity'].upper()
    if polarity not in ['POSITIVE', 'NEGATIVE']:
        return f"Invalid polarity: {polarity}. Must be POSITIVE or NEGATIVE"
    
    width = params['width']
    if not isinstance(width, int) or width <= 0:
        return f"Invalid width: {width}. Must be a positive integer (microseconds)"
    
    period = params['period']
    err = validate_range(period, 50, 20000, 'period')
    if err:
        return err
    err = validate_multiple_of(period, 50, 'period')
    if err:
        return err
    
    period_us = period * 1000
    if width >= period_us:
        return f"Invalid width: {width} microseconds must be smaller than period: {period_us} microseconds"
    
    err = validate_range(params['rf_delay'], -32768, 32767, 'rf_delay')
    if err:
        return err
    
    err = validate_range(params['user_delay'], -32768, 32767, 'user_delay')
    if err:
        return err
    
    return None


register_command('PPS', build_pps_command, validate_pps_command)


def build_dgps_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    timeout = params['timeout']
    return f"CONFIG DGPS TIMEOUT {timeout}", None


def validate_dgps_command(params: Dict[str, Any]) -> Optional[str]:
    timeout = params.get('timeout')
    err = validate_range(timeout, 0, 1800, 'timeout')
    return err


register_command('DGPS', build_dgps_command, validate_dgps_command)


def build_rtk_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    subcommand = params.get('subcommand', '').upper()
    
    if subcommand == 'TIMEOUT':
        timeout = params['timeout']
        return f"CONFIG RTK TIMEOUT {timeout}", None
    elif subcommand == 'RELIABILITY':
        param1 = params.get('param1')
        param2 = params.get('param2')
        if param2 is not None:
            return f"CONFIG RTK RELIABILITY {param1} {param2}", None
        else:
            return f"CONFIG RTK RELIABILITY {param1}", None
    elif subcommand == 'USER_DEFAULTS':
        return "CONFIG RTK USER_DEFAULTS", None
    elif subcommand == 'RESET':
        return "CONFIG RTK RESET", None
    elif subcommand == 'DISABLE':
        return "CONFIG RTK DISABLE", None
    else:
        return "", "Unknown RTK subcommand"


def validate_rtk_command(params: Dict[str, Any]) -> Optional[str]:
    subcommand = params.get('subcommand', '').upper()
    
    if not subcommand:
        return "RTK subcommand is required. Must be one of: TIMEOUT, RELIABILITY, USER_DEFAULTS, RESET, DISABLE"
    
    valid_subcommands = ['TIMEOUT', 'RELIABILITY', 'USER_DEFAULTS', 'RESET', 'DISABLE']
    if subcommand not in valid_subcommands:
        return f"Invalid RTK subcommand: {subcommand}. Must be one of {valid_subcommands}"
    
    if subcommand == 'TIMEOUT':
        timeout = params.get('timeout')
        err = validate_range(timeout, 0, 1800, 'timeout')
        if err:
            return err
    
    elif subcommand == 'RELIABILITY':
        param1 = params.get('param1')
        if param1 is None:
            return "Parameter 'param1' (RTK reliability threshold) is required for RELIABILITY subcommand"
        if param1 not in [1, 2, 3, 4]:
            return f"Invalid param1: {param1}. Must be 1, 2, 3, or 4 (RTK reliability threshold)"
        
        param2 = params.get('param2')
        if param2 is not None:
            if param2 not in [1, 2, 3, 4]:
                return f"Invalid param2: {param2}. Must be 1, 2, 3, or 4 (ADR reliability threshold)"
            if param2 not in [1, 4]:
                return f"Invalid param2: {param2}. Must be 1 (Low reliability, default) or 4 (High reliability). Values 2 and 3 are reserved"

    return None


register_command('RTK', build_rtk_command, validate_rtk_command)


def build_standalone_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    subcommand = params.get('subcommand', 'ENABLE').upper()
    
    if subcommand == 'DISABLE':
        return "CONFIG STANDALONE DISABLE", None

    latitude = params.get('latitude')
    longitude = params.get('longitude')
    altitude = params.get('altitude')
    time = params.get('time')
    
    if latitude is not None or longitude is not None or altitude is not None:
        if latitude is None or longitude is None or altitude is None:
            return "", "All coordinates (latitude, longitude, altitude) are required when using coordinate mode"
        return f"CONFIG STANDALONE ENABLE {latitude} {longitude} {altitude}", None
    elif time is not None:
        return f"CONFIG STANDALONE ENABLE {time}", None
    else:
        return "CONFIG STANDALONE ENABLE", None


def validate_standalone_command(params: Dict[str, Any]) -> Optional[str]:
    subcommand = params.get('subcommand', 'ENABLE').upper()
    
    if subcommand not in ['ENABLE', 'DISABLE']:
        return f"Invalid STANDALONE subcommand: {subcommand}. Must be ENABLE or DISABLE"
    
    if subcommand == 'DISABLE':
        return
    
    latitude = params.get('latitude')
    longitude = params.get('longitude')
    altitude = params.get('altitude')
    time = params.get('time')
    
    has_coords = any([latitude is not None, longitude is not None, altitude is not None])
    has_time = time is not None
    
    if has_coords and has_time:
        return "Cannot specify both coordinates and time parameters simultaneously"
    
    if latitude is not None or longitude is not None or altitude is not None:
        if latitude is None or longitude is None or altitude is None:
            return "All coordinates (latitude, longitude, altitude) must be provided together"
        
        if not isinstance(latitude, (int, float)):
            return f"Invalid latitude: {latitude}. Must be a number"
        if latitude < -90 or latitude > 90:
            return f"Invalid latitude: {latitude}. Must be between -90 and 90 degrees"
        
        if not isinstance(longitude, (int, float)):
            return f"Invalid longitude: {longitude}. Must be a number"
        if longitude < -180 or longitude > 180:
            return f"Invalid longitude: {longitude}. Must be between -180 and 180 degrees"
        
        if not isinstance(altitude, (int, float)):
            return f"Invalid altitude: {altitude}. Must be a number"
        if altitude < -30000 or altitude > 18000:
            return f"Invalid altitude: {altitude}. Must be between -30000 and 18000 meters"
    
    if time is not None:
        if not isinstance(time, (int, float)):
            return f"Invalid time: {time}. Must be a number"
        if time < 3 or time > 100:
            return f"Invalid time: {time}. Must be between 3 and 100 seconds"
    
    return None


register_command('STANDALONE', build_standalone_command, validate_standalone_command)


def build_heading_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """Построение команды CONFIG HEADING"""
    subcommand = params.get('subcommand', '').upper()
    
    if subcommand in ['FIXLENGTH', 'VARIABLELENGTH', 'STATIC', 'LOWDYNAMIC', 'TRACTOR']:
        return f"CONFIG HEADING {subcommand}", None
    elif subcommand == 'LENGTH':
        param1 = params.get('param1')
        param2 = params.get('param2')
        if param1 is not None and param2 is not None:
            return f"CONFIG HEADING LENGTH {param1} {param2}", None
        elif param1 is not None:
            return f"CONFIG HEADING LENGTH {param1}", None
        else:
            return "CONFIG HEADING LENGTH", None
    elif subcommand == 'RELIABILITY':
        param1 = params.get('param1')
        if param1 is not None:
            return f"CONFIG HEADING RELIABILITY {param1}", None
        else:
            return "", "Parameter 'param1' is required for RELIABILITY subcommand"
    elif subcommand == 'OFFSET':
        heading_offset = params.get('heading_offset')
        pitch_offset = params.get('pitch_offset')
        if heading_offset is not None and pitch_offset is not None:
            return f"CONFIG HEADING OFFSET {heading_offset} {pitch_offset}", None
        else:
            return "", "Both 'heading_offset' and 'pitch_offset' parameters are required for OFFSET subcommand"
    else:
        return "", f"Unknown HEADING subcommand: {subcommand}"


def validate_heading_command(params: Dict[str, Any]) -> Optional[str]:
    subcommand = params.get('subcommand', '').upper()
    
    if not subcommand:
        return "HEADING subcommand is required. Must be one of: FIXLENGTH, VARIABLELENGTH, STATIC, LOWDYNAMIC, TRACTOR, LENGTH, RELIABILITY, OFFSET"
    
    valid_subcommands = ['FIXLENGTH', 'VARIABLELENGTH', 'STATIC', 'LOWDYNAMIC', 'TRACTOR', 'LENGTH', 'RELIABILITY', 'OFFSET']
    if subcommand not in valid_subcommands:
        return f"Invalid HEADING subcommand: {subcommand}. Must be one of {valid_subcommands}"
    
    if subcommand == 'LENGTH':
        param1 = params.get('param1')
        param2 = params.get('param2')
        
        if param1 is not None:
            if not isinstance(param1, (int, float)) or param1 <= 0:
                return f"Invalid param1: {param1}. Must be a positive number (baseline length in centimeters)"
        
        if param2 is not None:
            if not isinstance(param2, (int, float)) or param2 <= 0:
                return f"Invalid param2: {param2}. Must be a positive number (error tolerance in centimeters)"
        
        if param2 is not None and param1 is None:
            return "Parameter 'param1' (baseline length) is required when 'param2' (error tolerance) is specified"
    
    elif subcommand == 'RELIABILITY':
        param1 = params.get('param1')
        if param1 is None:
            return "Parameter 'param1' (reliability threshold) is required for RELIABILITY subcommand"
        if param1 not in [1, 2, 3, 4]:
            return f"Invalid param1: {param1}. Must be 1, 2, 3, or 4 (reliability threshold)"
    
    elif subcommand == 'OFFSET':
        heading_offset = params.get('heading_offset')
        pitch_offset = params.get('pitch_offset')
        
        if heading_offset is None:
            return "Parameter 'heading_offset' is required for OFFSET subcommand"
        if pitch_offset is None:
            return "Parameter 'pitch_offset' is required for OFFSET subcommand"
        
        if not isinstance(heading_offset, (int, float)):
            return f"Invalid heading_offset: {heading_offset}. Must be a number"
        if heading_offset < -180.0 or heading_offset > 180.0:
            return f"Invalid heading_offset: {heading_offset}. Must be between -180.0 and 180.0 degrees"

        if not isinstance(pitch_offset, (int, float)):
            return f"Invalid pitch_offset: {pitch_offset}. Must be a number"
        if pitch_offset < -90.0 or pitch_offset > 90.0:
            return f"Invalid pitch_offset: {pitch_offset}. Must be between -90.0 and 90.0 degrees"
    return None


register_command('HEADING', build_heading_command, validate_heading_command)


def build_sbas_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    subcommand = params.get('subcommand', '').upper()
    
    if subcommand == 'DISABLE':
        return "CONFIG SBAS DISABLE", None
    
    elif subcommand == 'ENABLE':
        mode = params.get('mode', 'AUTO').upper()
        valid_modes = ['AUTO', 'WAAS', 'GAGAN', 'MSAS', 'EGNOS', 'SDCM', 'BDS']
        if mode not in valid_modes:
            return "", f"Invalid SBAS mode: {mode}. Must be one of {valid_modes}"
        return f"CONFIG SBAS ENABLE {mode}", None
    
    elif subcommand == 'TIMEOUT':
        timeout = params.get('timeout')
        if timeout is not None:
            return f"CONFIG SBAS TIMEOUT {timeout}", None
        else:
            return "", "Parameter 'timeout' is required for TIMEOUT subcommand"
    else:
        return "", f"Unknown SBAS subcommand: {subcommand}"


def validate_sbas_command(params: Dict[str, Any]) -> Optional[str]:
    subcommand = params.get('subcommand', '').upper()
    
    if not subcommand:
        return "SBAS subcommand is required. Must be one of: ENABLE, DISABLE, TIMEOUT"
    
    valid_subcommands = ['ENABLE', 'DISABLE', 'TIMEOUT']
    if subcommand not in valid_subcommands:
        return f"Invalid SBAS subcommand: {subcommand}. Must be one of {valid_subcommands}"
    
    if subcommand == 'ENABLE':
        mode = params.get('mode', 'AUTO').upper()
        valid_modes = ['AUTO', 'WAAS', 'GAGAN', 'MSAS', 'EGNOS', 'SDCM', 'BDS']
        if mode not in valid_modes:
            return f"Invalid SBAS mode: {mode}. Must be one of {valid_modes}"
    
    elif subcommand == 'TIMEOUT':
        timeout = params.get('timeout')
        if timeout is None:
            return "Parameter 'timeout' is required for TIMEOUT subcommand"
        err = validate_range(timeout, 120, 1800, 'timeout')
        if err:
            return err
    
    return None


register_command('SBAS', build_sbas_command, validate_sbas_command)


def build_event_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """Построение команды CONFIG EVENT"""
    subcommand = params.get('subcommand', 'DISABLE').upper()
    
    if subcommand == 'DISABLE':
        return "CONFIG EVENT DISABLE", None
    
    elif subcommand == 'ENABLE':
        polarity = params.get('polarity', 'POSITIVE').upper()
        tguard = params.get('tguard', 4)  # Default = 4 ms
        
        if polarity not in ['POSITIVE', 'NEGATIVE']:
            return "", f"Invalid polarity: {polarity}. Must be POSITIVE or NEGATIVE"
        
        return f"CONFIG EVENT ENABLE {polarity} {tguard}", None
    
    else:
        return "", f"Unknown EVENT subcommand: {subcommand}"


def validate_event_command(params: Dict[str, Any]) -> Optional[str]:
    subcommand = params.get('subcommand', 'DISABLE').upper()
    
    if subcommand not in ['ENABLE', 'DISABLE']:
        return f"Invalid EVENT subcommand: {subcommand}. Must be ENABLE or DISABLE"
    
    if subcommand == 'DISABLE':
        return None
    polarity = params.get('polarity', 'POSITIVE').upper()
    if polarity not in ['POSITIVE', 'NEGATIVE']:
        return f"Invalid polarity: {polarity}. Must be POSITIVE or NEGATIVE"
    
    tguard = params.get('tguard', 4)
    if not isinstance(tguard, (int, float)):
        return f"Invalid tguard: {tguard}. Must be a number"
    if tguard < 2 or tguard > 3599999:
        return f"Invalid tguard: {tguard}. Must be between 2 and 3599999 milliseconds"
    
    return None


register_command('EVENT', build_event_command, validate_event_command)


def build_undulation_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """
    Построение команды CONFIG UNDULATION
    
    Поддерживаются два режима:
    - AUTO: использовать встроенную геоидную модель
    - SEPARATION: фиксированное значение разделения геоида (метры)
    """
    mode = params.get('mode', 'AUTO').upper()
    separation = params.get('separation')

    if mode == 'AUTO' and separation is None:
        return "CONFIG UNDULATION AUTO", None

    if separation is not None:
        # Значение разделения геоида с точностью до 4 знаков после запятой
        return f"CONFIG UNDULATION {separation:.4f}", None

    # Если параметры заданы некорректно, вернём ошибку через строку
    return "", "Either mode='AUTO' or separation value must be provided"


def validate_undulation_command(params: Dict[str, Any]) -> Optional[str]:
    """
    Валидация команды CONFIG UNDULATION
    
    Параметры:
    - mode: 'AUTO' (по умолчанию)
    - separation: пользовательское значение разделения геоида, метры
                  диапазон: -1000.0000 .. +1000.0000
    """
    mode = params.get('mode', 'AUTO')
    separation = params.get('separation')

    # Если задано значение разделения геоида, проверяем его
    if separation is not None:
        if not isinstance(separation, (int, float)):
            return f"Invalid separation: {separation}. Must be a number in meters"
        if separation < -1000.0 or separation > 1000.0:
            return f"Invalid separation: {separation}. Must be between -1000.0000 and +1000.0000 meters"
        return None

    # Если separation не задан, разрешаем только режим AUTO (по умолчанию)
    if mode is not None and str(mode).upper() != 'AUTO':
        return f"Invalid mode: {mode}. Must be 'AUTO' or provide 'separation' value"

    return None


register_command('UNDULATION', build_undulation_command, validate_undulation_command)


def build_smooth_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """
    Построение команды CONFIG SMOOTH
    
    Синтаксис:
        CONFIG SMOOTH [computing_engine] [parameter]
    
    computing_engine:
        - RTKHEIGHT <time_length>
        - HEADING <time_length>
        - PSRVEL enable|disable
    """
    engine = params['computing_engine'].upper()
    parameter = params['parameter']

    # Для RTKHEIGHT и HEADING параметр — длина сглаживания (эпохи)
    if engine in ('RTKHEIGHT', 'HEADING'):
        return f"CONFIG SMOOTH {engine} {int(parameter)}", None

    # Для PSRVEL параметр — enable|disable
    if engine == 'PSRVEL':
        value = str(parameter).lower()
        return f"CONFIG SMOOTH PSRVEL {value}", None

    # На всякий случай (хотя валидатор не должен сюда пускать)
    return "", f"Unknown computing_engine: {engine}"


def validate_smooth_command(params: Dict[str, Any]) -> Optional[str]:
    """
    Валидация команды CONFIG SMOOTH
    
    computing_engine:
        - 'RTKHEIGHT' (сглаживание высоты RTK)
        - 'HEADING' (сглаживание курса)
        - 'PSRVEL' (сглаживание доплеровской скорости в SPPNAV)
    
    Параметры:
        - RTKHEIGHT/HEADING: time_length (целое число 0–100, в эпохах)
        - PSRVEL: 'enable' или 'disable'
    """
    engine = str(params.get('computing_engine', '')).upper()
    if not engine:
        return "computing_engine is required. Must be one of: RTKHEIGHT, HEADING, PSRVEL"

    valid_engines = ['RTKHEIGHT', 'HEADING', 'PSRVEL']
    if engine not in valid_engines:
        return f"Invalid computing_engine: {engine}. Must be one of {valid_engines}"

    if 'parameter' not in params:
        return "parameter is required for CONFIG SMOOTH"

    parameter = params['parameter']

    if engine in ('RTKHEIGHT', 'HEADING'):
        # time_length: 0–100 эпох
        if not isinstance(parameter, int):
            return f"Invalid parameter for {engine}: {parameter}. Must be an integer (time length in epochs)"
        if parameter < 0 or parameter > 100:
            return f"Invalid parameter for {engine}: {parameter}. Must be between 0 and 100 epochs"
        return None

    if engine == 'PSRVEL':
        value = str(parameter).lower()
        if value not in ('enable', 'disable'):
            return f"Invalid parameter for PSRVEL: {parameter}. Must be 'enable' or 'disable'"
        return None

    return None


register_command('SMOOTH', build_smooth_command, validate_smooth_command)


def build_mmp_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """
    Построение команды CONFIG MMP
    
    Синтаксис:
        CONFIG MMP [parameter]
    
    Параметр:
        - ENABLE  – включить multi-path mitigation
        - DISABLE – выключить (по умолчанию)
    """
    state = str(params.get('state', 'DISABLE')).upper()
    return f"CONFIG MMP {state}", None


def validate_mmp_command(params: Dict[str, Any]) -> Optional[str]:
    """
    Валидация команды CONFIG MMP
    
    Разрешённые значения:
        - ENABLE
        - DISABLE
    """
    state = str(params.get('state', 'DISABLE')).upper()
    valid_states = ['ENABLE', 'DISABLE']
    if state not in valid_states:
        return f"Invalid state: {state}. Must be one of {valid_states}"
    return None


register_command('MMP', build_mmp_command, validate_mmp_command)


def build_agnss_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """
    Построение команды CONFIG AGNSS
    
    Синтаксис:
        CONFIG AGNSS [parameter]
    
    Параметр:
        - ENABLE  – включить AGNSS
        - DISABLE – выключить (по умолчанию)
    """
    state = str(params.get('state', 'DISABLE')).upper()
    return f"CONFIG AGNSS {state}", None


def validate_agnss_command(params: Dict[str, Any]) -> Optional[str]:
    """
    Валидация команды CONFIG AGNSS
    
    Разрешённые значения:
        - ENABLE
        - DISABLE
    """
    state = str(params.get('state', 'DISABLE')).upper()
    valid_states = ['ENABLE', 'DISABLE']
    if state not in valid_states:
        return f"Invalid state: {state}. Must be one of {valid_states}"
    return None


register_command('AGNSS', build_agnss_command, validate_agnss_command)


def build_ppp_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """
    Построение команды CONFIG PPP
    
    Синтаксис:
        CONFIG PPP [parameter1] [parameter2]
        CONFIG PPP CONVERGE [HorSTD] [VerSTD]
    
    Параметры:
        - ENABLE <service>      (service: B2B-PPP | SSR-RX)
        - DATUM <datum>        (datum: WGS84 | PPPORIGINAL)
        - CONVERGE <HorSTD> <VerSTD> (см, целые)
        - DISABLE
    """
    subcommand = str(params.get('subcommand', '')).upper()
    
    if subcommand == 'ENABLE':
        service = str(params.get('service', 'B2B-PPP')).upper()
        # Формат сервиса должен сохранять дефис и регистр как в документации
        service_str = 'B2B-PPP' if service in ('B2B-PPP', 'B2BPPP', 'B2B') else 'SSR-RX'
        return f"CONFIG PPP ENABLE {service_str}", None
    
    if subcommand == 'DATUM':
        datum = str(params.get('datum', 'PPPORIGINAL')).upper()
        datum_str = 'WGS84' if datum == 'WGS84' else 'PPPORIGINAL'
        return f"CONFIG PPP DATUM {datum_str}", None
    
    if subcommand == 'CONVERGE':
        hor_std = params['hor_std']
        ver_std = params['ver_std']
        return f"CONFIG PPP CONVERGE {int(hor_std)} {int(ver_std)}", None
    
    if subcommand == 'DISABLE':
        return "CONFIG PPP DISABLE", None
    
    return "", f"Unknown PPP subcommand: {subcommand}"


def validate_ppp_command(params: Dict[str, Any]) -> Optional[str]:
    """
    Валидация команды CONFIG PPP
    
    subcommand:
        - ENABLE  service: B2B-PPP | SSR-RX
        - DATUM   datum: WGS84 | PPPORIGINAL
        - CONVERGE hor_std, ver_std: целые >= 0 (см)
        - DISABLE
    """
    subcommand = str(params.get('subcommand', '')).upper()
    if not subcommand:
        return "PPP subcommand is required. Must be one of: ENABLE, DATUM, CONVERGE, DISABLE"
    
    valid_subcommands = ['ENABLE', 'DATUM', 'CONVERGE', 'DISABLE']
    if subcommand not in valid_subcommands:
        return f"Invalid PPP subcommand: {subcommand}. Must be one of {valid_subcommands}"
    
    if subcommand == 'ENABLE':
        service = str(params.get('service', 'B2B-PPP')).upper()
        valid_services = ['B2B-PPP', 'SSR-RX']
        if service not in valid_services:
            return f"Invalid PPP service: {service}. Must be one of {valid_services}"
        return None
    
    if subcommand == 'DATUM':
        datum = str(params.get('datum', 'PPPORIGINAL')).upper()
        valid_datums = ['WGS84', 'PPPORIGINAL']
        if datum not in valid_datums:
            return f"Invalid PPP datum: {datum}. Must be one of {valid_datums}"
        return None
    
    if subcommand == 'CONVERGE':
        if 'hor_std' not in params or 'ver_std' not in params:
            return "Parameters 'hor_std' and 'ver_std' are required for PPP CONVERGE"
        hor_std = params.get('hor_std')
        ver_std = params.get('ver_std')
        for name, value in [('hor_std', hor_std), ('ver_std', ver_std)]:
            if not isinstance(value, int):
                return f"Invalid {name}: {value}. Must be an integer (centimeters)"
            if value < 0:
                return f"Invalid {name}: {value}. Must be >= 0 (centimeters)"
        return None
    
    if subcommand == 'DISABLE':
        return None
    
    return None


register_command('PPP', build_ppp_command, validate_ppp_command)


def build_mask_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    mask_type = params.get('mask_type', '').upper()
    
    if mask_type == 'RTCMCNO':
        cno = params.get('cno')
        if cno is None:
            return "", "Parameter 'cno' is required for RTCMCNO mask type"
        frequency = params.get('frequency')
        if frequency:
            return f"MASK RTCMCNO {cno} {frequency.upper()}", None
        else:
            return f"MASK RTCMCNO {cno}", None
    
    elif mask_type == 'CNO':
        cno = params.get('cno')
        if cno is None:
            return "", "Parameter 'cno' is required for CNO mask type"
        frequency = params.get('frequency')
        if frequency:
            return f"MASK CNO {cno} {frequency.upper()}", None
        else:
            return f"MASK CNO {cno}", None
    
    elif mask_type == 'PRN':
        system = params.get('system', '').upper()
        prn_id = params.get('prn_id')
        if not system:
            return "", "Parameter 'system' is required for PRN mask type"
        if prn_id is None:
            return "", "Parameter 'prn_id' is required for PRN mask type"
        return f"MASK {system} PRN {prn_id}", None
    
    else:
        elevation = params.get('elevation')
        system = params.get('system', '').upper()
        frequency = params.get('frequency', '').upper()
        
        if elevation is not None and system:
            return f"MASK {elevation} {system}", None

        elif elevation is not None:
            return f"MASK {elevation}", None

        elif system:
            return f"MASK {system}", None

        elif frequency:
            return f"MASK {frequency}", None
        
        else:
            return "", "At least one parameter (elevation, system, frequency, or mask_type) is required"


def validate_mask_command(params: Dict[str, Any]) -> Optional[str]:
    mask_type = params.get('mask_type', '').upper()
    
    valid_systems = ['GPS', 'BDS', 'GLO', 'GAL', 'QZSS', 'IRNSS']
    
    valid_frequencies = [
        'L1', 'L1CA', 'L1C', 'L2', 'L2C', 'L2P', 'L5',
        'B1', 'B2', 'B3', 'B1I', 'B2I', 'B3I', 'BD3B1C', 'BD3B2A', 'BD3B2B',
        'R1', 'R2', 'R3',
        'E1', 'E5A', 'E5B', 'E6C',
        'Q1', 'Q2', 'Q5', 'Q1CA', 'Q1C', 'Q2C',
        'I5'
    ]
    
    if mask_type == 'RTCMCNO':
        cno = params.get('cno')
        if cno is None:
            return "Parameter 'cno' is required for RTCMCNO mask type"
        if not isinstance(cno, (int, float)) or cno < 0:
            return f"Invalid cno: {cno}. Must be a non-negative number"
        
        frequency = params.get('frequency', '').upper()
        if frequency and frequency not in valid_frequencies:
            return f"Invalid frequency: {frequency}. Must be one of {valid_frequencies}"
        return None
    
    elif mask_type == 'CNO':
        cno = params.get('cno')
        if cno is None:
            return "Parameter 'cno' is required for CNO mask type"
        if not isinstance(cno, (int, float)) or cno < 0:
            return f"Invalid cno: {cno}. Must be a non-negative number"
        
        frequency = params.get('frequency', '').upper()
        if frequency and frequency not in valid_frequencies:
            return f"Invalid frequency: {frequency}. Must be one of {valid_frequencies}"
        return None
    
    elif mask_type == 'PRN':
        system = params.get('system', '').upper()
        if not system:
            return "Parameter 'system' is required for PRN mask type"
        if system not in valid_systems:
            return f"Invalid system: {system}. Must be one of {valid_systems}"
        
        prn_id = params.get('prn_id')
        if prn_id is None:
            return "Parameter 'prn_id' is required for PRN mask type"
        if not isinstance(prn_id, int) or prn_id < 1:
            return f"Invalid prn_id: {prn_id}. Must be a positive integer"
        return None
    
    else:
        elevation = params.get('elevation')
        system = params.get('system', '').upper()
        frequency = params.get('frequency', '').upper()
        
        if elevation is not None:
            if not isinstance(elevation, (int, float)):
                return f"Invalid elevation: {elevation}. Must be a number"
            if elevation < -90 or elevation > 90:
                return f"Invalid elevation: {elevation}. Must be between -90 and 90 degrees"
        
        if system:
            if system not in valid_systems:
                return f"Invalid system: {system}. Must be one of {valid_systems}"
        
        if frequency:
            if frequency not in valid_frequencies:
                return f"Invalid frequency: {frequency}. Must be one of {valid_frequencies}"
        
        if elevation is None and not system and not frequency:
            return "At least one parameter (elevation, system, or frequency) is required"
        
        return None


register_command('MASK', build_mask_command, validate_mask_command)


def build_unmask_command(params: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    system = params.get('system', '').upper()
    frequency = params.get('frequency', '').upper()
    prn_id = params.get('prn_id')
    
    if prn_id is not None:
        if not system:
            return "", "Parameter 'system' is required for UNMASK PRN command"
        return f"UNMASK {system} PRN {prn_id}", None
    
    elif system:
        return f"UNMASK {system}", None
    elif frequency:
        return f"UNMASK {frequency}", None
    else:
        return "", "At least one parameter (system, frequency, or prn_id) is required"


def validate_unmask_command(params: Dict[str, Any]) -> Optional[str]:
    valid_systems = ['GPS', 'BDS', 'GLO', 'GAL', 'QZSS', 'IRNSS']
    
    valid_frequencies = [
        'L1', 'L1CA', 'L1C', 'L2', 'L2C', 'L2P', 'L5',
        'B1', 'B2', 'B3', 'B1I', 'B2I', 'B3I', 'BD3B1C', 'BD3B2A', 'BD3B2B',
        'R1', 'R2', 'R3',
        'E1', 'E5A', 'E5B', 'E6C',
        'Q1', 'Q2', 'Q5', 'Q1CA', 'Q1C', 'Q2C',
        'I5'
    ]
    
    system = params.get('system', '').upper()
    frequency = params.get('frequency', '').upper()
    prn_id = params.get('prn_id')
    
    if prn_id is not None:
        if not system:
            return "Parameter 'system' is required for UNMASK PRN command"
        if system not in valid_systems:
            return f"Invalid system: {system}. Must be one of {valid_systems}"
        if not isinstance(prn_id, int) or prn_id < 1:
            return f"Invalid prn_id: {prn_id}. Must be a positive integer"
        return None
    
    if system:
        if system not in valid_systems:
            return f"Invalid system: {system}. Must be one of {valid_systems}"
        return None
    
    if frequency:
        if frequency not in valid_frequencies:
            return f"Invalid frequency: {frequency}. Must be one of {valid_frequencies}"
        return None
    
    if not system and not frequency and prn_id is None:
        return "At least one parameter (system, frequency, or prn_id) is required"
    
    return None


register_command('UNMASK', build_unmask_command, validate_unmask_command)


def get_command_names() -> List[str]:
    return list(COMMANDS.keys())


def get_command_definition(name: str) -> Optional[CommandDefinition]:
    return COMMANDS.get(name.upper())

