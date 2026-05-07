"""Сырые наблюдения: OBSVM, OBSVH, OBSVMCMP, OBSVBASE — запросы и парсеры."""
import re
import struct
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from um982.core import Um982Core
from um982.utils import format_log_period_wire, parse_unicore_header

from .base import _run_data_query
from .common import find_unicore_sync

# --- OBSVMCMP: Compressed Observation (Message ID 138), 24-byte records ---

OBSVMCMP_RECORD_SIZE = 24
OBSVMCMP_MESSAGE_ID = 138

# PSR_STD_TABLE: index 0..15 -> psr_std in meters (doc Table)
PSR_STD_TABLE: List[float] = [
    0.050, 0.075, 0.113, 0.169, 0.253, 0.380, 0.570, 0.854,
    1.281, 2.375, 4.750, 9.500, 19.000, 38.000, 76.000, 152.000,
]

# ASCII OBSVMCMP: заголовок на первой строке, далее переносы строк и запятые между 48-символьными hex (§7.3.4).
OBSVMCMP_ASCII_MESSAGE_RE = re.compile(rb"#OBSVMCMPA[\s\S]*?\*[0-9a-fA-F]{8}", re.IGNORECASE)

# Table 7-54 — биты 16–18 поля ch-tr-status (OBSVM / OBSVH / OBSVMCMP).
_NAV_SYSTEM_TABLE_7_54: Tuple[str, ...] = (
    "GPS",
    "GLONASS",
    "SBAS",
    "GAL",
    "BDS",
    "QZSS",
    "IRNSS",
    "?",
)

# Код сигнала — биты 21–27 (Table 7-54, «Signal type»), расшифровка зависит от системы.
_GPS_QZSS_SIGNAL: Dict[int, str] = {
    0: "L1 C/A",
    3: "L1C pilot",
    6: "L5 data",
    9: "L2P (Y)",
    11: "L1C data (semicodeless)",
    14: "L5 pilot",
    17: "L2C (L)",
}
_GLO_SIGNAL: Dict[int, str] = {
    0: "L1 C/A",
    5: "L2 C/A",
    6: "G3I",
    7: "G3Q",
}
_BDS_SIGNAL: Dict[int, str] = {
    0: "B1I",
    4: "B1Q",
    5: "B2Q",
    6: "B3Q",
    8: "B1C (Pilot)",
    12: "B2a (Pilot)",
    13: "B2b (I)",
    17: "B2I",
    21: "B3I",
    23: "B1C (Data)",
    28: "B2a (Data)",
}
_GAL_SIGNAL: Dict[int, str] = {
    1: "E1B",
    2: "E1C",
    12: "E5A pilot",
    17: "E5B pilot",
    18: "E6B",
    22: "E6C",
}
_SBAS_SIGNAL: Dict[int, str] = {
    0: "L1 C/A",
    6: "L5 (I)",
}
_IRNSS_SIGNAL: Dict[int, str] = {
    6: "L5 data",
    14: "L5 pilot",
}


def nav_system_from_ch_tr_status(ch_tr_status: int) -> str:
    code = (int(ch_tr_status) >> 16) & 0x7
    return _NAV_SYSTEM_TABLE_7_54[code] if 0 <= code < len(_NAV_SYSTEM_TABLE_7_54) else "?"


def obsv_signal_name_from_ch_tr(ch_tr_status: int) -> str:
    """Имя сигнала по Table 7-54 (биты 21–27 + особый случай GPS L2P/L2C, бит 26)."""
    st = int(ch_tr_status)
    nav = nav_system_from_ch_tr_status(st)
    code = (st >> 21) & 0x7F
    if nav == "GPS":
        if code == 9 and (st & 0x04000000):
            return "L2C (L)"
        return _GPS_QZSS_SIGNAL.get(code, f"код {code}")
    if nav == "GLONASS":
        return _GLO_SIGNAL.get(code, f"код {code}")
    if nav == "QZSS":
        return _GPS_QZSS_SIGNAL.get(code, f"код {code}")
    if nav == "BDS":
        return _BDS_SIGNAL.get(code, f"код {code}")
    if nav == "GAL":
        return _GAL_SIGNAL.get(code, f"код {code}")
    if nav == "SBAS":
        return _SBAS_SIGNAL.get(code, f"код {code}")
    if nav == "IRNSS":
        return _IRNSS_SIGNAL.get(code, f"код {code}")
    return "—"


def obsv_system_freq_field_text(system_freq: int, nav_system: str) -> str:
    sf = int(system_freq)
    if nav_system == "GLONASS":
        return str(sf)
    return "—"


# Совместимость со старым именем
def nav_system_from_obsvmcmp_channel_status(channel_tracking_status: int) -> str:
    return nav_system_from_ch_tr_status(channel_tracking_status)


def _obsvmcmp_get_bits(data: bytes, start_bit: int, num_bits: int) -> int:
    """
    Извлечь целое значение из битовой строки записи OBSVMCMP (24 байта = 192 бит).

    Бит 0 = LSB первого байта (little-endian). Диапазон [start_bit, start_bit+num_bits).
    """
    if len(data) < OBSVMCMP_RECORD_SIZE or start_bit < 0 or num_bits <= 0:
        return 0
    if start_bit + num_bits > OBSVMCMP_RECORD_SIZE * 8:
        num_bits = OBSVMCMP_RECORD_SIZE * 8 - start_bit
    value = sum(int(data[i]) << (i * 8) for i in range(min(24, len(data))))
    return (value >> start_bit) & ((1 << num_bits) - 1)


def _obsvmcmp_get_bits_signed(data: bytes, start_bit: int, num_bits: int) -> int:
    """Извлечь знаковое целое из битовой строки (знак по старшему биту)."""
    raw = _obsvmcmp_get_bits(data, start_bit, num_bits)
    if num_bits < 32 and (raw >> (num_bits - 1)) & 1:
        raw -= 1 << num_bits
    return raw


@dataclass
class ObsvmcmpRecord:
    """Одна сжатая запись OBSVMCMP (24 байта). Раскладка по битам из спецификации."""

    channel_tracking_status: int
    doppler_hz: float
    pseudorange_m: float
    adr_cycles: float
    psr_std_index: int
    psr_std_m: float
    adr_std_cycles: float
    prn: int
    lock_time_s: float
    cn0_dbhz: float
    glonass_frequency_number: int
    reserved: int
    raw_hex: str = ""

    def to_dict(self) -> Dict[str, Any]:
        nav = nav_system_from_ch_tr_status(self.channel_tracking_status)
        sig = obsv_signal_name_from_ch_tr(self.channel_tracking_status)
        return {
            "channel_tracking_status": self.channel_tracking_status,
            "channel_tracking_status_hex": f"0x{self.channel_tracking_status:08X}",
            "nav_system": nav,
            "signal_name": sig,
            "doppler_hz": self.doppler_hz,
            "pseudorange_m": self.pseudorange_m,
            "adr_cycles": self.adr_cycles,
            "psr_std_index": self.psr_std_index,
            "psr_std_m": self.psr_std_m,
            "adr_std_cycles": self.adr_std_cycles,
            "prn": self.prn,
            "lock_time_s": self.lock_time_s,
            "cn0_dbhz": self.cn0_dbhz,
            "glonass_frequency_number": self.glonass_frequency_number,
            "reserved": self.reserved,
            "raw_hex": self.raw_hex,
        }


def _parse_obsv_message(
    data: bytes,
    binary: bool,
    *,
    message_id: int,
    ascii_prefix: str,
    adr_std_divisor: float = 10000.0,
) -> Optional[dict]:
    """
    Общий парсер OBSV* для OBSVM / OBSVH / OBSVBASE.
    """
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != message_id:
                    continue
                offset = i + 24
                if len(data) < offset + 4:
                    continue

                obs_number = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                observations = []

                for _ in range(obs_number):
                    if len(data) < offset + 40:
                        break
                    system_freq = struct.unpack("<H", data[offset : offset + 2])[0]
                    offset += 2
                    prn = struct.unpack("<H", data[offset : offset + 2])[0]
                    offset += 2
                    psr = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    adr = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    psr_std = struct.unpack("<H", data[offset : offset + 2])[0]
                    offset += 2
                    adr_std = struct.unpack("<H", data[offset : offset + 2])[0]
                    offset += 2
                    dopp = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    cn0 = struct.unpack("<H", data[offset : offset + 2])[0]
                    offset += 2
                    reserved = struct.unpack("<H", data[offset : offset + 2])[0]
                    offset += 2
                    locktime = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    ch_tr_status = struct.unpack("<I", data[offset : offset + 4])[0]
                    offset += 4

                    nav = nav_system_from_ch_tr_status(ch_tr_status)
                    observations.append(
                        {
                            "system_freq": system_freq,
                            "nav_system": nav,
                            "signal_name": obsv_signal_name_from_ch_tr(ch_tr_status),
                            "system_freq_note": obsv_system_freq_field_text(system_freq, nav),
                            "prn": prn,
                            "psr": psr,
                            "adr": adr,
                            "psr_std": psr_std / 100.0,
                            "adr_std": adr_std / adr_std_divisor,
                            "dopp": dopp,
                            "cn0": cn0 / 100.0,
                            "reserved": reserved,
                            "locktime": locktime,
                            "ch_tr_status": ch_tr_status,
                            "ch_tr_status_hex": f"0x{ch_tr_status:08X}",
                        }
                    )

                msg_length = header.message_length
                crc_value = None
                if msg_length > 0 and len(data) >= i + msg_length:
                    crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]

                return {
                    "format": "binary",
                    "obs_number": obs_number,
                    "observations": observations,
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                        "week_number": header.week_number,
                        "seconds_of_week_ms": header.seconds_of_week_ms,
                        "time_status": header.time_status,
                    },
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        # OBSV* в ASCII может идти многострочно до *CRC (особенно OBSVBASE).
        pattern = rf"{re.escape(ascii_prefix)}[\s\S]*?\*[0-9a-fA-F]{{7,8}}"
        matches = list(re.finditer(pattern, text))
        if not matches:
            # Обратная совместимость: короткие строки без полного CRC (тесты/старые логи).
            pattern = rf"{re.escape(ascii_prefix)}[^\r\n]*"
            matches = list(re.finditer(pattern, text))
        for match in matches:
            line = match.group(0).strip()
            if not line:
                continue
            parts = line.split(";")
            if len(parts) < 2:
                continue
            data_part = parts[1]
            data_part_clean = data_part.split("*")[0]
            obs_fields = [f.strip() for f in re.split(r"[\r\n,]+", data_part_clean) if f.strip() != ""]
            if not obs_fields:
                continue
            try:
                obs_number = int(obs_fields[0])
            except ValueError:
                continue

            observations = []
            field_idx = 1
            for _ in range(obs_number):
                if field_idx + 10 >= len(obs_fields):
                    break
                try:
                    def _int(s: str) -> int:
                        return int(float(s)) if s else 0

                    system_freq = _int(obs_fields[field_idx])
                    prn = _int(obs_fields[field_idx + 1])
                    psr = float(obs_fields[field_idx + 2]) if obs_fields[field_idx + 2] else 0.0
                    adr = float(obs_fields[field_idx + 3]) if obs_fields[field_idx + 3] else 0.0
                    psr_std = _int(obs_fields[field_idx + 4])
                    adr_std = _int(obs_fields[field_idx + 5])
                    dopp = float(obs_fields[field_idx + 6]) if obs_fields[field_idx + 6] else 0.0
                    cn0 = _int(obs_fields[field_idx + 7])
                    reserved = _int(obs_fields[field_idx + 8])
                    locktime = float(obs_fields[field_idx + 9]) if obs_fields[field_idx + 9] else 0.0
                    ch_tr_status_str = obs_fields[field_idx + 10] if field_idx + 10 < len(obs_fields) else "0"
                    ch_tr_status = int(ch_tr_status_str, 16) if ch_tr_status_str else 0

                    nav = nav_system_from_ch_tr_status(ch_tr_status)
                    observations.append(
                        {
                            "system_freq": system_freq,
                            "nav_system": nav,
                            "signal_name": obsv_signal_name_from_ch_tr(ch_tr_status),
                            "system_freq_note": obsv_system_freq_field_text(system_freq, nav),
                            "prn": prn,
                            "psr": psr,
                            "adr": adr,
                            "psr_std": psr_std / 100.0,
                            "adr_std": adr_std / adr_std_divisor,
                            "dopp": dopp,
                            "cn0": cn0 / 100.0,
                            "reserved": reserved,
                            "locktime": locktime,
                            "ch_tr_status": ch_tr_status,
                            "ch_tr_status_hex": f"0x{ch_tr_status:08X}",
                        }
                    )
                except (ValueError, IndexError):
                    pass
                finally:
                    field_idx += 11

            # OBSVBASE ASCII на части прошивок приходит в компактном виде:
            # obsNumber, system_freq, prn,psr,adr,psrStd,adrStd,dopp,CNO,reserved,locktime,chTrStatus, prn,psr,...
            # Т.е. system_freq один раз, далее по 10 полей на наблюдение.
            if not observations and obs_number > 0 and len(obs_fields) >= 12:
                try:
                    def _int_compact(s: str) -> int:
                        return int(float(s)) if s else 0

                    system_freq_compact = _int_compact(obs_fields[1])
                    field_idx = 2
                    for _ in range(obs_number):
                        if field_idx >= len(obs_fields):
                            break
                        # Вариант 1 (классика): prn + 9 полей (включая dopp) = 10 токенов на запись.
                        # Вариант 2 (UM982 sample): prn + 8 полей (без dopp) = 9 токенов на запись.
                        rec_len = 10 if field_idx + 9 < len(obs_fields) else 9
                        if rec_len == 10:
                            # Если поле "dopp" выглядит как CNO (крупное целое), вероятно формат без dopp.
                            t_dopp = obs_fields[field_idx + 5]
                            try:
                                maybe_dopp = float(t_dopp)
                            except ValueError:
                                maybe_dopp = 0.0
                            if abs(maybe_dopp) > 1000:
                                rec_len = 9
                        if rec_len == 9 and field_idx + 8 >= len(obs_fields):
                            break

                        prn = _int_compact(obs_fields[field_idx])
                        psr = float(obs_fields[field_idx + 1]) if obs_fields[field_idx + 1] else 0.0
                        adr = float(obs_fields[field_idx + 2]) if obs_fields[field_idx + 2] else 0.0
                        psr_std = _int_compact(obs_fields[field_idx + 3])
                        adr_std = _int_compact(obs_fields[field_idx + 4])
                        if rec_len == 10:
                            dopp = float(obs_fields[field_idx + 5]) if obs_fields[field_idx + 5] else 0.0
                            cn0 = _int_compact(obs_fields[field_idx + 6])
                            reserved = _int_compact(obs_fields[field_idx + 7])
                            locktime = float(obs_fields[field_idx + 8]) if obs_fields[field_idx + 8] else 0.0
                            ch_tr_status_str = obs_fields[field_idx + 9]
                        else:
                            dopp = 0.0
                            cn0 = _int_compact(obs_fields[field_idx + 5])
                            reserved = _int_compact(obs_fields[field_idx + 6])
                            locktime = float(obs_fields[field_idx + 7]) if obs_fields[field_idx + 7] else 0.0
                            ch_tr_status_str = obs_fields[field_idx + 8]
                        ch_tr_status = int(ch_tr_status_str, 16) if ch_tr_status_str else 0
                        nav = nav_system_from_ch_tr_status(ch_tr_status)
                        observations.append(
                            {
                                "system_freq": system_freq_compact,
                                "nav_system": nav,
                                "signal_name": obsv_signal_name_from_ch_tr(ch_tr_status),
                                "system_freq_note": obsv_system_freq_field_text(system_freq_compact, nav),
                                "prn": prn,
                                "psr": psr,
                                "adr": adr,
                                "psr_std": psr_std / 100.0,
                                "adr_std": adr_std / adr_std_divisor,
                                "dopp": dopp,
                                "cn0": cn0 / 100.0,
                                "reserved": reserved,
                                "locktime": locktime,
                                "ch_tr_status": ch_tr_status,
                                "ch_tr_status_hex": f"0x{ch_tr_status:08X}",
                            }
                        )
                        field_idx += rec_len
                except Exception:
                    pass

            return {
                "format": "ascii",
                "obs_number": obs_number,
                "observations": observations,
                "raw": line,
            }
    except Exception:
        pass
    return None


def _parse_obsvm_message(data: bytes, binary: bool = False) -> Optional[dict]:
    return _parse_obsv_message(data, binary, message_id=12, ascii_prefix="#OBSVMA")


def _parse_obsvh_message(data: bytes, binary: bool = False) -> Optional[dict]:
    return _parse_obsv_message(data, binary, message_id=13, ascii_prefix="#OBSVHA")


def _obsvbase_observation_to_doc_format(obs: Dict[str, Any]) -> Dict[str, Any]:
    """Дополняет одну запись наблюдения полями из формата OBSVBASE (док): satellite_prn, pseudorange_m, carrier_phase_cycles, CNO_dbhz, locktime_sec, tracking_status."""
    out = dict(obs)
    out.setdefault("satellite_prn", obs.get("prn"))
    out.setdefault("pseudorange_m", obs.get("psr"))
    out.setdefault("carrier_phase_cycles", obs.get("adr"))
    out.setdefault("pseudorange_std", obs.get("psr_std"))
    out.setdefault("carrier_phase_std", obs.get("adr_std"))
    out.setdefault("doppler_hz", obs.get("dopp"))
    # CNO в доке в 0.01 dB-Hz (4500 = 45.00), у нас cn0 уже в dB-Hz
    out.setdefault("CNO_dbhz", int((obs.get("cn0") or 0) * 100))
    out.setdefault("locktime_sec", obs.get("locktime"))
    out.setdefault("tracking_status", obs.get("ch_tr_status_hex", "") or f"0x{obs.get('ch_tr_status', 0):08X}")
    return out


def _obsvbase_enrich_result(result: dict) -> dict:
    """Приводит результат парсера OBSVBASE к формату из доки (HEADER, OBSERVATION_SUMMARY, OBSERVATIONS)."""
    out = dict(result)
    h = out.get("header") or {}
    header_doc = {
        "message_name": "OBSVBASEA",
        "message_id": 284,
        "system": "GPS",
        "solution_status": "FINE" if h.get("time_status", 1) == 1 else "UNKNOWN",
        "gps_week": h.get("week_number"),
        "gps_time_ms": h.get("seconds_of_week_ms"),
    }
    out["header"] = {**h, **header_doc}
    out.setdefault("observation_summary", {"obs_number": out.get("obs_number", 0)})
    obs_list = out.get("observations", [])
    out["observations"] = [_obsvbase_observation_to_doc_format(o) for o in obs_list]
    return out


# Синхрослово альтернативного бинарного формата (не Unicore AA 44 B5)
_RAW_FE7E_SYNC = bytes((0xFE, 0x7E))


def _decode_raw_fe7e_format(data: bytes) -> Optional[dict]:
    """
    Попытка расшифровать сырой ответ с синхрословом 0xFE 0x7E (проприетарный формат).
    Гипотеза: 8 байт заголовок (FE 7E + 2 len + 2 type + 2 reserved), далее блоки по 40 байт.
    Для каждого блока пробуем LE/BE и выводим raw_hex + попытку полей (раскладка может отличаться).
    """
    if len(data) < 10:
        return None
    pos = data.find(_RAW_FE7E_SYNC)
    if pos < 0:
        return None
    # Заголовок 8 байт от sync
    head = data[pos : pos + 8]
    if len(head) < 8:
        return None
    payload_start = pos + 8
    payload = data[payload_start:]
    block_size = 40
    if len(payload) % block_size != 0:
        return None
    n_blocks = len(payload) // block_size
    # Поля заголовка (гипотетические)
    len_le = struct.unpack("<H", head[2:4])[0]
    len_be = struct.unpack(">H", head[2:4])[0]
    msg_type_le = struct.unpack("<H", head[4:6])[0]
    msg_type_be = struct.unpack(">H", head[4:6])[0]
    observations = []
    for i in range(n_blocks):
        block = payload[i * block_size : (i + 1) * block_size]
        if len(block) < 20:
            observations.append({"raw_hex": block.hex(), "decode_note": "block too short"})
            continue
        # Попытка как Unicore-подобный блок: 2 sys, 2 prn, 8 psr, 8 adr, 2 psr_std, 2 adr_std, 4 dopp, 2 cn0, 2 res, 4 lock, 4 ch_status
        prn_le = struct.unpack("<H", block[2:4])[0]
        prn_be = struct.unpack(">H", block[2:4])[0]
        psr_le = struct.unpack("<d", block[4:12])[0] if len(block) >= 12 else None
        psr_be = struct.unpack(">d", block[4:12])[0] if len(block) >= 12 else None
        adr_le = struct.unpack("<d", block[12:20])[0] if len(block) >= 20 else None
        adr_be = struct.unpack(">d", block[12:20])[0] if len(block) >= 20 else None
        # Выбираем правдоподобные: prn 1..37 или 1..24 GLONASS, psr ~1e7..3e7
        prn = prn_le if 1 <= prn_le <= 37 else (prn_be if 1 <= prn_be <= 37 else prn_le)
        psr = None
        if psr_le is not None and 1e7 <= abs(psr_le) <= 3e7:
            psr = psr_le
        elif psr_be is not None and 1e7 <= abs(psr_be) <= 3e7:
            psr = psr_be
        else:
            psr = psr_le if psr_le is not None else psr_be
        adr = adr_le if adr_le is not None else adr_be
        cn0_le = struct.unpack("<H", block[24:26])[0] / 100.0 if len(block) >= 26 else None
        cn0_be = struct.unpack(">H", block[24:26])[0] / 100.0 if len(block) >= 26 else None
        observations.append({
            "raw_hex": block.hex(),
            "format_hint": "raw_fe7e_40byte",
            "prn": prn,
            "prn_le": prn_le,
            "prn_be": prn_be,
            "psr": psr,
            "psr_le": psr_le,
            "psr_be": psr_be,
            "adr": adr,
            "adr_le": adr_le,
            "adr_be": adr_be,
            "cn0": cn0_le if cn0_le is not None else cn0_be,
            "cn0_le": cn0_le,
            "cn0_be": cn0_be,
        })
    return {
        "format": "raw_fe7e",
        "message_offset": pos,
        "header_hex": head.hex(),
        "header_fields_guess": {
            "sync": "FE 7E",
            "length_le": len_le,
            "length_be": len_be,
            "message_type_le": msg_type_le,
            "message_type_be": msg_type_be,
        },
        "obs_number": n_blocks,
        "observations": observations,
        "header": {
            "week_number": None,
            "seconds_of_week_ms": None,
            "time_status": None,
        },
    }


def _parse_obsvbase_message(data: bytes, binary: bool = False) -> Optional[dict]:
    """Парсер OBSVBASE. Пробуем оба формата (binary/ASCII) при неудаче; при формате FE 7E — попытка расшифровки raw. Результат в формате доки (HEADER, OBSERVATION_SUMMARY, OBSERVATIONS)."""
    result = _parse_obsv_message(
        data,
        binary,
        message_id=284,
        ascii_prefix="#OBSVBASEA",
        adr_std_divisor=1000.0,
    )
    # Если запрос был ASCII — пробуем бинарный разбор ответа
    if result is None and not binary:
        result = _parse_obsv_message(
            data,
            True,
            message_id=284,
            ascii_prefix="#OBSVBASEA",
            adr_std_divisor=1000.0,
        )
    # Если запрос был binary — пробуем ASCII разбор (устройство может ответить ASCII)
    if result is None and binary:
        result = _parse_obsv_message(
            data,
            False,
            message_id=284,
            ascii_prefix="#OBSVBASEA",
            adr_std_divisor=1000.0,
        )
    if result is None:
        result = _decode_raw_fe7e_format(data)
    if result is None:
        return None
    return _obsvbase_enrich_result(result)


def _check_obsv_complete(
    data: bytes,
    is_binary: bool,
    message_id: int,
    ascii_marker: bytes,
    *,
    fallback_len: int = 5000,
) -> bool:
    """Общий проверка полноты OBSV-сообщения (OBSVM/OBSVH/OBSVBASE)."""
    if is_binary:
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != message_id:
                    continue
                msg_length = header.message_length
                if msg_length > 0 and len(data) >= i + msg_length:
                    return True
                if len(data) >= i + 28:
                    obs_num = struct.unpack("<I", data[i + 24 : i + 28])[0]
                    if obs_num > 0:
                        return True
                    # Ноль наблюдений: без этого check_complete ждёт fallback_len и query «висит».
                    if obs_num == 0 and len(data) >= i + 32:
                        return True
        return len(data) > fallback_len
    try:
        if ascii_marker in data:
            return True
        if data.decode("ascii", errors="ignore").find(ascii_marker.decode("ascii")) >= 0:
            return True
    except Exception:
        pass
    return len(data) > fallback_len


def _check_obsvm_complete(data: bytes, is_binary: bool) -> bool:
    return _check_obsv_complete(data, is_binary, 12, b"#OBSVMA")


def _check_obsvh_complete(data: bytes, is_binary: bool) -> bool:
    return _check_obsv_complete(data, is_binary, 13, b"#OBSVHA")


def _check_obsvbase_complete(data: bytes, is_binary: bool) -> bool:
    return _check_obsv_complete(data, is_binary, 284, b"#OBSVBASEA")


def _check_obsvmcmp_complete(data: bytes, is_binary: bool) -> bool:
    if not is_binary and OBSVMCMP_ASCII_MESSAGE_RE.search(data):
        return True
    return _check_obsv_complete(data, is_binary, OBSVMCMP_MESSAGE_ID, b"#OBSVMCMPA")


def _decode_obsvmcmp_record(data: bytes) -> Optional[ObsvmcmpRecord]:
    """
    Декодирование одной сжатой записи OBSVMCMP (24 байта) по битовой раскладке спецификации.

    BIT_LAYOUT (bit 0 = LSB первого байта, little-endian):
    - bits 0-31   : channel_tracking_status
    - bits 32-59  : doppler, scale 1/256 Hz (signed)
    - bits 60-95  : pseudorange, scale 1/128 m
    - bits 96-127 : adr, scale 1/256 cycles (signed)
    - bits 128-131: psr_std_index -> PSR_STD_TABLE
    - bits 132-135: adr_std_index -> (n+1)/512 cycles
    - bits 136-143: prn
    - bits 144-164: lock_time, scale 1/32 s
    - bits 165-169: cn0 = 20 + n dB-Hz
    - bits 170-175: glonass_frequency_number (N+7)
    - bits 176-191: reserved
    """
    if len(data) < OBSVMCMP_RECORD_SIZE:
        return None
    try:
        ch_tr = _obsvmcmp_get_bits(data, 0, 32)
        doppler_raw = _obsvmcmp_get_bits_signed(data, 32, 28)
        doppler_hz = doppler_raw / 256.0

        pseudorange_raw = _obsvmcmp_get_bits(data, 60, 36)
        pseudorange_m = pseudorange_raw / 128.0

        adr_raw = _obsvmcmp_get_bits_signed(data, 96, 32)
        adr_cycles = adr_raw / 256.0

        psr_std_index = _obsvmcmp_get_bits(data, 128, 4)
        psr_std_m = PSR_STD_TABLE[psr_std_index] if 0 <= psr_std_index < len(PSR_STD_TABLE) else 0.0

        adr_std_index = _obsvmcmp_get_bits(data, 132, 4)
        adr_std_cycles = (adr_std_index + 1) / 512.0

        prn = _obsvmcmp_get_bits(data, 136, 8)
        lock_time_raw = _obsvmcmp_get_bits(data, 144, 21)
        lock_time_s = lock_time_raw / 32.0

        cn0_n = _obsvmcmp_get_bits(data, 165, 5)
        cn0_dbhz = 20.0 + cn0_n

        glonass_n = _obsvmcmp_get_bits(data, 170, 6)
        glonass_frequency_number = glonass_n + 7

        reserved = _obsvmcmp_get_bits(data, 176, 16)

        return ObsvmcmpRecord(
            channel_tracking_status=ch_tr,
            doppler_hz=doppler_hz,
            pseudorange_m=pseudorange_m,
            adr_cycles=adr_cycles,
            psr_std_index=psr_std_index,
            psr_std_m=psr_std_m,
            adr_std_cycles=adr_std_cycles,
            prn=prn,
            lock_time_s=lock_time_s,
            cn0_dbhz=cn0_dbhz,
            glonass_frequency_number=glonass_frequency_number,
            reserved=reserved,
            raw_hex=data.hex(),
        )
    except (IndexError, struct.error):
        return None


def _obsvmcmp_record_entry(
    compressed_data: bytes,
    index: int,
    *,
    raw_hex_override: Optional[str] = None,
) -> Dict[str, Any]:
    """Собрать один элемент compressed_records для OBSVMCMP (binary/ascii)."""
    record = _decode_obsvmcmp_record(compressed_data)
    hex_str = raw_hex_override if raw_hex_override is not None else compressed_data.hex()
    decoded = record.to_dict() if record else {"raw_hex": hex_str, "decode_error": "parse failed"}
    nav = decoded.get("nav_system") if isinstance(decoded, dict) else None
    out: Dict[str, Any] = {
        "index": index,
        "raw_hex": hex_str,
        "raw_bytes": compressed_data,
        "decoded": decoded,
        "record": record,
    }
    if nav:
        out["nav_system"] = nav
    return out


def _parse_obsvmcmp_ascii_block(block: bytes) -> Optional[dict]:
    """
    Разбор ASCII OBSVMCMP: первая строка до «;» — заголовок, далее до «*CRC» — число наблюдений и
    записи по 48 hex-символов (24 байта), между ними запятые и переводы строк (§7.3.4).
    """
    block = block.strip()
    if not re.match(rb"#OBSVMCMPA", block, flags=re.I):
        return None
    if b";" not in block:
        return None
    _head, rest = block.split(b";", 1)
    rest = rest.strip()
    if b"*" not in rest:
        return None
    data_only, _crc = rest.rsplit(b"*", 1)
    data_only = data_only.replace(b"\r", b"").replace(b"\n", b"")
    raw_tokens = [t.strip() for t in data_only.split(b",")]
    parts = [t for t in raw_tokens if t]
    if not parts:
        return None
    try:
        obs_number = int(parts[0])
    except ValueError:
        return None
    compressed_records: List[Dict[str, Any]] = []
    hex_idx = 0
    for tok in parts[1:]:
        if len(tok) < 48:
            continue
        hx = tok[:48]
        if not re.match(br"[0-9a-fA-F]{48}", hx):
            continue
        try:
            record_bytes = bytes.fromhex(hx.decode("ascii"))
        except ValueError:
            continue
        compressed_records.append(
            _obsvmcmp_record_entry(record_bytes, hex_idx, raw_hex_override=hx.decode("ascii"))
        )
        hex_idx += 1
        if hex_idx >= obs_number:
            break
    return {
        "format": "ascii",
        "obs_number": obs_number,
        "parsed_records": len(compressed_records),
        "compressed_records": compressed_records,
        "note": "OBSVMCMP 24-byte compressed records per spec (Message ID 138)",
    }


def _parse_obsvmcmp_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header is None:
                    continue
                msg_length = header.message_length
                if msg_length < 32 or len(data) < i + msg_length:
                    continue
                offset = i + 24
                if len(data) < offset + 4:
                    continue
                obs_number = struct.unpack("<I", data[offset : offset + 4])[0]
                offset += 4
                max_records_by_len = max(0, (msg_length - 24 - 4 - 4) // 24)
                if obs_number > 0 and max_records_by_len == 0:
                    continue
                records_to_parse = min(obs_number, max_records_by_len)
                compressed_records = []
                for obs_idx in range(records_to_parse):
                    if offset + 24 > i + msg_length - 4:
                        break
                    compressed_data = data[offset : offset + 24]
                    offset += 24
                    compressed_records.append(_obsvmcmp_record_entry(compressed_data, obs_idx))
                crc_offset = i + msg_length - 4
                crc_value = None
                if len(data) >= crc_offset + 4:
                    crc_value = struct.unpack("<I", data[crc_offset : crc_offset + 4])[0]
                return {
                    "format": "binary",
                    "obs_number": obs_number,
                    "parsed_records": len(compressed_records),
                    "compressed_records": compressed_records,
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                    "note": "OBSVMCMP 24-byte compressed records per spec (Message ID 138)",
                }
        return None
    else:
        try:
            m = OBSVMCMP_ASCII_MESSAGE_RE.search(data)
            if m:
                out = _parse_obsvmcmp_ascii_block(m.group(0))
                if out:
                    out["raw"] = m.group(0).decode("ascii", errors="replace")
                    return out
            text = data.decode("ascii", errors="ignore")
            pattern = r"#OBSVMCMPA[^\r\n]*\*[0-9a-fA-F]{8}"
            for match in re.finditer(pattern, text):
                line = match.group(0).strip()
                if not line:
                    continue
                out = _parse_obsvmcmp_ascii_block(line.encode("ascii", errors="surrogateescape"))
                if out:
                    out["raw"] = line
                    return out
        except Exception:
            pass
    return None


def _query_obsv_rate(
    core: Um982Core,
    port: Optional[str],
    rate: Union[int, float],
    binary: bool,
    add_crlf: Optional[bool],
    cmd_b: str,
    cmd_a: str,
    parse_func: Any,
    check_complete: Any,
    result_key: str,
) -> Dict[str, Any]:
    """Общий раннер для OBSVM/OBSVH (команда с портом и rate).

    Если port пустой/None — в команду не подставляется COM (как UNLOG без порта, AGRICA без порта):
    вывод идёт на «текущий» порт сессии, без явного COM1/COM2/COM3.
    """
    p = (port or "").strip()
    rw = format_log_period_wire(float(rate))
    if p:
        command = f"{cmd_b} {p} {rw}" if binary else f"{cmd_a} {p} {rw}"
    else:
        command = f"{cmd_b} {rw}" if binary else f"{cmd_a} {rw}"
    return _run_data_query(
        core,
        command=command,
        parse_func=parse_func,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.0,
        read_attempts=40,
        first_read_timeout=0.38,
        read_timeout=0.14,
        max_wait=12.0,
        check_complete=check_complete,
        result_key=result_key,
    )


def query_obsvm(
    core: Um982Core,
    port: Optional[str] = None,
    rate: Union[int, float] = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    return _query_obsv_rate(
        core, port, rate, binary, add_crlf,
        "OBSVMB", "OBSVMA",
        _parse_obsvm_message, _check_obsvm_complete, "obsvm",
    )


def query_obsvh(
    core: Um982Core,
    port: Optional[str] = None,
    rate: Union[int, float] = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    return _query_obsv_rate(
        core, port, rate, binary, add_crlf,
        "OBSVHB", "OBSVHA",
        _parse_obsvh_message, _check_obsvh_complete, "obsvh",
    )


def query_obsvmcmp(
    core: Um982Core,
    port: Optional[str] = None,
    rate: Union[int, float] = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    p = (port or "").strip()
    rw = format_log_period_wire(float(rate))
    if p:
        command = f"OBSVMCMPB {p} {rw}" if binary else f"OBSVMCMPA {p} {rw}"
    else:
        command = f"OBSVMCMPB {rw}" if binary else f"OBSVMCMPA {rw}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_obsvmcmp_message,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.0,
        read_attempts=48,
        first_read_timeout=0.38,
        read_timeout=0.14,
        max_wait=12.0,
        check_complete=_check_obsvmcmp_complete,
        result_key="obsvmcmp",
    )


# Типы для потокового чтения: (cmd_b, cmd_a, message_id, ascii_marker)
_STREAM_CONFIG = {
    "obsvm": ("OBSVMB", "OBSVMA", 12, b"#OBSVMA"),
    "obsvh": ("OBSVHB", "OBSVHA", 13, b"#OBSVHA"),
    "obsvmcmp": ("OBSVMCMPB", "OBSVMCMPA", OBSVMCMP_MESSAGE_ID, b"#OBSVMCMPA"),
}


def send_obsv_stream_command(
    core: Um982Core,
    stream_type: str,
    port: Optional[str] = None,
    rate: Union[int, float] = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> bool:
    """Отправить команду вывода OBSVM/OBSVH/OBSVMCMP один раз. Устройство затем шлёт ответы по порту."""
    if stream_type not in _STREAM_CONFIG:
        return False
    if add_crlf is None:
        add_crlf = core.baudrate >= 460800
    cmd_b, cmd_a, _mid, _marker = _STREAM_CONFIG[stream_type]
    p = (port or "").strip()
    rw = format_log_period_wire(float(rate))
    if p:
        command = f"{cmd_b} {p} {rw}" if binary else f"{cmd_a} {p} {rw}"
    else:
        command = f"{cmd_b} {rw}" if binary else f"{cmd_a} {rw}"
    if core.serial_conn and core.serial_conn.in_waiting > 0:
        core.serial_conn.reset_input_buffer()
    return bool(core.send_ascii_command(command, add_crlf=add_crlf))


def _obsv_binary_frame_length_candidates(
    buffer: bytes,
    offset: int,
    *,
    rec_bytes: int,
    header_ml: int,
) -> List[int]:
    """Длины полного бинарного кадра OBSV* от sync (учитываем битые message_length и 0 наблюдений)."""
    seen: set[int] = set()
    out: List[int] = []
    for L in (header_ml, header_ml + 4, header_ml - 4):
        if L >= 28 and L not in seen:
            seen.add(L)
            out.append(L)
    if len(buffer) >= offset + 28:
        try:
            obs_n = struct.unpack("<I", buffer[offset + 24 : offset + 28])[0]
        except Exception:
            obs_n = None
        if obs_n is not None and obs_n <= 2048:
            computed = 24 + 4 + rec_bytes * obs_n + 4
            for L in (computed, computed - 4):
                if L >= 28 and L not in seen:
                    seen.add(L)
                    out.append(L)
    return out


def extract_one_obsv_message(
    buffer: bytes,
    stream_type: str,
    binary: bool,
) -> Tuple[Optional[dict], bytes]:
    """
    Извлечь из буфера одно полное сообщение OBSVM/OBSVH/OBSVMCMP.
    Возвращает (parsed_data в формате result[obsvm/obsvh/obsvmcmp], оставшийся_буфер).
    Если полного сообщения нет — (None, buffer).
    """
    if stream_type not in _STREAM_CONFIG:
        return None, buffer
    _cmd_b, _cmd_a, message_id, ascii_marker = _STREAM_CONFIG[stream_type]
    parsers = {
        "obsvm": _parse_obsvm_message,
        "obsvh": _parse_obsvh_message,
        "obsvmcmp": _parse_obsvmcmp_message,
    }
    parse_func = parsers.get(stream_type)
    if not parse_func:
        return None, buffer

    if binary:
        found = find_unicore_sync(buffer)
        if not found:
            return None, buffer
        offset, header = found
        if header.message_id != message_id:
            return None, buffer
        rec_sz = 24 if stream_type == "obsvmcmp" else 40
        ml = int(header.message_length)
        want_lens = _obsv_binary_frame_length_candidates(buffer, offset, rec_bytes=rec_sz, header_ml=ml)
        for msg_len in want_lens:
            if msg_len < 28:
                continue
            if len(buffer) < offset + msg_len:
                continue
            slice_msg = buffer[offset : offset + msg_len]
            parsed = parse_func(slice_msg, True)
            if parsed is not None:
                return parsed, buffer[offset + msg_len :]
        return None, buffer
    else:
        if stream_type == "obsvmcmp":
            m = OBSVMCMP_ASCII_MESSAGE_RE.search(buffer)
            if not m:
                return None, buffer
            frag = m.group(0)
            parsed = parse_func(frag, False)
            if parsed is None:
                return None, buffer
            end = m.end()
            while end < len(buffer) and buffer[end] in (13, 10):
                end += 1
            return parsed, buffer[end:]
        pos = buffer.find(ascii_marker)
        if pos < 0:
            return None, buffer
        end = buffer.find(b"\r", pos)
        if end < 0:
            end = buffer.find(b"\n", pos)
        if end < 0:
            return None, buffer
        line = buffer[pos : end]
        parsed = parse_func(line, False)
        if parsed is None:
            return None, buffer
        return parsed, buffer[end:]


def query_obsvbase(
    core: Um982Core,
    port: Optional[str] = None,
    trigger: str = "ONCHANGED",
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    if trigger.upper() != "ONCHANGED":
        return {"error": f"Invalid trigger: {trigger}. Only 'ONCHANGED' is supported for OBSVBASE"}

    tr = trigger.upper()
    p = (port or "").strip()
    if p:
        command = f"OBSVBASEB {p} {tr}" if binary else f"OBSVBASEA {p} {tr}"
    else:
        command = f"OBSVBASEB {tr}" if binary else f"OBSVBASEA {tr}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_obsvbase_message,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.0,
        read_attempts=48,
        first_read_timeout=0.38,
        read_timeout=0.14,
        max_wait=12.0,
        check_complete=_check_obsvbase_complete,
        result_key="obsvbase",
    )


# --- OBSVMCMP: assumptions and example ---
#
# Assumptions (per doc):
# - Bit order: bit 0 = LSB of first byte (little-endian bit numbering).
# - Multi-byte fields: same byte order as in buffer (LE).
# - Doppler (28 bits) and ADR (32 bits) are sign-extended when top bit is set.

if __name__ == "__main__":
    # Unit test: decode one 24-byte record (all zeros except prn=6, cn0_index=5 -> 25 dB-Hz)
    _r = bytes(24)
    _r = bytearray(_r)
    _r[17] = 6
    _r[20] = (5 << 5) & 0xFF
    _rec = _decode_obsvmcmp_record(bytes(_r))
    assert _rec is not None
    assert _rec.prn == 6
    assert _rec.cn0_dbhz == 25.0
    assert _rec.psr_std_m == PSR_STD_TABLE[0]
    print("OBSVMCMP record decode: OK")
