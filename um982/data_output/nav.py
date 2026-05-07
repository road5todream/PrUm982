"""Навигационные решения, DOP, XYZ: BESTNAV, ADRNAV, PPPNAV, SPPNAV, ADRNAVH, SPPNAVH, STADOP, ADRDOP (7.3.36), BESTNAVXYZ, DOP ведомой антенны (2121)."""
import math
import re
import struct
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from um982.core import Um982Core
from um982.utils import parse_unicore_header

from .base import _run_data_query, _make_unicore_header_checker
from .nav_enums import datum_id_to_str, position_velocity_type_to_str, solution_status_to_str

# --- BESTNAV §7.3.24: расшифровка ext sol stat и масок сигналов ---
_GAL_BDS3_MASK_BITS: Tuple[Tuple[int, str], ...] = (
    (0, "Galileo E1"),
    (1, "Galileo E5b"),
    (2, "Galileo E5a"),
    (4, "BDS-3 B1I"),
    (5, "BDS-3 B3I"),
    (6, "BDS-3 B2a"),
    (7, "BDS-3 B1C"),
)
_GPS_GLO_BDS2_MASK_BITS: Tuple[Tuple[int, str], ...] = (
    (0, "GPS L1"),
    (1, "GPS L2"),
    (2, "GPS L5"),
    (3, "BDS-2 B3I"),
    (4, "GLONASS L1"),
    (5, "GLONASS L2"),
    (6, "BDS-2 B1I"),
    (7, "BDS-2 B2I"),
)


def _decode_bestnav_ext_sol_stat(ext_sol_stat: int) -> Dict[str, Any]:
    b = int(ext_sol_stat) & 0xFF
    iono = (b >> 1) & 7
    return {
        "ext_sol_rtk_verified": bool(b & 1),
        "ext_sol_rtk_verification": "checked" if (b & 1) else "unchecked",
        "ionospheric_correction_type": iono,
    }


def _bestnav_mask_active_signals(mask: int, pairs: Tuple[Tuple[int, str], ...]) -> List[str]:
    m = int(mask) & 0xFF
    return [desc for bit, desc in pairs if m & (1 << bit)]


def _bestnav_decode_signal_extensions(ext_sol_stat: int, gal_bds3_mask: int, gps_glo_bds2_mask: int) -> Dict[str, Any]:
    gal = _bestnav_mask_active_signals(gal_bds3_mask, _GAL_BDS3_MASK_BITS)
    gps = _bestnav_mask_active_signals(gps_glo_bds2_mask, _GPS_GLO_BDS2_MASK_BITS)
    out = {
        **_decode_bestnav_ext_sol_stat(ext_sol_stat),
        "gal_bds3_signals": gal,
        "gps_glo_bds2_signals": gps,
        "gal_bds3_signals_text": ", ".join(gal) if gal else "—",
        "gps_glo_bds2_signals_text": ", ".join(gps) if gps else "—",
    }
    return out


def _parse_bestnav_ascii_hex_byte(token: str) -> int:
    """Байт в ASCII (рук. Hex): 0x.., или 1–2 шестнадцатеричные цифры."""
    s = token.strip().strip('"')
    if not s:
        return 0
    if s.lower().startswith("0x"):
        return int(s, 0) & 0xFF
    if re.fullmatch(r"[0-9A-Fa-f]{1,2}", s):
        return int(s, 16) & 0xFF
    try:
        return int(s, 10) & 0xFF
    except ValueError:
        return 0


def _parse_bestnav_ascii_float(token: str) -> float:
    """Float; 8 hex-символов без точки — часто IEEE754 little-endian (как у horspd std в примерах)."""
    s = token.strip().strip('"')
    if not s:
        return 0.0
    if re.fullmatch(r"[0-9A-Fa-f]{8}", s):
        try:
            w = int(s, 16)
            return float(struct.unpack("<f", struct.pack("<I", w & 0xFFFFFFFF))[0])
        except (struct.error, OverflowError, ValueError):
            return 0.0
    try:
        return float(s)
    except ValueError:
        try:
            return float(int(s, 0))
        except ValueError:
            return 0.0


def _bestnav_ascii_header_prefix(text: str) -> Tuple[int, str]:
    """Возвращает (позиция начала, префикс) для #BESTNAVA, / #BESTNAVB,."""
    for tag in ("#BESTNAVA,", "#BESTNAVB,"):
        p = text.find(tag)
        if p >= 0:
            return p, tag
    return -1, ""


# --- BESTNAVXYZ §7.3.25 (message ID 240): 112 байт полей, +4 байта CRC сообщения в теле (H+112…). ---
BESTNAVXYZ_MESSAGE_ID = 240
_BESTNAVXYZ_BODY_FIELDS = 112
_BESTNAVXYZ_BODY_WITH_INNER_CRC = 116
_BESTNAVXYZ_STRUCT = struct.Struct("<iidddfffiidddfff4sfffBBBBBBBB")

_BESTNAVXYZ_ASCII_COMPLETE_RE = re.compile(
    r"#BESTNAVXYZ[AB].*?\*[0-9a-fA-F]{8}",
    re.DOTALL | re.IGNORECASE,
)


def _bestnavxyz_binary_total_len_candidates(message_length: int) -> List[int]:
    out: List[int] = []
    seen: set[int] = set()
    ml = int(message_length)
    if ml <= 0 or ml > 8192:
        return out

    def add(t: int) -> None:
        if 52 <= t <= 8192 and t not in seen:
            seen.add(t)
            out.append(t)

    add(ml)
    add(ml + 4)
    add(24 + _BESTNAVXYZ_BODY_FIELDS + 4)
    add(24 + _BESTNAVXYZ_BODY_WITH_INNER_CRC + 4)
    return out


def _bestnavxyz_body_from_frame(data: bytes, sync_off: int, total_len: int) -> Optional[bytes]:
    if total_len < 52 or len(data) < sync_off + total_len:
        return None
    end = sync_off + total_len - 4
    if end < sync_off + 24:
        return None
    return data[sync_off + 24 : end]


def _unpack_bestnavxyz_blob112(blob112: bytes) -> Optional[Dict[str, Any]]:
    """Разбор 112 байт полезной нагрузки BESTNAVXYZ (без внешнего CRC кадра)."""
    if len(blob112) < _BESTNAVXYZ_BODY_FIELDS:
        return None
    try:
        tup = _BESTNAVXYZ_STRUCT.unpack(blob112[:_BESTNAVXYZ_BODY_FIELDS])
    except struct.error:
        return None
    (
        p_sol_code,
        pos_code,
        p_x,
        p_y,
        p_z,
        p_xs,
        p_ys,
        p_zs,
        v_sol_code,
        vel_code,
        v_x,
        v_y,
        v_z,
        v_xs,
        v_ys,
        v_zs,
        stn_raw,
        v_latency,
        diff_age,
        sol_age,
        n_svs,
        n_soln,
        n_ggl1,
        n_multi,
        reserved_b,
        ext_b,
        gal_b,
        gps_b,
    ) = tup
    stn_b = stn_raw if isinstance(stn_raw, (bytes, bytearray)) else bytes(stn_raw)
    stn_id = stn_b.decode("ascii", errors="ignore").rstrip("\x00 \t")
    ext_b = int(ext_b) & 0xFF
    gal_b = int(gal_b) & 0xFF
    gps_b = int(gps_b) & 0xFF
    ext_dec = _bestnav_decode_signal_extensions(ext_b, gal_b, gps_b)
    out: Dict[str, Any] = {
        "position": {
            "P_sol_status": solution_status_to_str(int(p_sol_code)),
            "P_sol_status_code": int(p_sol_code),
            "pos_type": position_velocity_type_to_str(int(pos_code)),
            "pos_type_code": int(pos_code),
            "P_X": float(p_x),
            "P_Y": float(p_y),
            "P_Z": float(p_z),
            "P_X_sigma": float(p_xs),
            "P_Y_sigma": float(p_ys),
            "P_Z_sigma": float(p_zs),
        },
        "velocity": {
            "V_sol_status": solution_status_to_str(int(v_sol_code)),
            "V_sol_status_code": int(v_sol_code),
            "vel_type": position_velocity_type_to_str(int(vel_code)),
            "vel_type_code": int(vel_code),
            "V_X": float(v_x),
            "V_Y": float(v_y),
            "V_Z": float(v_z),
            "V_X_sigma": float(v_xs),
            "V_Y_sigma": float(v_ys),
            "V_Z_sigma": float(v_zs),
        },
        "metadata": {
            "station_id": stn_id,
            "V_latency": float(v_latency),
            "diff_age": float(diff_age),
            "sol_age": float(sol_age),
            "num_sats_tracked": int(n_svs) & 0xFF,
            "num_sats_used": int(n_soln) & 0xFF,
            "num_gg_l1": int(n_ggl1) & 0xFF,
            "num_soln_multi_svs": int(n_multi) & 0xFF,
            "reserved": int(reserved_b) & 0xFF,
        },
        "extended": {
            "ext_sol_stat": ext_b,
            "ext_sol_stat_hex": f"0x{ext_b:02X}",
            "gal_bds3_mask": gal_b,
            "gal_bds3_mask_hex": f"0x{gal_b:02X}",
            "gps_glo_bds2_mask": gps_b,
            "gps_glo_bds2_mask_hex": f"0x{gps_b:02X}",
            **ext_dec,
        },
    }
    return out


def _bestnavxyz_try_unpack_frame(
    data: bytes, sync_off: int, header_message_length: int
) -> Optional[Tuple[Dict[str, Any], int, int]]:
    for total in _bestnavxyz_binary_total_len_candidates(header_message_length):
        if len(data) < sync_off + total:
            continue
        body = _bestnavxyz_body_from_frame(data, sync_off, total)
        if not body:
            continue
        inner: Optional[int] = None
        if len(body) >= _BESTNAVXYZ_BODY_WITH_INNER_CRC:
            blob112 = body[:_BESTNAVXYZ_BODY_FIELDS]
            inner = struct.unpack("<I", body[_BESTNAVXYZ_BODY_FIELDS : _BESTNAVXYZ_BODY_WITH_INNER_CRC])[0]
        elif len(body) >= _BESTNAVXYZ_BODY_FIELDS:
            blob112 = body[:_BESTNAVXYZ_BODY_FIELDS]
        else:
            continue
        unpacked = _unpack_bestnavxyz_blob112(blob112)
        if unpacked is None:
            continue
        crc_val = struct.unpack("<I", data[sync_off + total - 4 : sync_off + total])[0]
        if inner is not None:
            unpacked = {**unpacked, "inner_crc": f"0x{inner:08X}"}
        return unpacked, total, crc_val
    return None


def _check_bestnavxyz_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        search_start = max(0, len(data) - 10000)
        for i in range(search_start, len(data) - 24):
            if data[i : i + 3] != bytes((0xAA, 0x44, 0xB5)):
                continue
            header = parse_unicore_header(data[i : i + 24])
            if not header or header.message_id != BESTNAVXYZ_MESSAGE_ID:
                continue
            trip = _bestnavxyz_try_unpack_frame(data, i, header.message_length)
            if trip is not None:
                return True
            cands = _bestnavxyz_binary_total_len_candidates(header.message_length)
            if cands:
                mx = max(cands)
                if len(data) < i + mx:
                    return False
        return len(data) > 500
    try:
        text = data.decode("ascii", errors="ignore")
        m = _BESTNAVXYZ_ASCII_COMPLETE_RE.search(text)
        if m and ";" in m.group(0):
            return True
    except Exception:
        pass
    return len(data) > 500


# --- Общие структуры: NavSolution, DopValues, NavXYZ ---

@dataclass
class NavSolution:
    """Позиция/решение навигации (lat, lon, hgt или обобщённые поля из BESTNAV/ADRNAV/SPPNAV/PPPNAV)."""
    sol_status: Any = None
    pos_type: Any = None
    lat: float = 0.0
    lon: float = 0.0
    hgt: float = 0.0
    undulation: float = 0.0
    datum_id: str = ""
    lat_std: float = 0.0
    lon_std: float = 0.0
    hgt_std: float = 0.0
    stn_id: str = ""
    diff_age: float = 0.0
    sol_age: float = 0.0
    num_svs: int = 0
    num_soln_svs: int = 0
    raw_position: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sol_status": self.sol_status,
            "pos_type": self.pos_type,
            "lat": self.lat,
            "lon": self.lon,
            "hgt": self.hgt,
            "undulation": self.undulation,
            "datum_id": self.datum_id,
            "lat_std": self.lat_std,
            "lon_std": self.lon_std,
            "hgt_std": self.hgt_std,
            "stn_id": self.stn_id,
            "diff_age": self.diff_age,
            "sol_age": self.sol_age,
            "num_svs": self.num_svs,
            "num_soln_svs": self.num_soln_svs,
            **self.raw_position,
        }

    @classmethod
    def from_parsed(cls, d: Dict[str, Any]) -> "NavSolution":
        pos = d.get("position") or {}
        return cls(
            sol_status=pos.get("sol_status"),
            pos_type=pos.get("pos_type"),
            lat=float(pos.get("lat", 0) or 0),
            lon=float(pos.get("lon", 0) or 0),
            hgt=float(pos.get("hgt", 0) or 0),
            undulation=float(pos.get("undulation", 0) or 0),
            datum_id=str(pos.get("datum_id", "") or ""),
            lat_std=float(pos.get("lat_std", pos.get("lat_sigma", 0)) or 0),
            lon_std=float(pos.get("lon_std", pos.get("lon_sigma", 0)) or 0),
            hgt_std=float(pos.get("hgt_std", pos.get("hgt_sigma", 0)) or 0),
            stn_id=str(pos.get("stn_id", pos.get("station_id", "")) or ""),
            diff_age=float(pos.get("diff_age", 0) or 0),
            sol_age=float(pos.get("sol_age", 0) or 0),
            num_svs=int(pos.get("num_svs", pos.get("num_sats_used", 0)) or 0),
            num_soln_svs=int(pos.get("num_soln_svs", pos.get("num_sats_used", 0)) or 0),
            raw_position=dict(pos),
        )


@dataclass
class DopValues:
    """DOP и связанные поля (STADOP/ADRDOP)."""
    time_of_week: float = 0.0
    gdop: float = 0.0
    pdop: float = 0.0
    tdop: float = 0.0
    vdop: float = 0.0
    hdop: float = 0.0
    ndop: float = 0.0
    edop: float = 0.0
    cutoff_angle: float = 0.0
    reserved: float = 0.0
    num_satellites: int = 0
    prn_list: List[int] = field(default_factory=list)
    raw: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "time_of_week": self.time_of_week,
            "gdop": self.gdop,
            "pdop": self.pdop,
            "tdop": self.tdop,
            "vdop": self.vdop,
            "hdop": self.hdop,
            "ndop": self.ndop,
            "edop": self.edop,
            "cutoff_angle": self.cutoff_angle,
            "reserved": self.reserved,
            "num_satellites": self.num_satellites,
            "prn_list": list(self.prn_list),
            "raw": self.raw,
        }

    @classmethod
    def from_parsed(cls, d: Dict[str, Any]) -> "DopValues":
        return cls(
            time_of_week=float(d.get("time_of_week", 0) or 0),
            gdop=float(d.get("gdop", 0) or 0),
            pdop=float(d.get("pdop", 0) or 0),
            tdop=float(d.get("tdop", 0) or 0),
            vdop=float(d.get("vdop", 0) or 0),
            hdop=float(d.get("hdop", 0) or 0),
            ndop=float(d.get("ndop", 0) or 0),
            edop=float(d.get("edop", 0) or 0),
            cutoff_angle=float(d.get("cutoff_angle", 0) or 0),
            reserved=float(d.get("reserved", 0) or 0),
            num_satellites=int(d.get("num_satellites", 0) or 0),
            prn_list=list(d.get("prn_list", []) or []),
            raw=d.get("raw"),
        )


@dataclass
class NavXYZ:
    """Позиция/скорость в ECEF (BESTNAVXYZ)."""
    p_sol_status: Any = None
    pos_type: Any = None
    P_X: float = 0.0
    P_Y: float = 0.0
    P_Z: float = 0.0
    P_X_sigma: float = 0.0
    P_Y_sigma: float = 0.0
    P_Z_sigma: float = 0.0
    V_sol_status: Any = None
    vel_type: Any = None
    V_X: float = 0.0
    V_Y: float = 0.0
    V_Z: float = 0.0
    V_X_sigma: float = 0.0
    V_Y_sigma: float = 0.0
    V_Z_sigma: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "position": {
                "P_sol_status": self.p_sol_status,
                "pos_type": self.pos_type,
                "P_X": self.P_X,
                "P_Y": self.P_Y,
                "P_Z": self.P_Z,
                "P_X_sigma": self.P_X_sigma,
                "P_Y_sigma": self.P_Y_sigma,
                "P_Z_sigma": self.P_Z_sigma,
            },
            "velocity": {
                "V_sol_status": self.V_sol_status,
                "vel_type": self.vel_type,
                "V_X": self.V_X,
                "V_Y": self.V_Y,
                "V_Z": self.V_Z,
                "V_X_sigma": self.V_X_sigma,
                "V_Y_sigma": self.V_Y_sigma,
                "V_Z_sigma": self.V_Z_sigma,
            },
        }

    @classmethod
    def from_parsed(cls, d: Dict[str, Any]) -> "NavXYZ":
        pos = d.get("position") or {}
        vel = d.get("velocity") or {}
        return cls(
            p_sol_status=pos.get("P_sol_status"),
            pos_type=pos.get("pos_type"),
            P_X=float(pos.get("P_X", 0) or 0),
            P_Y=float(pos.get("P_Y", 0) or 0),
            P_Z=float(pos.get("P_Z", 0) or 0),
            P_X_sigma=float(pos.get("P_X_sigma", 0) or 0),
            P_Y_sigma=float(pos.get("P_Y_sigma", 0) or 0),
            P_Z_sigma=float(pos.get("P_Z_sigma", 0) or 0),
            V_sol_status=vel.get("V_sol_status"),
            vel_type=vel.get("vel_type"),
            V_X=float(vel.get("V_X", 0) or 0),
            V_Y=float(vel.get("V_Y", 0) or 0),
            V_Z=float(vel.get("V_Z", 0) or 0),
            V_X_sigma=float(vel.get("V_X_sigma", 0) or 0),
            V_Y_sigma=float(vel.get("V_Y_sigma", 0) or 0),
            V_Z_sigma=float(vel.get("V_Z_sigma", 0) or 0),
        )


# --- NAV: BESTNAV / ADRNAV / PPPNAV / SPPNAV / STADOP / ADRDOP / BESTNAVXYZ (из _commands) ---

# --- NAV: BESTNAV / ADRNAV ---


def _check_bestnav_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        search_start = max(0, len(data) - 10000)
        for i in range(search_start, len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 2118:
                    msg_length = header.message_length
                    if len(data) >= i + msg_length:
                        return True
        return len(data) > 5000
    try:
        text = data.decode("ascii", errors="ignore")
        pos, _tag = _bestnav_ascii_header_prefix(text)
        if pos >= 0:
            end_pos = min(len(text), pos + 2000)
            if "*" in text[pos:end_pos]:
                semicolon_pos = text.find(";", pos)
                if pos < semicolon_pos < end_pos:
                    return True
    except Exception:
        pass
    return len(data) > 3000


def _check_adrnav_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(max(0, len(data) - 4000), len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 142:
                    msg_len = header.message_length
                    if msg_len > 0 and len(data) >= i + msg_len:
                        return True
        return len(data) > 640
    try:
        text = data.decode("ascii", errors="ignore")
        if "#ADRNAVA" in text:
            return True
    except Exception:
        pass
    return len(data) > 200


def _unpack_nav_binary_position72(data: bytes, offset: int) -> Optional[Dict[str, Any]]:
    """
    Первые 72 байта полезной нагрузки H..H+71 (little-endian): общие для BESTNAV/ADRNAV
    и полного кадра PPPNAV (разд. 7.3.31, msg_id 1026 — без блока скорости).
    """
    if len(data) < offset + 72:
        return None
    o = offset

    p_sol_status = struct.unpack("<i", data[o : o + 4])[0]
    o += 4
    pos_type = struct.unpack("<i", data[o : o + 4])[0]
    o += 4
    lat = struct.unpack("<d", data[o : o + 8])[0]
    o += 8
    lon = struct.unpack("<d", data[o : o + 8])[0]
    o += 8
    hgt = struct.unpack("<d", data[o : o + 8])[0]
    o += 8
    undulation = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    datum_id = struct.unpack("<i", data[o : o + 4])[0]
    o += 4
    lat_std = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    lon_std = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    hgt_std = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    stn_id = data[o : o + 4].decode("ascii", errors="ignore").rstrip("\x00")
    o += 4
    diff_age = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    sol_age = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    num_svs = struct.unpack("<B", data[o : o + 1])[0]
    o += 1
    num_soln_svs = struct.unpack("<B", data[o : o + 1])[0]
    o += 1
    reserved1 = struct.unpack("<B", data[o : o + 1])[0]
    o += 1
    reserved2 = struct.unpack("<B", data[o : o + 1])[0]
    o += 1
    reserved3 = struct.unpack("<B", data[o : o + 1])[0]
    o += 1
    ext_sol_stat = struct.unpack("<B", data[o : o + 1])[0]
    o += 1
    gal_bds3_mask = struct.unpack("<B", data[o : o + 1])[0]
    o += 1
    gps_glo_bds2_mask = struct.unpack("<B", data[o : o + 1])[0]
    o += 1

    ext_dec = _bestnav_decode_signal_extensions(ext_sol_stat, gal_bds3_mask, gps_glo_bds2_mask)

    return {
        "position": {
            "sol_status": solution_status_to_str(p_sol_status),
            "pos_type": position_velocity_type_to_str(pos_type),
            "p_sol_status_code": int(p_sol_status),
            "pos_type_code": int(pos_type),
            "lat": lat,
            "lon": lon,
            "hgt": hgt,
            "undulation": undulation,
            "datum_id": datum_id_to_str(datum_id),
            "datum_id_code": int(datum_id),
            "lat_std": lat_std,
            "lon_std": lon_std,
            "hgt_std": hgt_std,
            "stn_id": stn_id,
            "diff_age": diff_age,
            "sol_age": sol_age,
            "num_svs": num_svs,
            "num_soln_svs": num_soln_svs,
        },
        "extended": {
            "ext_sol_stat": ext_sol_stat,
            "ext_sol_stat_hex": f"0x{ext_sol_stat:02X}",
            "gal_bds3_mask": gal_bds3_mask,
            "gal_bds3_mask_hex": f"0x{gal_bds3_mask:02X}",
            "gps_glo_bds2_mask": gps_glo_bds2_mask,
            "gps_glo_bds2_mask_hex": f"0x{gps_glo_bds2_mask:02X}",
            **ext_dec,
        },
        "reserved": {
            "reserved1": reserved1,
            "reserved2": reserved2,
            "reserved3": reserved3,
        },
    }


def _unpack_nav_binary_solution_120(data: bytes, offset: int) -> Optional[Dict[str, Any]]:
    """
    120 байт полезной нагрузки BESTNAV / SPPNAV (§7.3.32) в binary (little-endian), включая horspd_std в конце.
    Общая часть H..H+71; блок скорости с H+72. У ADRNAV — `_unpack_adrnav_binary_solution` (116 байт, без horspd_std).
    """
    if len(data) < offset + 120:
        return None
    base = _unpack_nav_binary_position72(data, offset)
    if base is None:
        return None
    o = offset + 72

    v_sol_status = struct.unpack("<i", data[o : o + 4])[0]
    o += 4
    vel_type = struct.unpack("<i", data[o : o + 4])[0]
    o += 4
    latency = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    age = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    hor_spd = struct.unpack("<d", data[o : o + 8])[0]
    o += 8
    trk_gnd = struct.unpack("<d", data[o : o + 8])[0]
    o += 8
    vert_spd = struct.unpack("<d", data[o : o + 8])[0]
    o += 8
    versp_std = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    horspd_std = struct.unpack("<f", data[o : o + 4])[0]
    o += 4

    return {
        **base,
        "velocity": {
            "sol_status": solution_status_to_str(v_sol_status),
            "vel_type": position_velocity_type_to_str(vel_type),
            "v_sol_status_code": int(v_sol_status),
            "vel_type_code": int(vel_type),
            "latency": latency,
            "age": age,
            "hor_spd": hor_spd,
            "trk_gnd": trk_gnd,
            "vert_spd": vert_spd,
            "versp_std": versp_std,
            "horspd_std": horspd_std,
        },
    }


# §7.3.31 PPPNAV binary: после заголовка — 72 байта полей (H..H+71), CRC32 на H+72..H+75.
_PPPNAV_BINARY_PAYLOAD_LEN = 72
_PPPNAV_BINARY_AFTER_HEADER = 76

# §7.3.32 SPPNAV / SPPNAVH binary: как BESTNAV — 120 байт полей (H..H+119), CRC на H+120..H+123.
_SPPNAV_BINARY_PAYLOAD_LEN = 120
_SPPNAV_BINARY_AFTER_HEADER = 124

# §7.3.29 ADRNAV / ADRNAVH binary: после заголовка 24 байта — 116 байт полей (H..H+115), далее CRC.
# Поля 20–22 на H+69..71 совпадают с концом `_unpack_nav_binary_position72`; скорость с H+72 без horspd_std.
_ADRNAV_BINARY_PAYLOAD_LEN = 116


def _unpack_adrnav_binary_solution(data: bytes, offset: int) -> Optional[Dict[str, Any]]:
    if len(data) < offset + _ADRNAV_BINARY_PAYLOAD_LEN:
        return None
    base = _unpack_nav_binary_position72(data, offset)
    if base is None:
        return None
    o = offset + 72

    v_sol_status = struct.unpack("<i", data[o : o + 4])[0]
    o += 4
    vel_type = struct.unpack("<i", data[o : o + 4])[0]
    o += 4
    latency = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    age = struct.unpack("<f", data[o : o + 4])[0]
    o += 4
    hor_spd = struct.unpack("<d", data[o : o + 8])[0]
    o += 8
    trk_gnd = struct.unpack("<d", data[o : o + 8])[0]
    o += 8
    vert_spd = struct.unpack("<d", data[o : o + 8])[0]
    o += 8
    versp_std = struct.unpack("<f", data[o : o + 4])[0]

    return {
        **base,
        "velocity": {
            "sol_status": solution_status_to_str(v_sol_status),
            "vel_type": position_velocity_type_to_str(vel_type),
            "v_sol_status_code": int(v_sol_status),
            "vel_type_code": int(vel_type),
            "latency": latency,
            "age": age,
            "hor_spd": hor_spd,
            "trk_gnd": trk_gnd,
            "vert_spd": vert_spd,
            "versp_std": versp_std,
        },
    }


def _parse_bestnav_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None

        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 2118:
                    offset = i + 24
                    unpacked = _unpack_nav_binary_solution_120(data, offset)
                    if unpacked is None:
                        continue

                    msg_length = header.message_length
                    crc_value = None
                    if msg_length > 0:
                        crc_offset = i + msg_length - 4
                        if len(data) >= crc_offset + 4:
                            crc_value = struct.unpack("<I", data[crc_offset : crc_offset + 4])[0]

                    return {
                        "format": "binary",
                        **unpacked,
                        "header": {
                            "message_id": header.message_id,
                            "message_length": header.message_length,
                        },
                        "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                        "message_offset": i,
                    }

        return None

    try:
        text = data.decode("ascii", errors="ignore")
        bestnava_pos, _hdr = _bestnav_ascii_header_prefix(text)
        if bestnava_pos < 0:
            return None
        semicolon_pos = text.find(";", bestnava_pos + 10)
        if semicolon_pos <= bestnava_pos:
            return None
        crc_pos = text.find("*", semicolon_pos)
        if crc_pos < 0:
            crc_pos = text.find("\r", semicolon_pos)
        if crc_pos < 0:
            crc_pos = text.find("\n", semicolon_pos)
        if crc_pos < 0:
            crc_pos = min(len(text), semicolon_pos + 2000)

        line = text[bestnava_pos:crc_pos]

        parts = line.split(";", 1)
        if len(parts) < 2:
            return None
        data_part = parts[1].split("*")[0]
        flat = re.sub(r"[\r\n]+", "", data_part)
        fields = [f.strip().strip('"') for f in flat.split(",")]
        while len(fields) < 30:
            fields.append("")

        if len(fields) < 21:
            return None

        def _safe_int10(value: str) -> int:
            if not value:
                return 0
            try:
                return int(value.strip(), 10)
            except ValueError:
                return 0

        p_sol_status = fields[0] if fields[0] else "NONE"
        pos_type = fields[1] if fields[1] else "NONE"
        lat = _parse_bestnav_ascii_float(fields[2]) if len(fields) > 2 else 0.0
        lon = _parse_bestnav_ascii_float(fields[3]) if len(fields) > 3 else 0.0
        hgt = _parse_bestnav_ascii_float(fields[4]) if len(fields) > 4 else 0.0
        undulation = _parse_bestnav_ascii_float(fields[5]) if len(fields) > 5 else 0.0
        datum_id = fields[6] if fields[6] else "WGS84"
        lat_std = _parse_bestnav_ascii_float(fields[7]) if len(fields) > 7 else 0.0
        lon_std = _parse_bestnav_ascii_float(fields[8]) if len(fields) > 8 else 0.0
        hgt_std = _parse_bestnav_ascii_float(fields[9]) if len(fields) > 9 else 0.0
        stn_id = fields[10] if len(fields) > 10 else "0"
        diff_age = _parse_bestnav_ascii_float(fields[11]) if len(fields) > 11 else 0.0
        sol_age = _parse_bestnav_ascii_float(fields[12]) if len(fields) > 12 else 0.0

        num_svs = _safe_int10(fields[13]) if len(fields) > 13 else 0
        num_soln_svs = _safe_int10(fields[14]) if len(fields) > 14 else 0

        reserved1 = _safe_int10(fields[15]) if len(fields) > 15 else 0
        reserved2 = _safe_int10(fields[16]) if len(fields) > 16 else 0
        reserved3 = _safe_int10(fields[17]) if len(fields) > 17 else 0

        ext_sol_stat = _parse_bestnav_ascii_hex_byte(fields[18]) if len(fields) > 18 else 0
        gal_bds3_mask = _parse_bestnav_ascii_hex_byte(fields[19]) if len(fields) > 19 else 0
        gps_glo_bds2_mask = _parse_bestnav_ascii_hex_byte(fields[20]) if len(fields) > 20 else 0

        ext_dec = _bestnav_decode_signal_extensions(ext_sol_stat, gal_bds3_mask, gps_glo_bds2_mask)

        v_sol_status = fields[21] if len(fields) > 21 and fields[21] else "NONE"
        vel_type = fields[22] if len(fields) > 22 and fields[22] else "NONE"
        latency = _parse_bestnav_ascii_float(fields[23]) if len(fields) > 23 else 0.0
        age = _parse_bestnav_ascii_float(fields[24]) if len(fields) > 24 else 0.0
        hor_spd = _parse_bestnav_ascii_float(fields[25]) if len(fields) > 25 else 0.0
        trk_gnd = _parse_bestnav_ascii_float(fields[26]) if len(fields) > 26 else 0.0
        vert_spd = _parse_bestnav_ascii_float(fields[27]) if len(fields) > 27 else 0.0
        versp_std = _parse_bestnav_ascii_float(fields[28]) if len(fields) > 28 else 0.0
        horspd_std = _parse_bestnav_ascii_float(fields[29]) if len(fields) > 29 else 0.0

        return {
            "format": "ascii",
            "position": {
                "sol_status": p_sol_status,
                "pos_type": pos_type,
                "lat": lat,
                "lon": lon,
                "hgt": hgt,
                "undulation": undulation,
                "datum_id": datum_id,
                "lat_std": lat_std,
                "lon_std": lon_std,
                "hgt_std": hgt_std,
                "stn_id": stn_id,
                "diff_age": diff_age,
                "sol_age": sol_age,
                "num_svs": num_svs,
                "num_soln_svs": num_soln_svs,
            },
            "extended": {
                "ext_sol_stat": ext_sol_stat,
                "ext_sol_stat_hex": f"0x{ext_sol_stat:02X}",
                "gal_bds3_mask": gal_bds3_mask,
                "gal_bds3_mask_hex": f"0x{gal_bds3_mask:02X}",
                "gps_glo_bds2_mask": gps_glo_bds2_mask,
                "gps_glo_bds2_mask_hex": f"0x{gps_glo_bds2_mask:02X}",
                **ext_dec,
            },
            "velocity": {
                "sol_status": v_sol_status,
                "vel_type": vel_type,
                "latency": latency,
                "age": age,
                "hor_spd": hor_spd,
                "trk_gnd": trk_gnd,
                "vert_spd": vert_spd,
                "versp_std": versp_std,
                "horspd_std": horspd_std,
            },
            "reserved": {
                "reserved1": reserved1,
                "reserved2": reserved2,
                "reserved3": reserved3,
            },
            "raw": line,
        }
    except Exception:
        pass
    return None


def _parse_adrnav_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        need = 24 + _ADRNAV_BINARY_PAYLOAD_LEN
        for i in range(len(data) - need + 1):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id not in (142, 2117):
                    continue
                offset = i + 24
                if len(data) < offset + _ADRNAV_BINARY_PAYLOAD_LEN:
                    continue
                unpacked = _unpack_adrnav_binary_solution(data, offset)
                if unpacked is None:
                    continue
                msg_len = header.message_length
                crc_value = None
                if msg_len > 0 and len(data) >= i + msg_len:
                    crc_value = struct.unpack("<I", data[i + msg_len - 4 : i + msg_len])[0]
                elif len(data) >= offset + _ADRNAV_BINARY_PAYLOAD_LEN + 4:
                    crc_value = struct.unpack(
                        "<I",
                        data[offset + _ADRNAV_BINARY_PAYLOAD_LEN : offset + _ADRNAV_BINARY_PAYLOAD_LEN + 4],
                    )[0]
                return {
                    "format": "binary",
                    **unpacked,
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        start = -1
        for marker in ("#ADRNAVA", "#ADRNAVHA"):
            start = text.find(marker)
            if start >= 0:
                break
        if start < 0:
            return None
        star = text.find("*", start)
        if star < 0:
            end = text.find("\r", start)
            if end < 0:
                end = text.find("\n", start)
            if end < 0:
                end = len(text)
        else:
            end = star
        chunk = text[start:end]
        parts = chunk.split(";", 1)
        if len(parts) < 2:
            return None
        data_part = parts[1]
        tokens = [f.strip() for f in re.split(r"[\r\n,]+", data_part) if f.strip() != ""]
        if len(tokens) < 20:
            return None

        def _f(idx: int, default: float = 0.0) -> float:
            if idx >= len(tokens):
                return default
            try:
                return float(tokens[idx])
            except ValueError:
                return default

        def _i(idx: int, default: int = 0) -> int:
            if idx >= len(tokens):
                return default
            s = tokens[idx]
            try:
                return int(s)
            except ValueError:
                return default

        sol_status = tokens[0]
        pos_type = tokens[1] if len(tokens) > 1 else ""
        lat = _f(2)
        lon = _f(3)
        hgt = _f(4)
        undulation = _f(5)
        datum_id = tokens[6] if len(tokens) > 6 else ""
        lat_std = _f(7)
        lon_std = _f(8)
        hgt_std = _f(9)
        stn_id = tokens[10] if len(tokens) > 10 else ""
        diff_age = _f(11)
        sol_age = _f(12)
        num_svs = _i(13)
        num_soln_svs = _i(14)

        v_sol_status = tokens[15] if len(tokens) > 15 else ""
        vel_type = tokens[16] if len(tokens) > 16 else ""
        latency = _f(17)
        age = _f(18)
        hor_spd = _f(19)
        trk_gnd = _f(20)
        vert_spd = _f(21)
        versp_std = _f(22)
        horspd_std = _f(23)

        return {
            "format": "ascii",
            "position": {
                "sol_status": sol_status,
                "pos_type": pos_type,
                "lat": lat,
                "lon": lon,
                "hgt": hgt,
                "undulation": undulation,
                "datum_id": datum_id,
                "lat_std": lat_std,
                "lon_std": lon_std,
                "hgt_std": hgt_std,
                "stn_id": stn_id,
                "diff_age": diff_age,
                "sol_age": sol_age,
                "num_svs": num_svs,
                "num_soln_svs": num_soln_svs,
            },
            "velocity": {
                "sol_status": v_sol_status,
                "vel_type": vel_type,
                "latency": latency,
                "age": age,
                "hor_spd": hor_spd,
                "trk_gnd": trk_gnd,
                "vert_spd": vert_spd,
                "versp_std": versp_std,
                "horspd_std": horspd_std,
            },
            "raw": chunk,
        }
    except Exception:
        pass
    return None


def _check_pppnav_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(max(0, len(data) - 4000), len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 1026:
                    msg_len = header.message_length
                    if msg_len > 0 and len(data) >= i + msg_len:
                        return True
        return len(data) > 560
    try:
        text = data.decode("ascii", errors="ignore")
        if "#PPPNAVA" in text:
            return True
    except Exception:
        pass
    return len(data) > 200


def _check_sppnav_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(max(0, len(data) - 4000), len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 46:
                    msg_len = header.message_length
                    if msg_len > 0 and len(data) >= i + msg_len:
                        return True
        return len(data) > 640
    try:
        text = data.decode("ascii", errors="ignore")
        if "#SPPNAVA" in text:
            return True
    except Exception:
        pass
    return len(data) > 200


def _check_adrnavh_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(max(0, len(data) - 4000), len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 2117:
                    msg_len = header.message_length
                    if msg_len > 0 and len(data) >= i + msg_len:
                        return True
        return len(data) > 640
    try:
        text = data.decode("ascii", errors="ignore")
        if "#ADRNAVHA" in text:
            return True
    except Exception:
        pass
    return len(data) > 200


def _check_sppnavh_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(max(0, len(data) - 4000), len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 2116:
                    msg_len = header.message_length
                    if msg_len > 0 and len(data) >= i + msg_len:
                        return True
        return len(data) > 640
    try:
        text = data.decode("ascii", errors="ignore")
        if "#SPPNAVHA" in text:
            return True
    except Exception:
        pass
    return len(data) > 200


def _parse_pppnav_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        need = 24 + _PPPNAV_BINARY_PAYLOAD_LEN
        for i in range(len(data) - need + 1):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 1026:
                    continue
                offset = i + 24
                if len(data) < offset + _PPPNAV_BINARY_PAYLOAD_LEN:
                    continue
                unpacked = _unpack_nav_binary_position72(data, offset)
                if unpacked is None:
                    continue
                pos = dict(unpacked["position"])
                meta = {
                    "diff_age": pos.get("diff_age", 0.0),
                    "sol_age": pos.get("sol_age", 0.0),
                    "num_sats_tracked": pos.get("num_svs", 0),
                    "num_sats_used": pos.get("num_soln_svs", 0),
                    "extra": [],
                }
                pos["lat_sigma"] = pos.get("lat_std", 0.0)
                pos["lon_sigma"] = pos.get("lon_std", 0.0)
                pos["hgt_sigma"] = pos.get("hgt_std", 0.0)
                for k in ("diff_age", "sol_age", "num_svs", "num_soln_svs"):
                    pos.pop(k, None)
                msg_len = header.message_length
                crc_value = None
                if msg_len > 0 and len(data) >= i + msg_len:
                    crc_value = struct.unpack("<I", data[i + msg_len - 4 : i + msg_len])[0]
                elif len(data) >= offset + _PPPNAV_BINARY_AFTER_HEADER:
                    crc_value = struct.unpack(
                        "<I",
                        data[offset + _PPPNAV_BINARY_PAYLOAD_LEN : offset + _PPPNAV_BINARY_AFTER_HEADER],
                    )[0]
                return {
                    "format": "binary",
                    "position": pos,
                    "extended": unpacked["extended"],
                    "reserved": unpacked["reserved"],
                    "metadata": meta,
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        start = text.find("#PPPNAVA")
        if start < 0:
            return None
        star = text.find("*", start)
        if star < 0:
            end = text.find("\r", start)
            if end < 0:
                end = text.find("\n", start)
            if end < 0:
                end = len(text)
        else:
            end = star
        chunk = text[start:end]
        parts = chunk.split(";", 1)
        if len(parts) < 2:
            return None
        data_part = parts[1]
        tokens = [f.strip() for f in re.split(r"[\r\n,]+", data_part) if f.strip() != ""]
        if len(tokens) < 15:
            return None

        def _f(idx: int, default: float = 0.0) -> float:
            if idx >= len(tokens):
                return default
            try:
                return float(tokens[idx])
            except ValueError:
                return default

        def _i(idx: int, default: int = 0) -> int:
            if idx >= len(tokens):
                return default
            try:
                return int(tokens[idx])
            except ValueError:
                return default

        sol_status = tokens[0]
        pos_type = tokens[1] if len(tokens) > 1 else ""
        lat = _f(2)
        lon = _f(3)
        hgt = _f(4)
        undulation = _f(5)
        datum_id = tokens[6] if len(tokens) > 6 else "WGS84"
        lat_sigma = _f(7)
        lon_sigma = _f(8)
        hgt_sigma = _f(9)
        diff_age = _f(10)
        sol_age = _f(11)
        num_sats_tracked = _i(12)
        num_sats_used = _i(13)
        extra = tokens[14:]

        return {
            "format": "ascii",
            "position": {
                "sol_status": sol_status,
                "pos_type": pos_type,
                "lat": lat,
                "lon": lon,
                "hgt": hgt,
                "undulation": undulation,
                "datum_id": datum_id,
                "lat_sigma": lat_sigma,
                "lon_sigma": lon_sigma,
                "hgt_sigma": hgt_sigma,
            },
            "metadata": {
                "diff_age": diff_age,
                "sol_age": sol_age,
                "num_sats_tracked": num_sats_tracked,
                "num_sats_used": num_sats_used,
                "extra": extra,
            },
            "raw": chunk,
        }
    except Exception:
        pass
    return None


def _parse_sppnav_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        need = 24 + _SPPNAV_BINARY_PAYLOAD_LEN
        for i in range(len(data) - need + 1):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id not in (46, 2116):
                    continue
                offset = i + 24
                # Для SPPNAV обязательно ждём CRC после 120 байт payload (разд. 7.3.32: H+120..H+123).
                if len(data) < offset + _SPPNAV_BINARY_AFTER_HEADER:
                    continue
                unpacked = _unpack_nav_binary_solution_120(data, offset)
                if unpacked is None:
                    continue
                vel = unpacked["velocity"]
                velocity = {
                    "vel_sol_status": vel["sol_status"],
                    "vel_type": vel["vel_type"],
                    "latency": vel["latency"],
                    "age": vel["age"],
                    "hor_speed": vel["hor_spd"],
                    "track_ground": vel["trk_gnd"],
                    "vert_speed": vel["vert_spd"],
                    "vert_speed_sigma": vel["versp_std"],
                    "hor_speed_sigma": vel["horspd_std"],
                }
                pos = dict(unpacked["position"])
                meta = {
                    "station_id": pos.get("stn_id", ""),
                    "diff_age": pos.get("diff_age", 0.0),
                    "sol_age": pos.get("sol_age", 0.0),
                    "num_sats_tracked": pos.get("num_svs", 0),
                    "num_sats_used": pos.get("num_soln_svs", 0),
                }
                pos["lat_sigma"] = pos.get("lat_std", 0.0)
                pos["lon_sigma"] = pos.get("lon_std", 0.0)
                pos["hgt_sigma"] = pos.get("hgt_std", 0.0)
                for k in ("stn_id", "diff_age", "sol_age", "num_svs", "num_soln_svs", "lat_std", "lon_std", "hgt_std"):
                    pos.pop(k, None)
                msg_len = header.message_length
                crc_value = None
                if msg_len > 0 and len(data) >= i + msg_len:
                    crc_value = struct.unpack("<I", data[i + msg_len - 4 : i + msg_len])[0]
                elif len(data) >= offset + _SPPNAV_BINARY_AFTER_HEADER:
                    crc_value = struct.unpack(
                        "<I",
                        data[offset + _SPPNAV_BINARY_PAYLOAD_LEN : offset + _SPPNAV_BINARY_AFTER_HEADER],
                    )[0]
                return {
                    "format": "binary",
                    "position": pos,
                    "extended": unpacked["extended"],
                    "reserved": unpacked["reserved"],
                    "metadata": meta,
                    "velocity": velocity,
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        start = -1
        for marker in ("#SPPNAVA", "#SPPNAVHA"):
            start = text.find(marker)
            if start >= 0:
                break
        if start < 0:
            return None
        star = text.find("*", start)
        if star < 0:
            end = text.find("\r", start)
            if end < 0:
                end = text.find("\n", start)
            if end < 0:
                end = len(text)
        else:
            end = star
        chunk = text[start:end]
        parts = chunk.split(";", 1)
        if len(parts) < 2:
            return None
        data_part = parts[1]
        tokens = [f.strip() for f in re.split(r"[\r\n,]+", data_part) if f.strip() != ""]
        # Разд. 7.3.32: после «;» — позиция, stn id, diff/sol age, #SVs, #soln, 6 зарезерв./служебных полей, затем скорость.
        if len(tokens) < 28:
            return None

        def _f(idx: int, default: float = 0.0) -> float:
            if idx >= len(tokens):
                return default
            try:
                return float(tokens[idx])
            except ValueError:
                return default

        def _i(idx: int, default: int = 0) -> int:
            if idx >= len(tokens):
                return default
            try:
                return int(tokens[idx], 0)
            except ValueError:
                try:
                    return int(tokens[idx])
                except ValueError:
                    return default

        def _sigma_from_token(idx: int) -> float:
            if idx >= len(tokens):
                return 0.0
            t = tokens[idx]
            if "." in t or "e" in t.lower():
                return _f(idx, 0.0)
            return 0.0

        sol_status = tokens[0]
        pos_type = tokens[1] if len(tokens) > 1 else ""
        lat = _f(2)
        lon = _f(3)
        hgt = _f(4)
        undulation = _f(5)
        datum_id = tokens[6] if len(tokens) > 6 else "WGS84"
        lat_sigma = _f(7)
        lon_sigma = _f(8)
        hgt_sigma = _f(9)
        station_id = tokens[10].strip('"') if len(tokens) > 10 else ""
        diff_age = _f(11)
        sol_age = _f(12)
        num_sats_tracked = _i(13)
        num_sats_used = _i(14)
        pre_vel_extra = tokens[15:21] if len(tokens) > 15 else []
        vel_sol_status = tokens[21] if len(tokens) > 21 else ""
        vel_type = tokens[22] if len(tokens) > 22 else ""
        latency = _f(23)
        age = _f(24)
        hor_speed = _f(25)
        track_ground = _f(26)
        vert_speed = _f(27)
        vert_speed_sigma = _sigma_from_token(28)
        hor_speed_sigma = _sigma_from_token(29) if len(tokens) > 29 else 0.0
        tail_extra: List[str] = []
        if len(tokens) > 28 and vert_speed_sigma == 0.0 and tokens[28]:
            tail_extra.append(tokens[28])
        if len(tokens) > 29 and hor_speed_sigma == 0.0 and tokens[29] and tokens[29] not in tail_extra:
            tail_extra.append(tokens[29])
        if len(tokens) > 30:
            tail_extra.extend(tokens[30:])

        return {
            "format": "ascii",
            "position": {
                "sol_status": sol_status,
                "pos_type": pos_type,
                "lat": lat,
                "lon": lon,
                "hgt": hgt,
                "undulation": undulation,
                "datum_id": datum_id,
                "lat_sigma": lat_sigma,
                "lon_sigma": lon_sigma,
                "hgt_sigma": hgt_sigma,
            },
            "extended": {},
            "metadata": {
                "station_id": station_id,
                "diff_age": diff_age,
                "sol_age": sol_age,
                "num_sats_tracked": num_sats_tracked,
                "num_sats_used": num_sats_used,
                "reserved": pre_vel_extra,
                "extra": tail_extra,
            },
            "velocity": {
                "vel_sol_status": vel_sol_status,
                "vel_type": vel_type,
                "latency": latency,
                "age": age,
                "hor_speed": hor_speed,
                "track_ground": track_ground,
                "vert_speed": vert_speed,
                "vert_speed_sigma": vert_speed_sigma,
                "hor_speed_sigma": hor_speed_sigma,
            },
            "raw": chunk,
        }
    except Exception:
        pass
    return None


# STADOP (рук. §7.3.34): message_id 954; в части прошивок встречался 964 — принимаем оба.
_STADOP_BINARY_MSG_IDS = (954, 964)
# ADRDOP (раздел 7.3.36): message_id 953; в части прошивок встречался 963 — принимаем оба.
_ADRDOP_BINARY_MSG_IDS = (953, 963)

# ASCII DOP: несколько строк до «*» + 8 hex CRC; иначе check_complete срабатывает на первой строке.
_STADOP_ASCII_COMPLETE_RE = re.compile(
    r"#STADOP[AB].*?\*[0-9a-fA-F]{8}",
    re.DOTALL | re.IGNORECASE,
)
_ADRDOP_ASCII_COMPLETE_RE = re.compile(
    r"#ADRDOP[AB].*?\*[0-9a-fA-F]{8}",
    re.DOTALL | re.IGNORECASE,
)


def _dop_binary_total_len_candidates(data: bytes, sync_off: int, message_length: int) -> List[int]:
    """
    Кандидаты полной длины кадра DOP (от sync): учитываем обе трактовки message_length
    (полный кадр / тело), а также вычисление по #PRN из бинарного тела.
    """
    out: List[int] = []
    seen: set[int] = set()

    def _add(v: int) -> None:
        if 52 <= v <= 8192 and v not in seen:
            seen.add(v)
            out.append(v)

    ml = int(message_length)
    if ml > 0:
        _add(ml)
        _add(ml + 24)

    # По таблице §7.3.34: body = 42 + 2*#PRN, затем CRC(4), итого total = 24 + 42 + 2*#PRN + 4
    # В альтернативной раскладке (без U4 Itow в теле): total = 24 + 38 + 2*#PRN + 4.
    off = sync_off + 24
    if len(data) >= off + 42:
        n = struct.unpack("<H", data[off + 40 : off + 42])[0]
        if 0 <= n <= 255:
            _add(24 + 42 + 2 * n + 4)
    if len(data) >= off + 38:
        n2 = struct.unpack("<H", data[off + 36 : off + 38])[0]
        if 0 <= n2 <= 255:
            _add(24 + 38 + 2 * n2 + 4)
    return out


def _check_stadop_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(max(0, len(data) - 4000), len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id in _STADOP_BINARY_MSG_IDS:
                    msg_len = header.message_length
                    if msg_len > 0 and len(data) >= i + msg_len:
                        return True
        return len(data) > 500
    try:
        text = data.decode("ascii", errors="ignore")
        if _STADOP_ASCII_COMPLETE_RE.search(text):
            return True
    except Exception:
        pass
    return len(data) > 200


def _check_adrdop_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(max(0, len(data) - 4000), len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id in _ADRDOP_BINARY_MSG_IDS:
                    msg_len = header.message_length
                    if msg_len > 0 and len(data) >= i + msg_len:
                        return True
        return len(data) > 500
    try:
        text = data.decode("ascii", errors="ignore")
        if _ADRDOP_ASCII_COMPLETE_RE.search(text):
            return True
    except Exception:
        pass
    return len(data) > 200


def _unpack_dop_binary_payload(
    payload: bytes,
    *,
    frame_header_ms: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    Полезная нагрузка STADOP/ADRDOP (байты после 24-байтного заголовка кадра, без CRC кадра).

    По мануалу: Itow U4, 9×float (gdop…reserved), #PRN U2, PRN×U2.
    На практике UM982 иногда отдаёт перед телом дополнительное выравнивание / поле H —
    перебираем skip кратно 4 до 60 байт и выбираем вариант, где длина согласована с #PRN.

    Вариант без Itow в теле (редко): сразу 9×float, #PRN, PRN — тогда time_of_week берётся из
    заголовка кадра (ms), если передан frame_header_ms.
    """
    best: Optional[Tuple[int, Dict[str, Any]]] = None

    def _score(skip: int, tail: int, gdop: float, n: int, has_itow: bool) -> int:
        s = 500 - min(skip, 500) // 4
        if tail == 0:
            s += 40
        elif tail <= 4:
            s += 25
        elif tail <= 8:
            s += 10
        if has_itow:
            s += 15
        if abs(gdop) < 1e6 and math.isfinite(gdop):
            s += 20
        if 0 < n < 200:
            s += min(n // 2, 30)
        return s

    L = len(payload)
    if L < 38:
        return None

    # --- Вариант A: U4 Itow + 9f + U2 #PRN + PRN×U2 ---
    for skip in range(0, min(L, 68), 4):
        chunk = payload[skip:]
        lc = len(chunk)
        if lc < 42:
            continue
        itow = struct.unpack("<I", chunk[0:4])[0]
        try:
            floats = struct.unpack("<9f", chunk[4:40])
        except struct.error:
            continue
        if not all(math.isfinite(f) for f in floats):
            continue
        gdop, pdop, tdop, vdop, hdop, ndop, edop, cutoff_angle, reserved = floats
        if any(abs(f) > 1e8 for f in floats):
            continue
        num_satellites = struct.unpack("<H", chunk[40:42])[0]
        if num_satellites > 255:
            continue
        need = 42 + 2 * num_satellites
        if lc < need:
            continue
        tail = lc - need
        if tail > 12:
            continue
        try:
            prn_list = list(struct.unpack(f"<{num_satellites}H", chunk[42:need]))
        except struct.error:
            continue
        out = {
            "time_of_week": float(itow),
            "gdop": float(gdop),
            "pdop": float(pdop),
            "tdop": float(tdop),
            "vdop": float(vdop),
            "hdop": float(hdop),
            "ndop": float(ndop),
            "edop": float(edop),
            "cutoff_angle": float(cutoff_angle),
            "reserved": float(reserved),
            "num_satellites": int(num_satellites),
            "prn_list": prn_list,
            "binary_body_skip": skip,
        }
        sc = _score(skip, tail, gdop, num_satellites, True)
        if best is None or sc > best[0]:
            best = (sc, out)

    # --- Вариант B: без U4 Itow — 9f + U2 + PRN×U2 (минимум 38 байт + 2×#PRN) ---
    for skip in range(0, min(L, 68), 4):
        chunk = payload[skip:]
        lc = len(chunk)
        if lc < 38:
            continue
        try:
            floats = struct.unpack("<9f", chunk[0:36])
        except struct.error:
            continue
        if not all(math.isfinite(f) for f in floats):
            continue
        gdop, pdop, tdop, vdop, hdop, ndop, edop, cutoff_angle, reserved = floats
        if any(abs(f) > 1e8 for f in floats):
            continue
        num_satellites = struct.unpack("<H", chunk[36:38])[0]
        if num_satellites > 255:
            continue
        need = 38 + 2 * num_satellites
        if lc < need:
            continue
        tail = lc - need
        if tail > 12:
            continue
        try:
            prn_list = list(struct.unpack(f"<{num_satellites}H", chunk[38:need]))
        except struct.error:
            continue
        tow = float(frame_header_ms) if frame_header_ms is not None else 0.0
        out = {
            "time_of_week": tow,
            "gdop": float(gdop),
            "pdop": float(pdop),
            "tdop": float(tdop),
            "vdop": float(vdop),
            "hdop": float(hdop),
            "ndop": float(ndop),
            "edop": float(edop),
            "cutoff_angle": float(cutoff_angle),
            "reserved": float(reserved),
            "num_satellites": int(num_satellites),
            "prn_list": prn_list,
            "binary_body_skip": skip,
            "binary_body_layout": "no_itow_u4",
        }
        sc = _score(skip, tail, gdop, num_satellites, False)
        if best is None or sc > best[0]:
            best = (sc, out)

    return best[1] if best else None


def _parse_dop_message(
    data: bytes,
    binary: bool,
    binary_msg_ids: Tuple[int, ...],
    ascii_markers: Tuple[str, ...],
) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id not in binary_msg_ids:
                    continue
                cands = _dop_binary_total_len_candidates(data, i, header.message_length)
                if not cands:
                    continue
                need_more = False
                for total_len in cands:
                    if len(data) < i + total_len:
                        need_more = True
                        continue
                    payload = data[i + 24 : i + total_len - 4]
                    try:
                        crc_value = struct.unpack("<I", data[i + total_len - 4 : i + total_len])[0]
                    except struct.error:
                        continue
                    unpacked = _unpack_dop_binary_payload(
                        payload,
                        frame_header_ms=int(header.seconds_of_week_ms),
                    )
                    if not unpacked:
                        continue
                    out: Dict[str, Any] = {
                        "format": "binary",
                        "header": {
                            "message_id": header.message_id,
                            "message_length": header.message_length,
                        },
                        "payload": payload,
                        "crc": f"0x{crc_value:08X}",
                        "message_offset": i,
                        "frame_length": total_len,
                    }
                    out.update(unpacked)
                    return out
                if need_more:
                    return None
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        start = -1
        for marker in ascii_markers:
            start = text.find(marker)
            if start >= 0:
                break
        if start < 0:
            return None
        star = text.find("*", start)
        if star < 0:
            end = text.find("\r", start)
            if end < 0:
                end = text.find("\n", start)
            if end < 0:
                end = len(text)
        else:
            end = star
        chunk = text[start:end]

        def _f(tok: List[str], idx: int, default: float = 0.0) -> float:
            if idx >= len(tok):
                return default
            try:
                return float(tok[idx])
            except ValueError:
                return default

        def _i(tok: List[str], idx: int, default: int = 0) -> int:
            if idx >= len(tok):
                return default
            try:
                return int(tok[idx])
            except ValueError:
                return default

        # Вариант 1a: «…;Itow,gdop,…,reserved,#PRN,PRN…» (Itow в теле после «;», целое)
        # Вариант 1b: ADRDOP (7.3.36) — «…;gdop,…,reserved,#PRN,…» (Itow только в заголовке до «;»)
        if ";" in chunk:
            parts = chunk.split(";", 1)
            if len(parts) >= 2:
                header_blob = parts[0]
                header_fields = [f.strip() for f in header_blob.split(",") if f.strip() != ""]
                time_from_hdr = 0.0
                if len(header_fields) > 5:
                    try:
                        time_from_hdr = float(header_fields[5])
                    except ValueError:
                        time_from_hdr = 0.0
                data_part = parts[1]
                tokens = [f.strip() for f in re.split(r"[\r\n,]+", data_part) if f.strip() != ""]
                if len(tokens) < 9:
                    return None

                def _pure_int_field(s: str) -> bool:
                    return bool(re.fullmatch(r"-?\d+", (s or "").strip()))

                def _floatish_field(s: str) -> bool:
                    x = (s or "").strip()
                    return bool(x) and ("." in x or "e" in x.lower() or "E" in x)

                # Классика: Itow целым в теле, второе поле — уже float (gdop)
                if len(tokens) >= 11 and _pure_int_field(tokens[0]) and _floatish_field(tokens[1]):
                    time_of_week = _f(tokens, 0)
                    gdop = _f(tokens, 1)
                    pdop = _f(tokens, 2)
                    tdop = _f(tokens, 3)
                    vdop = _f(tokens, 4)
                    hdop = _f(tokens, 5)
                    ndop = _f(tokens, 6)
                    edop = _f(tokens, 7)
                    cutoff_angle = _f(tokens, 8)
                    reserved = _f(tokens, 9)
                    num_satellites = _i(tokens, 10)
                    prn_tokens = tokens[11 : 11 + max(0, num_satellites)]
                    prn_list: List[int] = []
                    for t in prn_tokens:
                        try:
                            prn_list.append(int(t))
                        except ValueError:
                            continue
                    return {
                        "format": "ascii",
                        "time_of_week": time_of_week,
                        "gdop": gdop,
                        "pdop": pdop,
                        "tdop": tdop,
                        "vdop": vdop,
                        "hdop": hdop,
                        "ndop": ndop,
                        "edop": edop,
                        "cutoff_angle": cutoff_angle,
                        "reserved": reserved,
                        "num_satellites": num_satellites,
                        "prn_list": prn_list,
                        "raw": chunk,
                    }
                gdop = _f(tokens, 0)
                pdop = _f(tokens, 1)
                tdop = _f(tokens, 2)
                vdop = _f(tokens, 3)
                hdop = _f(tokens, 4)
                ndop = _f(tokens, 5)
                edop = _f(tokens, 6)
                cutoff_angle = _f(tokens, 7)
                reserved = _f(tokens, 8)
                num_satellites = 0
                prn_list = []
                if len(tokens) > 9:
                    try:
                        num_satellites = int(float(tokens[9]))
                    except ValueError:
                        num_satellites = 0
                    for j in range(max(0, num_satellites)):
                        k = 10 + j
                        if k >= len(tokens):
                            break
                        try:
                            prn_list.append(int(float(tokens[k])))
                        except ValueError:
                            break
                return {
                    "format": "ascii",
                    "time_of_week": time_from_hdr,
                    "gdop": gdop,
                    "pdop": pdop,
                    "tdop": tdop,
                    "vdop": vdop,
                    "hdop": hdop,
                    "ndop": ndop,
                    "edop": edop,
                    "cutoff_angle": cutoff_angle,
                    "reserved": reserved,
                    "num_satellites": num_satellites,
                    "prn_list": prn_list,
                    "raw": chunk,
                }
            return None

        # Вариант 2 (рук. §7.3.34, пример ASCII): без «;», стандартный заголовок лога (9 полей после имени),
        # затем 9 float, UShort #PRN, список PRN (часто с переносами строк).
        tokens_all = [f.strip() for f in re.split(r"[\r\n,]+", chunk) if f.strip() != ""]
        if not tokens_all:
            return None
        t0 = tokens_all[0].upper()
        if not any(t0 == m.upper() or t0.startswith(m.upper()) for m in ascii_markers):
            return None
        # 9 полей заголовка после «#STADOPA» / «#ADRDOPA» и т.д.: port, GNSS, sol_status, week, tow, …
        header_tail = 9
        if len(tokens_all) < 1 + header_tail + 9 + 1:
            return None
        hdr = tokens_all[1 : 1 + header_tail]
        time_of_week = float(hdr[4]) if len(hdr) > 4 else 0.0
        body0 = 1 + header_tail

        def _plain_int_token(s: str) -> bool:
            return bool(re.fullmatch(r"-?\d+", (s or "").strip()))

        # В бинарном теле 9 float (рук.); в ASCII-примере UM982 часто 10 float подряд, затем #PRN.
        num_idx = body0 + 9
        n_float_body = 9
        if len(tokens_all) > body0 + 10 and not _plain_int_token(tokens_all[body0 + 9]):
            n_float_body = 10
            num_idx = body0 + 10
        if len(tokens_all) < num_idx + 1:
            return None
        gdop = _f(tokens_all, body0 + 0)
        pdop = _f(tokens_all, body0 + 1)
        tdop = _f(tokens_all, body0 + 2)
        vdop = _f(tokens_all, body0 + 3)
        hdop = _f(tokens_all, body0 + 4)
        ndop = _f(tokens_all, body0 + 5)
        edop = _f(tokens_all, body0 + 6)
        cutoff_angle = _f(tokens_all, body0 + 7)
        reserved = _f(tokens_all, body0 + 8)
        if n_float_body == 10:
            # Десятое значение в примере руководства — нулевой «хвост» перед #PRN, не дубль reserved.
            _ = _f(tokens_all, body0 + 9)
        num_satellites = _i(tokens_all, num_idx)
        prn_start = num_idx + 1
        prn_list = []
        max_prn = min(num_satellites, max(0, len(tokens_all) - prn_start))
        for j in range(max_prn):
            try:
                prn_list.append(int(tokens_all[prn_start + j]))
            except ValueError:
                continue

        return {
            "format": "ascii",
            "time_of_week": time_of_week,
            "gdop": gdop,
            "pdop": pdop,
            "tdop": tdop,
            "vdop": vdop,
            "hdop": hdop,
            "ndop": ndop,
            "edop": edop,
            "cutoff_angle": cutoff_angle,
            "reserved": reserved,
            "num_satellites": num_satellites,
            "prn_list": prn_list,
            "raw": chunk,
        }
    except Exception:
        pass
    return None


def _parse_stadop_message(data: bytes, binary: bool = False) -> Optional[dict]:
    return _parse_dop_message(data, binary, _STADOP_BINARY_MSG_IDS, ("#STADOPA", "#STADOPB"))


def _parse_adrdop_message(data: bytes, binary: bool = False) -> Optional[dict]:
    return _parse_dop_message(
        data,
        binary,
        _ADRDOP_BINARY_MSG_IDS,
        ("#ADRDOPA", "#ADRDOPB"),
    )


def _parse_adrdoph_message(data: bytes, binary: bool = False) -> Optional[dict]:
    # Принимаем оба префикса: корректный ADRDOPH* и исторический ARDDOPH* (опечатка в ранних версиях).
    return _parse_dop_message(
        data,
        binary,
        (2121,),
        ("#ADRDOPHA", "#ADRDOPHB", "#ARDDOPHA", "#ARDDOPHB"),
    )


def _parse_bestnavxyz_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i : i + 3] != bytes((0xAA, 0x44, 0xB5)):
                continue
            header = parse_unicore_header(data[i : i + 24])
            if not header or header.message_id != BESTNAVXYZ_MESSAGE_ID:
                continue
            trip = _bestnavxyz_try_unpack_frame(data, i, header.message_length)
            if trip is None:
                continue
            unpacked, used_total, crc_value = trip
            return {
                "format": "binary",
                **unpacked,
                "header": {
                    "message_id": header.message_id,
                    "message_length": header.message_length,
                    "frame_bytes": used_total,
                },
                "crc": f"0x{crc_value:08X}",
                "message_offset": i,
            }
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        ascii_crc_hex: Optional[str] = None
        m = _BESTNAVXYZ_ASCII_COMPLETE_RE.search(text)
        if m:
            chunk_full = m.group(0)
            star = chunk_full.rfind("*")
            if star < 0:
                return None
            crc_hex = chunk_full[star + 1 : star + 9]
            if not re.fullmatch(r"[0-9a-fA-F]{8}", crc_hex, flags=re.I):
                return None
            ascii_crc_hex = crc_hex
            line = chunk_full[:star]
        else:
            pos = -1
            for marker in ("#BESTNAVXYZA,", "#BESTNAVXYZB,", "#BESTNAVXYZA", "#BESTNAVXYZB"):
                p = text.find(marker)
                if p >= 0:
                    pos = p
                    break
            if pos < 0:
                return None
            rest = text[pos:]
            end = rest.find("*")
            if end < 0:
                end = min(len(rest), 1500)
            line = rest[:end]
        ls = line.strip()
        if not (ls.startswith("#BESTNAVXYZA") or ls.startswith("#BESTNAVXYZB")):
            return None
        parts = line.split(";", 1)
        if len(parts) < 2:
            return None
        data_part = parts[1].split("*")[0]
        flat = re.sub(r"[\r\n]+", "", data_part)
        fields = [f.strip().strip('"') for f in flat.split(",")]
        while len(fields) < 27:
            fields.append("")
        if len(fields) < 27:
            return None

        def _safe_int10(s: str) -> int:
            if not s:
                return 0
            try:
                return int(s.strip(), 10)
            except ValueError:
                return 0

        p_sol_status = fields[0] if fields[0] else ""
        pos_type = fields[1] if len(fields) > 1 else ""
        p_x = _parse_bestnav_ascii_float(fields[2]) if len(fields) > 2 else 0.0
        p_y = _parse_bestnav_ascii_float(fields[3]) if len(fields) > 3 else 0.0
        p_z = _parse_bestnav_ascii_float(fields[4]) if len(fields) > 4 else 0.0
        p_x_sigma = _parse_bestnav_ascii_float(fields[5]) if len(fields) > 5 else 0.0
        p_y_sigma = _parse_bestnav_ascii_float(fields[6]) if len(fields) > 6 else 0.0
        p_z_sigma = _parse_bestnav_ascii_float(fields[7]) if len(fields) > 7 else 0.0

        v_sol_status = fields[8] if len(fields) > 8 else ""
        vel_type = fields[9] if len(fields) > 9 else ""
        v_x = _parse_bestnav_ascii_float(fields[10]) if len(fields) > 10 else 0.0
        v_y = _parse_bestnav_ascii_float(fields[11]) if len(fields) > 11 else 0.0
        v_z = _parse_bestnav_ascii_float(fields[12]) if len(fields) > 12 else 0.0
        v_x_sigma = _parse_bestnav_ascii_float(fields[13]) if len(fields) > 13 else 0.0
        v_y_sigma = _parse_bestnav_ascii_float(fields[14]) if len(fields) > 14 else 0.0
        v_z_sigma = _parse_bestnav_ascii_float(fields[15]) if len(fields) > 15 else 0.0

        station_id = fields[16] if len(fields) > 16 else ""
        v_latency = _parse_bestnav_ascii_float(fields[17]) if len(fields) > 17 else 0.0
        diff_age = _parse_bestnav_ascii_float(fields[18]) if len(fields) > 18 else 0.0
        sol_age = _parse_bestnav_ascii_float(fields[19]) if len(fields) > 19 else 0.0

        num_sats_tracked = _safe_int10(fields[20]) if len(fields) > 20 else 0
        num_sats_used = _safe_int10(fields[21]) if len(fields) > 21 else 0
        num_gg_l1 = _safe_int10(fields[22]) if len(fields) > 22 else 0
        num_soln_multi = _safe_int10(fields[23]) if len(fields) > 23 else 0

        if len(fields) >= 28:
            reserved_b = _safe_int10(fields[24]) & 0xFF
            ext_b = _parse_bestnav_ascii_hex_byte(fields[25])
            gal_b = _parse_bestnav_ascii_hex_byte(fields[26])
            gps_b = _parse_bestnav_ascii_hex_byte(fields[27])
        else:
            reserved_b = _safe_int10(fields[24]) & 0xFF if len(fields) > 24 else 0
            ext_b = _parse_bestnav_ascii_hex_byte(fields[25]) if len(fields) > 25 else 0
            gal_b = 0
            gps_b = _parse_bestnav_ascii_hex_byte(fields[26]) if len(fields) > 26 else 0

        ext_dec = _bestnav_decode_signal_extensions(ext_b, gal_b, gps_b)

        out: Dict[str, Any] = {
            "format": "ascii",
            "position": {
                "P_sol_status": p_sol_status,
                "pos_type": pos_type,
                "P_X": p_x,
                "P_Y": p_y,
                "P_Z": p_z,
                "P_X_sigma": p_x_sigma,
                "P_Y_sigma": p_y_sigma,
                "P_Z_sigma": p_z_sigma,
            },
            "velocity": {
                "V_sol_status": v_sol_status,
                "vel_type": vel_type,
                "V_X": v_x,
                "V_Y": v_y,
                "V_Z": v_z,
                "V_X_sigma": v_x_sigma,
                "V_Y_sigma": v_y_sigma,
                "V_Z_sigma": v_z_sigma,
            },
            "metadata": {
                "station_id": station_id,
                "V_latency": v_latency,
                "diff_age": diff_age,
                "sol_age": sol_age,
                "num_sats_tracked": num_sats_tracked,
                "num_sats_used": num_sats_used,
                "num_gg_l1": num_gg_l1,
                "num_soln_multi_svs": num_soln_multi,
                "reserved": reserved_b,
            },
            "extended": {
                "ext_sol_stat": ext_b,
                "ext_sol_stat_hex": f"0x{ext_b:02X}",
                "gal_bds3_mask": gal_b,
                "gal_bds3_mask_hex": f"0x{gal_b:02X}",
                "gps_glo_bds2_mask": gps_b,
                "gps_glo_bds2_mask_hex": f"0x{gps_b:02X}",
                **ext_dec,
            },
            "raw": line,
        }
        if ascii_crc_hex is not None:
            out["ascii_crc_hex"] = ascii_crc_hex
        return out
    except Exception:
        pass
    return None


def query_bestnav(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"BESTNAVB {rate}" if binary else f"BESTNAVA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_bestnav_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=_check_bestnav_complete,
        result_key="bestnav",
    )


def query_adrnav(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"ADRNAVB {rate}" if binary else f"ADRNAVA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_adrnav_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=_check_adrnav_complete,
        result_key="adrnav",
    )


def query_pppnav(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"PPPNAVB {rate}" if binary else f"PPPNAVA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_pppnav_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=_check_pppnav_complete,
        result_key="pppnav",
    )


def query_sppnav(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"SPPNAVB {rate}" if binary else f"SPPNAVA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_sppnav_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=_check_sppnav_complete,
        result_key="sppnav",
    )


def query_adrnavh(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"ADRNAVHB {rate}" if binary else f"ADRNAVHA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_adrnav_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=_check_adrnavh_complete,
        result_key="adrnavh",
    )


def query_sppnavh(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"SPPNAVHB {rate}" if binary else f"SPPNAVHA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_sppnav_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=_check_sppnavh_complete,
        result_key="sppnavh",
    )



def query_stadop(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"STADOPB {rate}" if binary else f"STADOPA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_stadop_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=_check_stadop_complete,
        result_key="stadop",
    )


def query_adrdop(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"ADRDOPB {rate}" if binary else f"ADRDOPA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_adrdop_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=_check_adrdop_complete,
        result_key="adrdop",
    )


def query_bestnavxyz(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"BESTNAVXYZB {rate}" if binary else f"BESTNAVXYZA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_bestnavxyz_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=_check_bestnavxyz_complete,
        result_key="bestnavxyz",
    )


def query_adrdoph(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"ADRDOPHB {rate}" if binary else f"ADRDOPHA {rate}"
    check_complete = _make_unicore_header_checker(
        2121,
        min_length=0,
        ascii_tag=b"#ADRDOPHA",
        ascii_window=500,
        binary_min_total=500,
        ascii_min_total=200,
    )
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_adrdoph_message,
        binary=binary,
        add_crlf=add_crlf,
        read_attempts=24,
        check_complete=check_complete,
        result_key="adrdoph",
    )
