"""Навигационные решения, DOP, XYZ: BESTNAV, ADRNAV, PPPNAV, SPPNAV, ADRNAVH, SPPNAVH, STADOP, ARDDOP, BESTNAVXYZ, ARDDOPH."""
import re
import struct
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from um982.core import Um982Core
from um982.utils import parse_unicore_header

from .base import _run_data_query, _make_unicore_header_checker


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
    """DOP и связанные поля (STADOP/ARDDOP)."""
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


# --- NAV: BESTNAV / ADRNAV / PPPNAV / SPPNAV / STADOP / ARDDOP / BESTNAVXYZ ---

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
        bestnava_pos = text.find("#BESTNAVA,")
        if bestnava_pos >= 0:
            end_pos = min(len(text), bestnava_pos + 2000)
            if "*" in text[bestnava_pos:end_pos]:
                semicolon_pos = text.find(";", bestnava_pos)
                if bestnava_pos < semicolon_pos < end_pos:
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
        return len(data) > 500
    try:
        text = data.decode("ascii", errors="ignore")
        if "#ADRNAVA" in text:
            return True
    except Exception:
        pass
    return len(data) > 200


def _parse_bestnav_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None

        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 2118:
                    offset = i + 24
                    if len(data) < offset + 120:
                        continue

                    p_sol_status = struct.unpack("<i", data[offset : offset + 4])[0]
                    offset += 4

                    pos_type = struct.unpack("<i", data[offset : offset + 4])[0]
                    offset += 4

                    lat = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    lon = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    hgt = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8

                    undulation = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4

                    datum_id = struct.unpack("<i", data[offset : offset + 4])[0]
                    offset += 4

                    lat_std = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    lon_std = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    hgt_std = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4

                    stn_id = data[offset : offset + 4].decode("ascii", errors="ignore").rstrip("\x00")
                    offset += 4

                    diff_age = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    sol_age = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4

                    num_svs = struct.unpack("<B", data[offset : offset + 1])[0]
                    offset += 1
                    num_soln_svs = struct.unpack("<B", data[offset : offset + 1])[0]
                    offset += 1
                    reserved1 = struct.unpack("<B", data[offset : offset + 1])[0]
                    offset += 1

                    reserved2 = struct.unpack("<B", data[offset : offset + 1])[0]
                    offset += 1
                    reserved3 = struct.unpack("<B", data[offset : offset + 1])[0]
                    offset += 1

                    ext_sol_stat = struct.unpack("<B", data[offset : offset + 1])[0]
                    offset += 1
                    gal_bds3_mask = struct.unpack("<B", data[offset : offset + 1])[0]
                    offset += 1
                    gps_glo_bds2_mask = struct.unpack("<B", data[offset : offset + 1])[0]
                    offset += 1

                    v_sol_status = struct.unpack("<i", data[offset : offset + 4])[0]
                    offset += 4

                    vel_type = struct.unpack("<i", data[offset : offset + 4])[0]
                    offset += 4

                    latency = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    age = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4

                    hor_spd = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    trk_gnd = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8
                    vert_spd = struct.unpack("<d", data[offset : offset + 8])[0]
                    offset += 8

                    versp_std = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4
                    horspd_std = struct.unpack("<f", data[offset : offset + 4])[0]
                    offset += 4

                    msg_length = header.message_length
                    crc_value = None
                    if msg_length > 0:
                        crc_offset = i + msg_length - 4
                        if len(data) >= crc_offset + 4:
                            crc_value = struct.unpack("<I", data[crc_offset : crc_offset + 4])[0]

                    return {
                        "format": "binary",
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
        bestnava_pos = text.find("#BESTNAVA,")
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
        if not line.startswith("#BESTNAVA,"):
            return None

        parts = line.split(";", 1)
        if len(parts) < 2:
            return None
        data_part = parts[1].split("*")[0]
        fields = data_part.split(",")
        if len(fields) < 30:
            return None

        p_sol_status = fields[0] if fields[0] else "NONE"
        pos_type = fields[1] if fields[1] else "NONE"
        lat = float(fields[2]) if fields[2] else 0.0
        lon = float(fields[3]) if fields[3] else 0.0
        hgt = float(fields[4]) if fields[4] else 0.0
        undulation = float(fields[5]) if fields[5] else 0.0
        datum_id = fields[6] if fields[6] else "WGS84"
        lat_std = float(fields[7]) if fields[7] else 0.0
        lon_std = float(fields[8]) if fields[8] else 0.0
        hgt_std = float(fields[9]) if fields[9] else 0.0
        stn_id = fields[10].strip('"') if fields[10] else "0"
        diff_age = float(fields[11]) if fields[11] else 0.0
        sol_age = float(fields[12]) if fields[12] else 0.0

        num_svs = int(fields[13]) if fields[13] else 0
        num_soln_svs = int(fields[14]) if fields[14] else 0

        def _safe_int(value: str) -> int:
            try:
                return int(value)
            except ValueError:
                return 0

        reserved1 = _safe_int(fields[15]) if len(fields) > 15 and fields[15] else 0
        reserved2 = _safe_int(fields[16]) if len(fields) > 16 and fields[16] else 0
        reserved3 = _safe_int(fields[17]) if len(fields) > 17 and fields[17] else 0

        def _decode_hex_or_int(value: str) -> int:
            try:
                if len(value) <= 2 and all(c in "0123456789ABCDEFabcdef" for c in value):
                    return int(value, 16)
                return int(value)
            except ValueError:
                return 0

        ext_sol_stat = _decode_hex_or_int(fields[18]) if len(fields) > 18 and fields[18] else 0
        gal_bds3_mask = _decode_hex_or_int(fields[19]) if len(fields) > 19 and fields[19] else 0
        gps_glo_bds2_mask = _decode_hex_or_int(fields[20]) if len(fields) > 20 and fields[20] else 0

        v_sol_status = fields[21] if len(fields) > 21 and fields[21] else "NONE"
        vel_type = fields[22] if len(fields) > 22 and fields[22] else "NONE"
        latency = float(fields[23]) if len(fields) > 23 and fields[23] else 0.0
        age = float(fields[24]) if len(fields) > 24 and fields[24] else 0.0
        hor_spd = float(fields[25]) if len(fields) > 25 and fields[25] else 0.0
        trk_gnd = float(fields[26]) if len(fields) > 26 and fields[26] else 0.0
        vert_spd = float(fields[27]) if len(fields) > 27 and fields[27] else 0.0
        versp_std = float(fields[28]) if len(fields) > 28 and fields[28] else 0.0
        horspd_std = float(fields[29]) if len(fields) > 29 and fields[29] else 0.0

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
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 142:
                    continue
                msg_len = header.message_length
                if msg_len <= 0 or len(data) < i + msg_len:
                    continue
                payload = data[i + 24 : i + msg_len - 4] if msg_len >= 28 else data[i + 24 : i + msg_len]
                crc_value = None
                if msg_len >= 28 and len(data) >= i + msg_len:
                    crc_value = struct.unpack("<I", data[i + msg_len - 4 : i + msg_len])[0]
                return {
                    "format": "binary",
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "payload": payload,
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        start = text.find("#ADRNAVA")
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
        tokens = [f.strip() for f in re.split(r"[,\\r\\n]+", data_part) if f.strip() != ""]
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
        return len(data) > 500
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
        return len(data) > 500
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
        return len(data) > 500
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
        return len(data) > 500
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
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 1026:
                    continue
                msg_len = header.message_length
                if msg_len <= 0 or len(data) < i + msg_len:
                    continue
                payload = data[i + 24 : i + msg_len - 4] if msg_len >= 28 else data[i + 24 : i + msg_len]
                crc_value = None
                if msg_len >= 28 and len(data) >= i + msg_len:
                    crc_value = struct.unpack("<I", data[i + msg_len - 4 : i + msg_len])[0]
                return {
                    "format": "binary",
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "payload": payload,
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
        tokens = [f.strip() for f in re.split(r"[,\\r\\n]+", data_part) if f.strip() != ""]
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
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 46:
                    continue
                msg_len = header.message_length
                if msg_len <= 0 or len(data) < i + msg_len:
                    continue
                payload = data[i + 24 : i + msg_len - 4] if msg_len >= 28 else data[i + 24 : i + msg_len]
                crc_value = None
                if msg_len >= 28 and len(data) >= i + msg_len:
                    crc_value = struct.unpack("<I", data[i + msg_len - 4 : i + msg_len])[0]
                return {
                    "format": "binary",
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "payload": payload,
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        start = text.find("#SPPNAVA")
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
        tokens = [f.strip() for f in re.split(r"[,\\r\\n]+", data_part) if f.strip() != ""]
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
        station_id = tokens[10].strip('"') if len(tokens) > 10 else ""
        diff_age = _f(11)
        sol_age = _f(12)
        num_sats_tracked = _i(13)
        num_sats_used = _i(14)
        reserved = tokens[15:18]
        vel_sol_status = tokens[18] if len(tokens) > 18 else ""
        vel_type = tokens[19] if len(tokens) > 19 else ""
        latency = _f(20)
        age = _f(21)
        hor_speed = _f(22)
        track_ground = _f(23)
        vert_speed = _f(24)
        vert_speed_sigma = _f(25)
        hor_speed_sigma = _f(26)

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
                "station_id": station_id,
                "diff_age": diff_age,
                "sol_age": sol_age,
                "num_sats_tracked": num_sats_tracked,
                "num_sats_used": num_sats_used,
                "reserved": reserved,
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


def _check_stadop_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(max(0, len(data) - 4000), len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 964:
                    msg_len = header.message_length
                    if msg_len > 0 and len(data) >= i + msg_len:
                        return True
        return len(data) > 500
    try:
        text = data.decode("ascii", errors="ignore")
        if "#STADOPA" in text:
            return True
    except Exception:
        pass
    return len(data) > 200


def _check_arddop_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(max(0, len(data) - 4000), len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 963:
                    msg_len = header.message_length
                    if msg_len > 0 and len(data) >= i + msg_len:
                        return True
        return len(data) > 500
    try:
        text = data.decode("ascii", errors="ignore")
        if "#ARDDOPA" in text:
            return True
    except Exception:
        pass
    return len(data) > 200


def _check_bestnavxyz_complete(data: bytes, is_binary: bool) -> bool:
    if is_binary:
        for i in range(max(0, len(data) - 200), len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if header and header.message_id == 240:
                    msg_len = header.message_length
                    if msg_len > 0 and len(data) >= i + msg_len:
                        return True
        return len(data) > 500
    try:
        text = data.decode("ascii", errors="ignore")
        if "#BESTNAVXYZA" in text or "#BESTNAVXYZA," in text:
            return True
    except Exception:
        pass
    return len(data) > 500


def _parse_stadop_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 964:
                    continue
                msg_len = header.message_length
                if msg_len <= 0 or len(data) < i + msg_len:
                    continue
                payload = data[i + 24 : i + msg_len - 4] if msg_len >= 28 else data[i + 24 : i + msg_len]
                crc_value = None
                if msg_len >= 28 and len(data) >= i + msg_len:
                    crc_value = struct.unpack("<I", data[i + msg_len - 4 : i + msg_len])[0]
                return {
                    "format": "binary",
                    "header": {
                        "message_id": header.message_id,
                        "message_length": header.message_length,
                    },
                    "payload": payload,
                    "crc": f"0x{crc_value:08X}" if crc_value is not None else None,
                    "message_offset": i,
                }
        return None

    try:
        text = data.decode("ascii", errors="ignore")
        start = text.find("#STADOPA")
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
        tokens = [f.strip() for f in re.split(r"[,\\r\\n]+", data_part) if f.strip() != ""]
        if len(tokens) < 11:
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

        time_of_week = _f(0)
        gdop = _f(1)
        pdop = _f(2)
        tdop = _f(3)
        vdop = _f(4)
        hdop = _f(5)
        ndop = _f(6)
        edop = _f(7)
        cutoff_angle = _f(8)
        reserved = _f(9)
        num_satellites = _i(10)
        prn_tokens = tokens[11:]
        prn_list = []
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
    except Exception:
        pass
    return None


def _parse_arddop_message(data: bytes, binary: bool = False) -> Optional[dict]:
    # Формат ARDDOP идентичен STADOP
    return _parse_stadop_message(data, binary)


def _parse_bestnavxyz_message(data: bytes, binary: bool = False) -> Optional[dict]:
    if binary:
        if len(data) < 24:
            return None
        payload_len = 110
        for i in range(len(data) - 24):
            if data[i] == 0xAA and data[i + 1] == 0x44 and data[i + 2] == 0xB5:
                header = parse_unicore_header(data[i : i + 24])
                if not header or header.message_id != 240:
                    continue
                offset = i + 24
                if len(data) < offset + payload_len:
                    continue

                p_sol_status = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                pos_type = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                p_x = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                p_y = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                p_z = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                p_x_sigma = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                p_y_sigma = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                p_z_sigma = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                v_sol_status = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                vel_type = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                v_x = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                v_y = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                v_z = struct.unpack("<d", data[offset : offset + 8])[0]
                offset += 8
                v_x_sigma = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                v_y_sigma = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                v_z_sigma = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4

                station_id = data[offset : offset + 4].decode("ascii", errors="ignore").rstrip("\x00")
                offset += 4
                v_latency = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                diff_age = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                sol_age = struct.unpack("<f", data[offset : offset + 4])[0]
                offset += 4
                num_sats_tracked = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                num_sats_used = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                num_l1_signals = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                num_multi_freq = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                reserved = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                ext_solution_status = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                galileo_bds3_mask = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1
                gps_glonass_bds2_mask = struct.unpack("<B", data[offset : offset + 1])[0]
                offset += 1

                msg_length = header.message_length
                crc_value = None
                if msg_length > 0 and len(data) >= i + msg_length:
                    crc_value = struct.unpack("<I", data[i + msg_length - 4 : i + msg_length])[0]

                return {
                    "format": "binary",
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
                        "num_L1_signals": num_l1_signals,
                        "num_multi_freq": num_multi_freq,
                        "reserved": reserved,
                        "ext_solution_status": ext_solution_status,
                        "ext_solution_status_hex": f"0x{ext_solution_status:02X}",
                        "galileo_bds3_mask": galileo_bds3_mask,
                        "galileo_bds3_mask_hex": f"0x{galileo_bds3_mask:02X}",
                        "gps_glonass_bds2_mask": gps_glonass_bds2_mask,
                        "gps_glonass_bds2_mask_hex": f"0x{gps_glonass_bds2_mask:02X}",
                    },
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
        pos = text.find("#BESTNAVXYZA")
        if pos < 0:
            pos = text.find("#BESTNAVXYZA,")
        if pos < 0:
            return None
        rest = text[pos:]
        end = rest.find("*")
        if end < 0:
            end = min(len(rest), 1500)
        line = rest[:end]
        if not line.strip().startswith("#BESTNAVXYZA"):
            return None
        parts = line.split(";", 1)
        if len(parts) < 2:
            return None
        data_part = parts[1].split("*")[0]
        fields = [f.strip() for f in re.split(r"[,\\r\\n]+", data_part) if f.strip()]
        if len(fields) < 25:
            return None

        def _f(idx: int, default: float = 0.0) -> float:
            if idx >= len(fields):
                return default
            try:
                return float(fields[idx])
            except ValueError:
                return default

        p_sol_status = fields[0]
        pos_type = fields[1] if len(fields) > 1 else ""
        p_x = _f(2)
        p_y = _f(3)
        p_z = _f(4)
        p_x_sigma = _f(5)
        p_y_sigma = _f(6)
        p_z_sigma = _f(7)

        v_sol_status = fields[8] if len(fields) > 8 else ""
        vel_type = fields[9] if len(fields) > 9 else ""
        v_x = _f(10)
        v_y = _f(11)
        v_z = _f(12)
        v_x_sigma = _f(13)
        v_y_sigma = _f(14)
        v_z_sigma = _f(15)

        station_id = fields[16] if len(fields) > 16 else ""
        v_latency = _f(17)
        diff_age = _f(18)
        sol_age = _f(19)

        return {
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
            },
            "raw": line,
        }
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
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
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
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
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
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
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
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
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
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
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
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
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
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
        check_complete=_check_stadop_complete,
        result_key="stadop",
    )


def query_arddop(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"ARDDOPB {rate}" if binary else f"ARDDOPA {rate}"
    return _run_data_query(
        core,
        command=command,
        parse_func=_parse_arddop_message,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
        check_complete=_check_arddop_complete,
        result_key="arddop",
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
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
        check_complete=_check_bestnavxyz_complete,
        result_key="bestnavxyz",
    )


def query_arddoph(
    core: Um982Core,
    rate: int = 1,
    binary: bool = False,
    add_crlf: Optional[bool] = None,
) -> Dict[str, Any]:
    command = f"ARDDOPHB {rate}" if binary else f"ARDDOPHA {rate}"
    check_complete = _make_unicore_header_checker(
        2121,
        min_length=0,
        ascii_tag=b"#ARDDOPHA",
        ascii_window=500,
        binary_min_total=500,
        ascii_min_total=200,
    )
    return _run_data_query(
        core,
        command=command,
        parse_func=lambda d, b: None,
        binary=binary,
        add_crlf=add_crlf,
        wait_time=0.5,
        read_attempts=5,
        read_timeout=0.6,
        check_complete=check_complete,
        result_key="arddoph",
    )
