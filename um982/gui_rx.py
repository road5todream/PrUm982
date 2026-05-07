"""
Фоновый разбор входящих кадров Unicore (0xAA 0x44 0xB5) для GUI.

Сопоставление message_id → парсер и ключ результата (как у query_* в um982_gui).
"""
from __future__ import annotations

import re
import struct
from typing import AbstractSet, Any, Callable, Dict, List, Optional, Tuple, Union

from um982.utils import parse_unicore_header
from um982.data_output.version_rx import parse_version_rx

UNICORE_SYNC = bytes((0xAA, 0x44, 0xB5))

# STADOP / ADRDOP / AGRIC / PVTSLN в ASCII часто несколько строк до «*CRC».
_DOP_MULTILINE_ASCII_RE = re.compile(
    rb"#(?:STADOP[AB]|ADRDOP[AB]|ADRDOPH[AB]|ARDDOPH[AB]|AGRIC[AB]|PVTSLN[AB]|OBSVBASE[AB])[\s\S]*?\*[0-9a-fA-F]{7,8}",
    re.IGNORECASE,
)
_MULTILINE_ASCII_PREFIX_RE = re.compile(
    rb"#(?:STADOP[AB]|ADRDOP[AB]|ADRDOPH[AB]|ARDDOPH[AB]|AGRIC[AB]|PVTSLN[AB]|OBSVBASE[AB])",
    re.IGNORECASE,
)
# Сообщения, для которых в ASCII используется многострочный regex выше (дорого — не гоняем, если не нужны).
_DOP_MULTILINE_RX_CMDS = frozenset(
    {"query_stadop", "query_adrdop", "query_adrdoph", "query_agric", "query_pvtsln", "query_obsvbase"}
)


def _parse_version_frame(data: bytes, binary: bool) -> Optional[dict]:
    """Один полный бинарный кадр VERSION (message_id 37)."""
    return parse_version_rx(data, True)

# message_id → (имя команды в GUI, ключ в словаре результата, парсер)
_Handler = Tuple[str, str, Callable[[bytes, bool], Optional[dict]]]
_HANDLERS: Optional[Dict[int, _Handler]] = None


def _load_handlers() -> Dict[int, _Handler]:
    global _HANDLERS
    if _HANDLERS is not None:
        return _HANDLERS

    from um982.data_output.observation import (
        _parse_obsvm_message,
        _parse_obsvh_message,
        _parse_obsvmcmp_message,
        _parse_obsvbase_message,
    )
    from um982.data_output.baseinfo import _parse_baseinfo_message
    from um982.data_output.pvt import _parse_pvtsln_message
    from um982.data_output.ionosphere import (
        _parse_gpsion_message,
        _parse_bdsion_message,
        _parse_galion_message,
        _parse_bd3ion_message,
    )
    from um982.data_output.time_utc import _parse_gpsutc_message, _parse_bd3utc_message
    from um982.data_output._commands import _parse_agric_message, _parse_hwstatus_message, _parse_agc_message
    from um982.data_output.nav import (
        _parse_bestnav_message,
        _parse_adrnav_message,
        _parse_pppnav_message,
        _parse_sppnav_message,
        _parse_stadop_message,
        _parse_adrdop_message,
        _parse_adrdoph_message,
        _parse_bestnavxyz_message,
    )

    _HANDLERS = {
        37: ("query_version", "version", _parse_version_frame),
        12: ("query_obsvm", "obsvm", _parse_obsvm_message),
        13: ("query_obsvh", "obsvh", _parse_obsvh_message),
        138: ("query_obsvmcmp", "obsvmcmp", _parse_obsvmcmp_message),
        284: ("query_obsvbase", "obsvbase", _parse_obsvbase_message),
        176: ("query_baseinfo", "baseinfo", _parse_baseinfo_message),
        1021: ("query_pvtsln", "pvtsln", _parse_pvtsln_message),
        8: ("query_gpsion", "gpsion", _parse_gpsion_message),
        # BDSION = message_id 4, GALION = 9 (см. ionosphere._ionosphere_complete_checker / парсеры).
        4: ("query_bdsion", "bdsion", _parse_bdsion_message),
        9: ("query_galion", "galion", _parse_galion_message),
        21: ("query_bd3ion", "bd3ion", _parse_bd3ion_message),
        19: ("query_gpsutc", "gpsutc", _parse_gpsutc_message),
        22: ("query_bd3utc", "bd3utc", _parse_bd3utc_message),
        11276: ("query_agric", "agric", _parse_agric_message),
        218: ("query_hwstatus", "hwstatus", _parse_hwstatus_message),
        220: ("query_agc", "agc", _parse_agc_message),
        2118: ("query_bestnav", "bestnav", _parse_bestnav_message),
        142: ("query_adrnav", "adrnav", _parse_adrnav_message),
        2117: ("query_adrnavh", "adrnavh", _parse_adrnav_message),
        1026: ("query_pppnav", "pppnav", _parse_pppnav_message),
        46: ("query_sppnav", "sppnav", _parse_sppnav_message),
        2116: ("query_sppnavh", "sppnavh", _parse_sppnav_message),
        954: ("query_stadop", "stadop", _parse_stadop_message),
        964: ("query_stadop", "stadop", _parse_stadop_message),
        953: ("query_adrdop", "adrdop", _parse_adrdop_message),
        963: ("query_adrdop", "adrdop", _parse_adrdop_message),
        2121: ("query_adrdoph", "adrdoph", _parse_adrdoph_message),
        240: ("query_bestnavxyz", "bestnavxyz", _parse_bestnavxyz_message),
    }
    return _HANDLERS


def _try_peel_obsv_like_frame(buf: bytearray, hdr: Any) -> Optional[Tuple[bytes, int]]:
    """
    Длина бинарных OBSVM/OBSVH/OBSVBASE/OBSVMCMP в заголовке на части прошивок бывает 0 или не совпадает
    с фактической (особенно при obs_number==0). Тогда общий peel по message_length ломает поток:
    байт sync снимается, кадр никогда не парсится. Пробуем длину из поля числа наблюдений + фикс. записи + CRC.
    """
    mid = int(hdr.message_id)
    if mid == 138:
        rec_sz = 24
        from um982.data_output.observation import _parse_obsvmcmp_message as _pfn
    elif mid == 284:
        rec_sz = 40
        from um982.data_output.observation import _parse_obsvbase_message as _pfn
    elif mid == 13:
        rec_sz = 40
        from um982.data_output.observation import _parse_obsvh_message as _pfn
    elif mid == 12:
        rec_sz = 40
        from um982.data_output.observation import _parse_obsvm_message as _pfn
    else:
        return None

    if len(buf) < 28:
        return None
    try:
        obs_n = struct.unpack("<I", bytes(buf[24:28]))[0]
    except Exception:
        return None
    if obs_n > 2048:
        return None

    computed = 24 + 4 + rec_sz * obs_n + 4
    ml = int(hdr.message_length)
    seen: set[int] = set()
    candidates: List[int] = []
    for L in (computed, ml, ml + 4, ml - 4, computed - 4):
        if L not in seen:
            seen.add(L)
            candidates.append(L)

    need_more = False
    for total_len in candidates:
        if total_len < 28 or total_len > 65536:
            continue
        if len(buf) < total_len:
            need_more = True
            continue
        frame = bytes(buf[:total_len])
        try:
            parsed = _pfn(frame, True)
        except Exception:
            parsed = None
        if parsed:
            del buf[:total_len]
            return frame, mid

    return None


def peel_one_unicore_frame(buf: bytearray) -> Optional[Tuple[bytes, int]]:
    """Удалить из начала buf один полный кадр Unicore или вернуть None (нужно больше данных)."""
    if len(buf) < 24:
        return None
    if buf[:3] != UNICORE_SYNC:
        idx = buf.find(UNICORE_SYNC)
        if idx < 0:
            if len(buf) > 65536:
                del buf[: len(buf) - 65536]
            return None
        del buf[:idx]
        if len(buf) < 24:
            return None
    hdr = parse_unicore_header(bytes(buf[:24]))
    if hdr is None:
        del buf[0]
        return None

    # OBSV*: при неверном или нулевом message_length общая ветка ниже отбрасывает sync и кадры теряются.
    if hdr.message_id in (12, 13, 138, 284):
        peeled = _try_peel_obsv_like_frame(buf, hdr)
        if peeled is not None:
            return peeled
        if hdr.message_length <= 0:
            # Не откусываем 0xAA: длина в заголовке неверна — ждём байты или сработает повтор ниже.
            return None

    if hdr.message_length <= 0:
        del buf[0]
        return None
    # AGRIC (11276): поле message_length на устройствах может означать только payload (228/232),
    # не полный кадр — иначе peel откусывает буфер слишком рано и парсер получает обрезок.
    if hdr.message_id == 11276:
        from um982.data_output._commands import _agric_binary_total_len_candidates, _agric_try_unpack_frame

        trip = _agric_try_unpack_frame(bytes(buf), 0, hdr.message_length)
        if trip is not None:
            _, total_len, _ = trip
            frame = bytes(buf[:total_len])
            del buf[:total_len]
            return frame, hdr.message_id
        candidates = _agric_binary_total_len_candidates(hdr.message_length)
        if any(len(buf) < t for t in candidates):
            return None
        del buf[0]
        return None
    if hdr.message_id == 1021:
        from um982.data_output.pvt import _pvtsln_try_unpack_frame

        trip = _pvtsln_try_unpack_frame(bytes(buf), 0, hdr.message_length)
        if trip is not None:
            _, total_len, _ = trip
            frame = bytes(buf[:total_len])
            del buf[:total_len]
            return frame, hdr.message_id
        from um982.data_output.pvt import _pvtsln_total_len_candidates

        cand = _pvtsln_total_len_candidates(hdr.message_length)
        if any(len(buf) < t for t in cand):
            return None
        del buf[0]
        return None
    if hdr.message_id == 218:
        from um982.data_output._commands import _hwstatus_binary_total_len_candidates, _hwstatus_try_unpack_frame

        trip = _hwstatus_try_unpack_frame(bytes(buf), 0, hdr.message_length)
        if trip is not None:
            _, total_len, _ = trip
            frame = bytes(buf[:total_len])
            del buf[:total_len]
            return frame, hdr.message_id
        cands = _hwstatus_binary_total_len_candidates(hdr.message_length)
        if cands:
            mx = max(cands)
            if len(buf) < mx:
                return None
        del buf[0]
        return None
    if hdr.message_id == 220:
        from um982.data_output._commands import _parse_agc_message

        ml = int(hdr.message_length)
        seen220: set[int] = set()
        cands220: List[int] = []
        for L in (ml, ml + 24, 24 + 24, 44, 48):
            if 40 <= L <= 4096 and L not in seen220:
                seen220.add(L)
                cands220.append(L)
        need_more220 = False
        for total_len in cands220:
            if len(buf) < total_len:
                need_more220 = True
                continue
            frame = bytes(buf[:total_len])
            try:
                parsed220 = _parse_agc_message(frame, True)
            except Exception:
                parsed220 = None
            if parsed220:
                del buf[:total_len]
                return frame, hdr.message_id
        if need_more220:
            return None
        del buf[0]
        return None
    if hdr.message_id == 176:
        from um982.data_output.baseinfo import _parse_baseinfo_message

        ml = int(hdr.message_length)
        seen176: set[int] = set()
        cands176: List[int] = []
        for L in (ml, ml + 24, 64, 68):
            if 56 <= L <= 4096 and L not in seen176:
                seen176.add(L)
                cands176.append(L)
        need_more176 = False
        for total_len in cands176:
            if len(buf) < total_len:
                need_more176 = True
                continue
            frame = bytes(buf[:total_len])
            try:
                parsed176 = _parse_baseinfo_message(frame, True)
            except Exception:
                parsed176 = None
            if parsed176:
                del buf[:total_len]
                return frame, hdr.message_id
        if need_more176:
            return None
        del buf[0]
        return None
    if hdr.message_id == 240:
        from um982.data_output.nav import _bestnavxyz_binary_total_len_candidates, _bestnavxyz_try_unpack_frame

        trip = _bestnavxyz_try_unpack_frame(bytes(buf), 0, hdr.message_length)
        if trip is not None:
            _, total_len, _ = trip
            frame = bytes(buf[:total_len])
            del buf[:total_len]
            return frame, hdr.message_id
        cands = _bestnavxyz_binary_total_len_candidates(hdr.message_length)
        if cands:
            mx = max(cands)
            if len(buf) < mx:
                return None
        del buf[0]
        return None
    if hdr.message_id == 2118:
        from um982.data_output.nav import _parse_bestnav_message

        cands: List[int] = []
        seen: set[int] = set()

        def _add_len(x: int) -> None:
            if 52 <= x <= 8192 and x not in seen:
                seen.add(x)
                cands.append(x)

        ml = int(hdr.message_length)
        _add_len(ml)
        _add_len(ml + 4)
        _add_len(ml + 24)
        _add_len(ml + 28)
        _add_len(24 + 120 + 4)

        need_more = False
        for total_len in cands:
            if len(buf) < total_len:
                need_more = True
                continue
            frame = bytes(buf[:total_len])
            try:
                parsed = _parse_bestnav_message(frame, True)
            except Exception:
                parsed = None
            if parsed:
                del buf[:total_len]
                return frame, hdr.message_id
        if need_more:
            return None
        del buf[0]
        return None
    if hdr.message_id == 37:
        cands: List[int] = []
        seen: set[int] = set()

        def _add_len(x: int) -> None:
            if 52 <= x <= 8192 and x not in seen:
                seen.add(x)
                cands.append(x)

        ml = int(hdr.message_length)
        _add_len(ml)
        _add_len(ml + 4)
        _add_len(336)
        _add_len(340)
        _add_len(24 + 308 + 4)

        need_more = False
        for total_len in cands:
            if len(buf) < total_len:
                need_more = True
                continue
            frame = bytes(buf[:total_len])
            try:
                parsed = _parse_version_frame(frame, True)
            except Exception:
                parsed = None
            if parsed:
                del buf[:total_len]
                return frame, hdr.message_id
        if need_more:
            return None
        del buf[0]
        return None
    # GPSUTC (19) §7.3.12: после заголовка 52 байта (поля + CRC). В поле длины иногда 52 (только тело),
    # иногда 76 (полный кадр от sync) — общая ветка с L=52 откусывает обрезок.
    if hdr.message_id == 19:
        from um982.data_output.time_utc import _parse_gpsutc_message

        ml = int(hdr.message_length)
        seen19: set[int] = set()
        cands19: List[int] = []
        for L in (ml, ml + 24, 24 + 52):
            if 52 <= L <= 4096 and L not in seen19:
                seen19.add(L)
                cands19.append(L)
        need_more19 = False
        for total_len in cands19:
            if len(buf) < total_len:
                need_more19 = True
                continue
            frame = bytes(buf[:total_len])
            try:
                parsed19 = _parse_gpsutc_message(frame, True)
            except Exception:
                parsed19 = None
            if parsed19:
                del buf[:total_len]
                return frame, hdr.message_id
        if need_more19:
            return None
        del buf[0]
        return None
    # BD3UTC (22) §7.3.13: после заголовка 60 байт (поля + CRC); поле длины может быть 60 или 84 (полный кадр).
    if hdr.message_id == 22:
        from um982.data_output.time_utc import _parse_bd3utc_message

        ml = int(hdr.message_length)
        seen22: set[int] = set()
        cands22: List[int] = []
        for L in (ml, ml + 24, 24 + 60):
            if 52 <= L <= 4096 and L not in seen22:
                seen22.add(L)
                cands22.append(L)
        need_more22 = False
        for total_len in cands22:
            if len(buf) < total_len:
                need_more22 = True
                continue
            frame = bytes(buf[:total_len])
            try:
                parsed22 = _parse_bd3utc_message(frame, True)
            except Exception:
                parsed22 = None
            if parsed22:
                del buf[:total_len]
                return frame, hdr.message_id
        if need_more22:
            return None
        del buf[0]
        return None
    # ADRNAV (142) / ADRNAVH (2117) §7.3.29: 116 байт полезной нагрузки + CRC; старые прошивки могли отдавать 120+4 (BESTNAV-подобно).
    if hdr.message_id in (142, 2117):
        from um982.data_output.nav import _parse_adrnav_message

        ml = int(hdr.message_length)
        seen_adr: set[int] = set()
        cands_adr: List[int] = []
        for L in (ml, ml + 24, 24 + 120, 144, 148):
            if 64 <= L <= 8192 and L not in seen_adr:
                seen_adr.add(L)
                cands_adr.append(L)
        need_more_adr = False
        for total_len in cands_adr:
            if len(buf) < total_len:
                need_more_adr = True
                continue
            frame = bytes(buf[:total_len])
            try:
                parsed_adr = _parse_adrnav_message(frame, True)
            except Exception:
                parsed_adr = None
            if parsed_adr:
                del buf[:total_len]
                return frame, hdr.message_id
        if need_more_adr:
            return None
        del buf[0]
        return None
    # PPPNAV (1026) §7.3.31: 72 байта полей + 4 CRC после заголовка (76); полный кадр от sync часто 100.
    if hdr.message_id == 1026:
        from um982.data_output.nav import _parse_pppnav_message

        ml = int(hdr.message_length)
        seen_ppp: set[int] = set()
        cands_ppp: List[int] = []
        for L in (ml, ml + 24, 24 + 76, 100, 104):
            if 64 <= L <= 8192 and L not in seen_ppp:
                seen_ppp.add(L)
                cands_ppp.append(L)
        need_more_ppp = False
        for total_len in cands_ppp:
            if len(buf) < total_len:
                need_more_ppp = True
                continue
            frame = bytes(buf[:total_len])
            try:
                parsed_ppp = _parse_pppnav_message(frame, True)
            except Exception:
                parsed_ppp = None
            if parsed_ppp:
                del buf[:total_len]
                return frame, hdr.message_id
        if need_more_ppp:
            return None
        del buf[0]
        return None
    # SPPNAV (46) / SPPNAVH (2116) §7.3.32: 120 байт полей + 4 CRC (как BESTNAV); полный кадр от sync часто 148.
    if hdr.message_id in (46, 2116):
        from um982.data_output.nav import _parse_sppnav_message

        ml = int(hdr.message_length)
        seen_spp: set[int] = set()
        cands_spp: List[int] = []
        for L in (ml, ml + 24, 24 + 124, 148, 152):
            if 64 <= L <= 8192 and L not in seen_spp:
                seen_spp.add(L)
                cands_spp.append(L)
        need_more_spp = False
        for total_len in cands_spp:
            if len(buf) < total_len:
                need_more_spp = True
                continue
            frame = bytes(buf[:total_len])
            try:
                parsed_spp = _parse_sppnav_message(frame, True)
            except Exception:
                parsed_spp = None
            if parsed_spp:
                del buf[:total_len]
                return frame, hdr.message_id
        if need_more_spp:
            return None
        del buf[0]
        return None
    # DOP (STADOP/ADRDOP): длина зависит от #PRN, а message_length на прошивках встречается в двух трактовках.
    if hdr.message_id in (954, 964, 953, 963, 2121):
        handlers = _load_handlers()
        h = handlers.get(hdr.message_id)
        if h:
            _, _, parse_fn = h
            ml = int(hdr.message_length)
            seen_dop: set[int] = set()
            cands_dop: List[int] = []

            def _add_dop_len(v: int) -> None:
                if 52 <= v <= 8192 and v not in seen_dop:
                    seen_dop.add(v)
                    cands_dop.append(v)

            _add_dop_len(ml)
            _add_dop_len(ml + 24)
            # Формулы из бинарного layout: total = 24 + (42|38) + 2*#PRN + 4.
            if len(buf) >= 24 + 42:
                n = struct.unpack("<H", bytes(buf[24 + 40 : 24 + 42]))[0]
                if 0 <= n <= 255:
                    _add_dop_len(24 + 42 + 2 * n + 4)
            if len(buf) >= 24 + 38:
                n2 = struct.unpack("<H", bytes(buf[24 + 36 : 24 + 38]))[0]
                if 0 <= n2 <= 255:
                    _add_dop_len(24 + 38 + 2 * n2 + 4)

            need_more_dop = False
            for total_len in cands_dop:
                if len(buf) < total_len:
                    need_more_dop = True
                    continue
                frame = bytes(buf[:total_len])
                try:
                    parsed_dop = parse_fn(frame, True)
                except Exception:
                    parsed_dop = None
                if parsed_dop:
                    del buf[:total_len]
                    return frame, hdr.message_id
            if need_more_dop:
                return None
        del buf[0]
        return None
    # Ионосфера (4 BDSION, 8 GPSION, 9 GALION, 21 BD3ION): message_length на прошивках часто не совпадает
    # с фактом — общий peel откусывает короткий кусок, парсер не срабатывает.
    if hdr.message_id in (4, 8, 9, 21):
        handlers = _load_handlers()
        h = handlers.get(hdr.message_id)
        if h:
            _, _, parse_fn = h
            ml = int(hdr.message_length)
            seen: set[int] = set()
            cands: List[int] = []
            for L in (ml, ml + 4, ml - 4, ml + 8, ml - 8, ml + 24, ml + 28):
                if 52 <= L <= 4096 and L not in seen:
                    seen.add(L)
                    cands.append(L)
            for nom in range(52, 132, 2):
                if nom not in seen:
                    seen.add(nom)
                    cands.append(nom)
            need_more = False
            for total_len in cands:
                if len(buf) < total_len:
                    need_more = True
                    continue
                frame = bytes(buf[:total_len])
                try:
                    parsed = parse_fn(frame, True)
                except Exception:
                    parsed = None
                if parsed:
                    del buf[:total_len]
                    return frame, hdr.message_id
            if need_more:
                return None
            del buf[0]
            return None
    # Дальше — общий случай: длина кадра = hdr.message_length от начала sync.
    # На части типов (BASEINFO, ION, NAV, DOP, …) поле length в заголовке не совпадает с фактическим
    # размером кадра — peel откусывает мало/много байт, парсер не срабатывает (в отличие от OBSV*/AGRIC,
    # где выше свои ветки с подбором длины / unpack).
    L = hdr.message_length
    if len(buf) < L:
        return None
    frame = bytes(buf[:L])
    del buf[:L]
    return frame, hdr.message_id


_ASCII_HANDLERS: Optional[List[Tuple[bytes, str, str, Callable[[bytes, bool], Optional[dict]]]]] = None


def _load_ascii_handlers() -> List[Tuple[bytes, str, str, Callable[[bytes, bool], Optional[dict]]]]:
    """Префиксы ASCII-строк (длинные первыми), те же парсеры что у query_*, binary=False."""
    global _ASCII_HANDLERS
    if _ASCII_HANDLERS is not None:
        return _ASCII_HANDLERS

    from um982.data_output.observation import (
        _parse_obsvm_message,
        _parse_obsvh_message,
        _parse_obsvmcmp_message,
        _parse_obsvbase_message,
    )
    from um982.data_output.baseinfo import _parse_baseinfo_message
    from um982.data_output.pvt import _parse_pvtsln_message
    from um982.data_output.ionosphere import (
        _parse_gpsion_message,
        _parse_bdsion_message,
        _parse_galion_message,
        _parse_bd3ion_message,
    )
    from um982.data_output.time_utc import _parse_gpsutc_message, _parse_bd3utc_message
    from um982.data_output._commands import _parse_agric_message, _parse_hwstatus_message, _parse_agc_message
    from um982.data_output.logging import _parse_uniloglist_message
    from um982.data_output.nav import (
        _parse_bestnav_message,
        _parse_adrnav_message,
        _parse_pppnav_message,
        _parse_sppnav_message,
        _parse_stadop_message,
        _parse_adrdop_message,
        _parse_adrdoph_message,
        _parse_bestnavxyz_message,
    )

    pairs: List[Tuple[bytes, str, str, Callable[[bytes, bool], Optional[dict]]]] = [
        (b"#BESTNAVXYZA", "query_bestnavxyz", "bestnavxyz", _parse_bestnavxyz_message),
        (b"#BESTNAVA", "query_bestnav", "bestnav", _parse_bestnav_message),
        (b"#BESTNAVXYZB", "query_bestnavxyz", "bestnavxyz", _parse_bestnavxyz_message),
        (b"#BESTNAVB", "query_bestnav", "bestnav", _parse_bestnav_message),
        (b"#VERSIONA", "query_version", "version", parse_version_rx),
        (b"#ADRNAVA", "query_adrnav", "adrnav", _parse_adrnav_message),
        (b"#ADRNAVB", "query_adrnav", "adrnav", _parse_adrnav_message),
        (b"#ADRNAVHA", "query_adrnavh", "adrnavh", _parse_adrnav_message),
        (b"#ADRNAVHB", "query_adrnavh", "adrnavh", _parse_adrnav_message),
        (b"#PPPNAVA", "query_pppnav", "pppnav", _parse_pppnav_message),
        (b"#PPPNAVB", "query_pppnav", "pppnav", _parse_pppnav_message),
        (b"#SPPNAVA", "query_sppnav", "sppnav", _parse_sppnav_message),
        (b"#SPPNAVB", "query_sppnav", "sppnav", _parse_sppnav_message),
        (b"#SPPNAVHA", "query_sppnavh", "sppnavh", _parse_sppnav_message),
        (b"#SPPNAVHB", "query_sppnavh", "sppnavh", _parse_sppnav_message),
        (b"#STADOPA", "query_stadop", "stadop", _parse_stadop_message),
        (b"#STADOPB", "query_stadop", "stadop", _parse_stadop_message),
        (b"#ADRDOPA", "query_adrdop", "adrdop", _parse_adrdop_message),
        (b"#ADRDOPB", "query_adrdop", "adrdop", _parse_adrdop_message),
        (b"#ADRDOPHA", "query_adrdoph", "adrdoph", _parse_adrdoph_message),
        (b"#ADRDOPHB", "query_adrdoph", "adrdoph", _parse_adrdoph_message),
        # Совместимость с исторической опечаткой ARDDOPH*.
        (b"#ARDDOPHA", "query_adrdoph", "adrdoph", _parse_adrdoph_message),
        (b"#ARDDOPHB", "query_adrdoph", "adrdoph", _parse_adrdoph_message),
        (b"#OBSVMA", "query_obsvm", "obsvm", _parse_obsvm_message),
        (b"#OBSVMB", "query_obsvm", "obsvm", _parse_obsvm_message),
        (b"#OBSVHA", "query_obsvh", "obsvh", _parse_obsvh_message),
        (b"#OBSVHB", "query_obsvh", "obsvh", _parse_obsvh_message),
        (b"#OBSVMCMPA", "query_obsvmcmp", "obsvmcmp", _parse_obsvmcmp_message),
        (b"#OBSVMCMPB", "query_obsvmcmp", "obsvmcmp", _parse_obsvmcmp_message),
        (b"#OBSVBASEA", "query_obsvbase", "obsvbase", _parse_obsvbase_message),
        (b"#OBSVBASEB", "query_obsvbase", "obsvbase", _parse_obsvbase_message),
        (b"#BASEINFOA", "query_baseinfo", "baseinfo", _parse_baseinfo_message),
        (b"#BASEINFOB", "query_baseinfo", "baseinfo", _parse_baseinfo_message),
        (b"#PVTSLNA", "query_pvtsln", "pvtsln", _parse_pvtsln_message),
        (b"#PVTSLNB", "query_pvtsln", "pvtsln", _parse_pvtsln_message),
        (b"#GPSIONA", "query_gpsion", "gpsion", _parse_gpsion_message),
        (b"#GPSIONB", "query_gpsion", "gpsion", _parse_gpsion_message),
        (b"#BDSIONA", "query_bdsion", "bdsion", _parse_bdsion_message),
        (b"#BDSIONB", "query_bdsion", "bdsion", _parse_bdsion_message),
        (b"#GALIONA", "query_galion", "galion", _parse_galion_message),
        (b"#GALIONB", "query_galion", "galion", _parse_galion_message),
        (b"#BD3IONA", "query_bd3ion", "bd3ion", _parse_bd3ion_message),
        (b"#BD3IONB", "query_bd3ion", "bd3ion", _parse_bd3ion_message),
        (b"#GPSUTCA", "query_gpsutc", "gpsutc", _parse_gpsutc_message),
        (b"#GPSUTCB", "query_gpsutc", "gpsutc", _parse_gpsutc_message),
        (b"#BD3UTCA", "query_bd3utc", "bd3utc", _parse_bd3utc_message),
        (b"#BD3UTCB", "query_bd3utc", "bd3utc", _parse_bd3utc_message),
        (b"#AGRICA", "query_agric", "agric", _parse_agric_message),
        (b"#AGRICB", "query_agric", "agric", _parse_agric_message),
        (b"#HWSTATUSA", "query_hwstatus", "hwstatus", _parse_hwstatus_message),
        (b"#HWSTATUSB", "query_hwstatus", "hwstatus", _parse_hwstatus_message),
        (b"#AGCA", "query_agc", "agc", _parse_agc_message),
        (b"#AGCB", "query_agc", "agc", _parse_agc_message),
        (b"#UNILOGLIST", "query_uniloglist", "uniloglist", _parse_uniloglist_message),
    ]
    pairs.sort(key=lambda x: len(x[0]), reverse=True)
    _ASCII_HANDLERS = pairs
    return pairs


def _trim_leading_junk_before_ascii_log(buf: bytearray, *, parse_binary: bool) -> None:
    """В режиме «только ASCII» снять префикс до первого '#', если это не начало Unicore sync."""
    if parse_binary:
        return
    while buf and buf[0] in (9, 10, 13, 32):
        del buf[0]
    if not buf or buf[0] == ord("#"):
        return
    if len(buf) >= 3 and buf[:3] == UNICORE_SYNC:
        return
    pos = buf.find(b"#")
    if pos > 0:
        del buf[:pos]


def _last_ascii_line_terminator(buf: Union[bytes, bytearray]) -> int:
    """Индекс последнего \\n или \\r в буфере (-1 если строка ещё не завершена). Unicore часто шлёт только \\r."""
    n = buf.rfind(b"\n")
    r = buf.rfind(b"\r")
    return n if n > r else r


def _process_ascii_lines(
    buf: bytearray,
    accept_commands: Optional[AbstractSet[str]] = None,
) -> List[Tuple[str, Dict[str, Any]]]:
    """Разбор полных ASCII-строк #... до перевода строки \\n или \\r (типичный вывод после LOG)."""
    out: List[Tuple[str, Dict[str, Any]]] = []
    last_end = _last_ascii_line_terminator(buf)
    if last_end < 0:
        return out
    block = buf[: last_end + 1]
    del buf[: last_end + 1]

    from um982.data_output.nav import (
        _parse_adrdop_message,
        _parse_adrdoph_message,
        _parse_stadop_message,
    )
    from um982.data_output._commands import _parse_agric_message
    from um982.data_output.pvt import _parse_pvtsln_message
    from um982.data_output.observation import (
        OBSVMCMP_ASCII_MESSAGE_RE,
        _parse_obsvmcmp_message,
        _parse_obsvbase_message,
    )

    if accept_commands is not None and "query_obsvmcmp" not in accept_commands:
        cmp_matches: List[Any] = []
    else:
        cmp_matches = list(OBSVMCMP_ASCII_MESSAGE_RE.finditer(block))
    for m in cmp_matches:
        frag = m.group(0)
        try:
            parsed = _parse_obsvmcmp_message(frag, False)
            if parsed:
                out.append(("query_obsvmcmp", {"obsvmcmp": parsed}))
        except Exception:
            pass
    if cmp_matches:
        cut = bytearray(block)
        for m in reversed(cmp_matches):
            del cut[m.start() : m.end()]
        block = bytes(cut)

    if accept_commands is not None and accept_commands.isdisjoint(_DOP_MULTILINE_RX_CMDS):
        dop_matches = []
    else:
        dop_matches = list(_DOP_MULTILINE_ASCII_RE.finditer(block))
    for m in dop_matches:
        frag = m.group(0)
        try:
            if re.match(rb"#STADOP", frag, re.I):
                parsed = _parse_stadop_message(frag, False)
                if parsed:
                    out.append(("query_stadop", {"stadop": parsed}))
            elif re.match(rb"#ADRDOPH", frag, re.I) or re.match(rb"#ARDDOPH", frag, re.I):
                parsed = _parse_adrdoph_message(frag, False)
                if parsed:
                    out.append(("query_adrdoph", {"adrdoph": parsed}))
            elif re.match(rb"#ADRDOP", frag, re.I):
                parsed = _parse_adrdop_message(frag, False)
                if parsed:
                    out.append(("query_adrdop", {"adrdop": parsed}))
            elif re.match(rb"#AGRIC", frag, re.I):
                parsed = _parse_agric_message(frag, False)
                if parsed:
                    out.append(("query_agric", {"agric": parsed}))
            elif re.match(rb"#PVTSLN", frag, re.I):
                parsed = _parse_pvtsln_message(frag, False)
                if parsed:
                    out.append(("query_pvtsln", {"pvtsln": parsed}))
            elif re.match(rb"#OBSVBASE", frag, re.I):
                parsed = _parse_obsvbase_message(frag, False)
                if parsed:
                    out.append(("query_obsvbase", {"obsvbase": parsed}))
        except Exception:
            pass
    if dop_matches:
        cut = bytearray(block)
        for m in reversed(dop_matches):
            del cut[m.start() : m.end()]
        block = bytes(cut)
    elif _MULTILINE_ASCII_PREFIX_RE.search(block):
        # Есть начало многострочного сообщения, но ещё нет *CRC — ждём следующие куски, не теряя буфер.
        buf[:0] = block
        return out

    handlers = _load_ascii_handlers()
    for raw in block.splitlines():  # \\n, \\r, \\r\\n — как у реального COM
        line = raw.strip()
        if not line.startswith(b"#"):
            continue
        for prefix, cmd, key, parse_fn in handlers:
            if line.startswith(prefix):
                if accept_commands is not None and cmd not in accept_commands:
                    break
                try:
                    parsed = parse_fn(line, False)
                except Exception:
                    parsed = None
                if parsed:
                    out.append((cmd, {key: parsed}))
                break
    return out


def _drain_ascii_lines(buf: bytearray) -> None:
    """Удалить из буфера завершённые по \\n или \\r ASCII-строки без разбора (режим «только binary»)."""
    last_end = _last_ascii_line_terminator(buf)
    if last_end < 0:
        return
    del buf[: last_end + 1]


def _ascii_starts_multiline_log(buf: bytes) -> bool:
    """Логи, для которых нужен перевод строки / многострочный разбор (не режем по первому *CRC)."""
    s = buf.lstrip(b" \t\r\n").upper()
    return bool(
        s.startswith(b"#AGRIC")
        or s.startswith(b"#STADOP")
        or s.startswith(b"#ADRDOP")
        or s.startswith(b"#ADRDOPH")
        or s.startswith(b"#ARDDOPH")
        or s.startswith(b"#PVTSLN")
        or s.startswith(b"#OBSVBASE")
    )


def _process_ascii_crc_idle(
    buf: bytearray,
    accept_commands: Optional[AbstractSet[str]] = None,
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Однострочные #...*xxxxxxxx без завершающего \\r/\\n: read_response часто возвращает кусок ровно до CRC,
    тогда _process_ascii_lines не снимает буфер. Ищем * + 8 hex и безопасный хвост.
    """
    out: List[Tuple[str, Dict[str, Any]]] = []
    if _last_ascii_line_terminator(buf) >= 0:
        return out
    if not buf:
        return out
    if _ascii_starts_multiline_log(bytes(buf)):
        return out

    handlers = _load_ascii_handlers()
    while True:
        j = 0
        while j < len(buf) and buf[j] in (9, 10, 13, 32):
            j += 1
        if j > 0:
            del buf[:j]
        if not buf or buf[0] != ord("#"):
            break
        star = -1
        search = 1
        while search < len(buf):
            i = buf.find(b"*", search)
            if i < 0:
                break
            if i + 9 > len(buf):
                break
            if re.fullmatch(br"[0-9a-fA-F]{8}", bytes(buf[i + 1 : i + 9]), flags=re.I):
                star = i
                break
            search = i + 1
        if star < 0:
            break
        end = star + 9
        while end < len(buf) and buf[end] in (9, 10, 13, 32):
            end += 1
        # После *CRC часто идёт NMEA ($…), бинарный кадр (0xAA…) или конец буфера — иначе буфер
        # никогда не снимается и непрерывный ASCII-парс (в т.ч. #OBSVMA…*crc) «застывает».
        if end < len(buf):
            nxt = buf[end]
            if nxt not in (ord("#"), ord("$"), 0xAA):
                break
        msg = bytes(buf[:end])
        line = msg.strip()
        if not line.startswith(b"#"):
            break
        parsed: Optional[dict] = None
        matched = False
        filtered_out = False
        for prefix, cmd, key, parse_fn in handlers:
            if line.startswith(prefix):
                matched = True
                if accept_commands is not None and cmd not in accept_commands:
                    filtered_out = True
                    break
                try:
                    parsed = parse_fn(line, False)
                except Exception:
                    parsed = None
                if parsed:
                    out.append((cmd, {key: parsed}))
                break
        if not matched:
            break
        if filtered_out:
            del buf[:end]
            continue
        if not parsed:
            break
        del buf[:end]
    return out


def process_rx_buffer(
    buf: bytearray,
    *,
    parse_binary: bool = True,
    parse_ascii: bool = True,
    accept_commands: Optional[AbstractSet[str]] = None,
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Извлечь из буфера бинарные кадры Unicore (0xAA 0x44 0xB5) и/или ASCII-строки #...

    Кадры Unicore снимаются с начала буфера. В режиме «только ASCII» не вызываем поиск sync по всему буферу
    (иначе возможен сдвиг/потеря префикса до первого AA 44 B5 внутри данных).

    parse_binary=False — отбрасываем только кадры, которые начинаются с sync (без парсинга в UI).
    parse_ascii=False — завершённые по \\n/\\r строки удаляются без парсинга (режим «только binary»).

    accept_commands — если задан непустой набор id команд GUI (например frozenset({'query_bestnav'})),
    полный разбор выполняется только для них; остальные кадры/строки снимаются с буфера без parse_fn
    (экономия CPU при нескольких LOG на разных типах сообщений).
    """
    handlers = _load_handlers()
    out: List[Tuple[str, Dict[str, Any]]] = []
    if parse_binary:
        while True:
            peeled = peel_one_unicore_frame(buf)
            if peeled is None:
                break
            frame, mid = peeled
            h = handlers.get(mid)
            if not h:
                continue
            cmd, key, parse_fn = h
            if accept_commands is not None and cmd not in accept_commands:
                continue
            try:
                parsed = parse_fn(frame, True)
            except Exception:
                parsed = None
            if parsed:
                out.append((cmd, {key: parsed}))
    else:
        while len(buf) >= 3 and buf[:3] == UNICORE_SYNC:
            peeled = peel_one_unicore_frame(buf)
            if peeled is not None:
                continue
            # Неполный кадр в начале: дальше по UART может идти ASCII (смена LOG/режима),
            # тогда без сдвига _process_ascii_* никогда не увидит '#'.
            hdr = parse_unicore_header(bytes(buf[:24]))
            if hdr is None or hdr.message_length <= 0:
                del buf[0]
                continue
            L = int(hdr.message_length)
            hash_pos = buf.find(b"#", 3)
            if 0 < hash_pos < len(buf) and len(buf) < L:
                del buf[:hash_pos]
                continue
            stall_limit = min(max(L + 4096, 24 + 4096), 262144)
            if len(buf) > stall_limit:
                del buf[0]
                continue
            break
    if parse_ascii:
        _trim_leading_junk_before_ascii_log(buf, parse_binary=parse_binary)
        out.extend(_process_ascii_lines(buf, accept_commands=accept_commands))
        out.extend(_process_ascii_crc_idle(buf, accept_commands=accept_commands))
    else:
        _drain_ascii_lines(buf)
    return out
