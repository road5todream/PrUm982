"""
Порядок и группировка полей формы «Текущее сообщение» для команд категории Data Output.

Ключи совпадают с подписями в format_data_for_table (um982_gui).
"""
from __future__ import annotations

import re
from typing import Any, Dict, FrozenSet, List, Optional, Tuple, Union

# Внутренние имена команд из списка «Data Output» в GUI
DATA_OUTPUT_QUERY_COMMANDS: FrozenSet[str] = frozenset(
    {
        "query_version",
        "query_obsvm",
        "query_obsvh",
        "query_obsvmcmp",
        "query_obsvbase",
        "query_baseinfo",
        "query_gpsion",
        "query_bdsion",
        "query_bd3ion",
        "query_galion",
        "query_gpsutc",
        "query_bd3utc",
        "query_adrnav",
        "query_adrnavh",
        "query_pppnav",
        "query_sppnav",
        "query_sppnavh",
        "query_stadop",
        "query_adrdop",
        "query_adrdoph",
        "query_agric",
        "query_pvtsln",
        "query_uniloglist",
        "query_bestnav",
        "query_bestnavxyz",
        "query_hwstatus",
        "query_agc",
    }
)

# Все query-команды, для которых в GUI задан осмысленный порядок/секции формы
FORM_VIEW_QUERY_COMMANDS: FrozenSet[str] = DATA_OUTPUT_QUERY_COMMANDS | frozenset(
    {
        "query_mode",
        "query_mask",
        "query_config",
    }
)

LayoutItem = Union[str, Tuple[str, str]]  # ("section", "Заголовок") или ключ поля

_GION_GPS_BDS_KEYS = (
    "Формат",
    "Alpha a0",
    "Alpha a1",
    "Alpha a2",
    "Alpha a3",
    "Beta b0",
    "Beta b1",
    "Beta b2",
    "Beta b3",
    "SVID",
    "Week",
    "Second",
    "Reserved",
)

_GION_GPS_BDS_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    ("section", "Коэффициенты α"),
    "Alpha a0",
    "Alpha a1",
    "Alpha a2",
    "Alpha a3",
    ("section", "Коэффициенты β"),
    "Beta b0",
    "Beta b1",
    "Beta b2",
    "Beta b3",
    ("section", "Время и идентификаторы"),
    "SVID",
    "Week",
    "Second",
    "Reserved",
)

_GALION_KEYS = (
    "Формат",
    "Alpha a0",
    "Alpha a1",
    "Alpha a2",
    "SF1",
    "SF2",
    "SF3",
    "SF4",
    "SF5",
    "Reserved",
)

_GALION_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    ("section", "Коэффициенты α"),
    "Alpha a0",
    "Alpha a1",
    "Alpha a2",
    ("section", "Коэффициенты SF"),
    "SF1",
    "SF2",
    "SF3",
    "SF4",
    "SF5",
    ("section", "Прочее"),
    "Reserved",
)

_BD3ION_KEYS = ("Формат",) + tuple(f"Коэфф. a{i}" for i in range(1, 10)) + ("Reserved",)

_BD3ION_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    ("section", "Коэффициенты ai"),
    *[f"Коэфф. a{i}" for i in range(1, 10)],
    ("section", "Прочее"),
    "Reserved",
)

_VERSION_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Устройство"),
    "Формат",
    "Продукт",
    "Тип продукта",
    "Версия ПО",
    "PN/SN",
    ("section", "Прочее"),
    "Авторизация",
    "Board ID",
    "Дата компиляции",
)

_BASEINFO_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Базовая станция"),
    "Статус",
    "Station ID",
    "Формат",
    ("section", "Координаты ECEF"),
    "Координаты ECEF",
)

_GPSUTC_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    "UTC нед.",
    "TOT",
    ("section", "Параметры UTC"),
    "A0 (смещ.)",
    "A1 (скорость)",
    "wn_lsf",
    "dn",
    "delta_ls",
    "delta_lsf",
    "delta_utc",
    ("section", "Прочее"),
    "Reserved",
)

_BD3UTC_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    "UTC нед.",
    "TOT",
    ("section", "Параметры UTC"),
    "A0 (смещ.)",
    "A1 (дрейф)",
    "A2 (дрейф)",
    "wn_lsf",
    "dn",
    "delta_ls",
    "delta_lsf",
    ("section", "Прочее"),
    "Reserved",
    "Reserved2",
)

_BESTNAV_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    ("section", "Решение"),
    "Статус решения",
    "Тип позиции",
    "Позиция",
    "Datum",
    "Спутники",
    ("section", "Маски и расширенный статус"),
    "Маска GAL/BDS3",
    "Маска GPS/GLO/BDS2",
    "RTK verify",
    "Iono type",
    ("section", "Скорость"),
    "Статус скорости",
    "Тип скорости",
    "Скорость (hor/trk°/vert)",
)

_BESTNAVXYZ_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    ("section", "Позиция ECEF"),
    "P sol status",
    "Тип позиции",
    "Позиция ECEF",
    "Sigma X,Y,Z",
    ("section", "Скорость ECEF"),
    "V sol status",
    "Тип скорости",
    "Скорость ECEF",
    ("section", "Метаданные"),
    "Station ID",
    "Спутники (отслеж/реш)",
    "L1 / multi",
    ("section", "Маски и расшир. статус"),
    "Маска GAL/BDS3",
    "Маска GPS/GLO/BDS2",
    "RTK verify",
    "Iono type",
)

_ADRNAV_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Позиция"),
    "Статус позиции",
    "Тип позиции",
    "Позиция",
    "Sigma (lat,lon,hgt)",
    ("section", "Скорость"),
    "Скорость",
    ("section", "Метаданные"),
    "Station ID",
    "Спутников в решении",
)

_PPPNAV_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Решение PPP"),
    "Статус PPP",
    "Тип позиции",
    "Позиция",
    "Sigma (lat,lon,hgt)",
    ("section", "Метаданные"),
    "Спутников в решении",
)

_SPPNAV_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Позиция"),
    "Статус SPP",
    "Тип позиции",
    "Позиция",
    "Sigma (lat,lon,hgt)",
    ("section", "Скорость"),
    "Скорость",
)

_DOP_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "DOP"),
    "GDOP",
    "PDOP",
    "HDOP / VDOP",
    ("section", "Спутники"),
    "Число спутников",
)

_AGRIC_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    "Статус позиции",
    "Статус heading",
    "Дата и время (GNSS)",
    "Система",
    "Длина полезной нагрузки",
    "Спутники (GPS/BDS/GLO/GAL)",
    ("section", "Позиция и база"),
    "Позиция Rover",
    "σ позиции (lat, lon, hgt)",
    "Baseline",
    "σ baseline (N,E,U)",
    "База (lat, lon, alt)",
    "Вторичная антенна (lat, lon, alt)",
    ("section", "Ориентация и скорость"),
    "Attitude (H/P/R)",
    "Heading (поле сообщения)",
    "Скорость",
    ("section", "ECEF и время"),
    "ECEF",
    "σ ECEF",
    "GPS week second",
    "Diffage",
    "Speed heading",
    "Undulation",
    "Speed type",
    ("section", "Контрольная сумма"),
    "CRC (ASCII)",
    "CRC (binary)",
)

_PVTSLN_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    ("section", "Позиция"),
    "Тип позиции (best)",
    "Лучшая позиция",
    "PSR позиция",
    "Undulation",
    ("section", "Скорость и heading"),
    "PSR скорость",
    "Heading",
    ("section", "DOP и спутники"),
    "DOP",
    "Cutoff",
    "PRN список",
)

_HWSTATUS_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    "Температура",
    ("section", "Питание"),
    "DC09",
    "DC10",
    "DC18",
    ("section", "Часы и флаги"),
    "Clock Valid",
    "Clock Drift",
    "Hardware Flag",
    "PLL Lock",
    "Осциллятор",
)

_AGC_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Общее"),
    "Формат",
    ("section", "Антенны"),
    "Главная антенна",
    "Ведомая антенна",
)

_MODE_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Режим"),
    "Режим",
    "Подтип",
    "Heading Mode",
    "Строка режима",
)

_CONFIG_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Конфигурация"),
    "Формат",
)

_OBS_META_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Кадр"),
    "Формат",
    "Число наблюдений",
    ("section", "Последний кадр"),
    "Последняя строка (кадр)",
)

_OBSVM_CMP_META_LAYOUT: Tuple[LayoutItem, ...] = (
    ("section", "Кадр"),
    "Формат",
    "Число наблюдений",
    "Примечание",
    ("section", "Последний кадр"),
    "Последняя строка (кадр)",
)

# Явный порядок ключей (как в словаре после format_data_for_table)
_DATA_OUTPUT_FORM_FIELD_ORDER: Dict[str, Tuple[str, ...]] = {
    "query_version": (
        "Формат",
        "Продукт",
        "Тип продукта",
        "Версия ПО",
        "PN/SN",
        "Авторизация",
        "Board ID",
        "Дата компиляции",
    ),
    "query_baseinfo": ("Статус", "Координаты ECEF", "Station ID", "Формат"),
    "query_gpsion": _GION_GPS_BDS_KEYS,
    "query_bdsion": _GION_GPS_BDS_KEYS,
    "query_galion": _GALION_KEYS,
    "query_bd3ion": _BD3ION_KEYS,
    "query_gpsutc": (
        "Формат",
        "UTC нед.",
        "TOT",
        "A0 (смещ.)",
        "A1 (скорость)",
        "wn_lsf",
        "dn",
        "delta_ls",
        "delta_lsf",
        "delta_utc",
        "Reserved",
    ),
    "query_bd3utc": (
        "Формат",
        "UTC нед.",
        "TOT",
        "A0 (смещ.)",
        "A1 (дрейф)",
        "A2 (дрейф)",
        "wn_lsf",
        "dn",
        "delta_ls",
        "delta_lsf",
        "Reserved",
        "Reserved2",
    ),
    "query_bestnav": (
        "Формат",
        "Статус решения",
        "Тип позиции",
        "Позиция",
        "Datum",
        "Спутники",
        "Маска GAL/BDS3",
        "Маска GPS/GLO/BDS2",
        "RTK verify",
        "Iono type",
        "Статус скорости",
        "Тип скорости",
        "Скорость (hor/trk°/vert)",
    ),
    "query_bestnavxyz": (
        "Формат",
        "P sol status",
        "Тип позиции",
        "Позиция ECEF",
        "Sigma X,Y,Z",
        "V sol status",
        "Тип скорости",
        "Скорость ECEF",
        "Station ID",
        "Спутники (отслеж/реш)",
        "L1 / multi",
        "Маска GAL/BDS3",
        "Маска GPS/GLO/BDS2",
        "RTK verify",
        "Iono type",
    ),
    "query_adrnav": (
        "Статус позиции",
        "Тип позиции",
        "Позиция",
        "Sigma (lat,lon,hgt)",
        "Скорость",
        "Station ID",
        "Спутников в решении",
    ),
    "query_adrnavh": (
        "Статус позиции",
        "Тип позиции",
        "Позиция",
        "Sigma (lat,lon,hgt)",
        "Скорость",
        "Station ID",
        "Спутников в решении",
    ),
    "query_pppnav": (
        "Статус PPP",
        "Тип позиции",
        "Позиция",
        "Sigma (lat,lon,hgt)",
        "Спутников в решении",
    ),
    "query_sppnav": (
        "Статус SPP",
        "Тип позиции",
        "Позиция",
        "Sigma (lat,lon,hgt)",
        "Скорость",
    ),
    "query_sppnavh": (
        "Статус SPP",
        "Тип позиции",
        "Позиция",
        "Sigma (lat,lon,hgt)",
        "Скорость",
    ),
    "query_stadop": ("GDOP", "PDOP", "HDOP / VDOP", "Число спутников"),
    "query_adrdop": ("GDOP", "PDOP", "HDOP / VDOP", "Число спутников"),
    "query_adrdoph": ("GDOP", "PDOP", "HDOP / VDOP", "Число спутников"),
    "query_agric": (
        "Формат",
        "Статус позиции",
        "Статус heading",
        "Дата и время (GNSS)",
        "Система",
        "Длина полезной нагрузки",
        "Спутники (GPS/BDS/GLO/GAL)",
        "Позиция Rover",
        "σ позиции (lat, lon, hgt)",
        "Baseline",
        "σ baseline (N,E,U)",
        "База (lat, lon, alt)",
        "Вторичная антенна (lat, lon, alt)",
        "Attitude (H/P/R)",
        "Heading (поле сообщения)",
        "Скорость",
        "ECEF",
        "σ ECEF",
        "GPS week second",
        "Diffage",
        "Speed heading",
        "Undulation",
        "Speed type",
        "CRC (ASCII)",
        "CRC (binary)",
    ),
    "query_pvtsln": (
        "Формат",
        "Тип позиции (best)",
        "Лучшая позиция",
        "PSR позиция",
        "Undulation",
        "PSR скорость",
        "Heading",
        "DOP",
        "Cutoff",
        "PRN список",
    ),
    "query_hwstatus": (
        "Формат",
        "Температура",
        "DC09",
        "DC10",
        "DC18",
        "Clock Valid",
        "Clock Drift",
        "Hardware Flag",
        "PLL Lock",
        "Осциллятор",
    ),
    "query_agc": ("Формат", "Главная антенна", "Ведомая антенна"),
    "query_obsvm": ("Формат", "Число наблюдений", "Последняя строка (кадр)"),
    "query_obsvh": ("Формат", "Число наблюдений", "Последняя строка (кадр)"),
    "query_obsvbase": ("Формат", "Число наблюдений", "Последняя строка (кадр)"),
    "query_obsvmcmp": ("Формат", "Число наблюдений", "Примечание", "Последняя строка (кадр)"),
    "query_mode": ("Режим", "Подтип", "Heading Mode", "Строка режима"),
    "query_mask": (
        "Строк MASK",
        "Углы возвышения (°)",
        "Маски по системам",
        "Маски PRN",
    ),
    "query_config": ("Формат",),
}

_DATA_OUTPUT_FORM_LAYOUT: Dict[str, Tuple[LayoutItem, ...]] = {
    "query_version": _VERSION_LAYOUT,
    "query_baseinfo": _BASEINFO_LAYOUT,
    "query_gpsion": _GION_GPS_BDS_LAYOUT,
    "query_bdsion": _GION_GPS_BDS_LAYOUT,
    "query_galion": _GALION_LAYOUT,
    "query_bd3ion": _BD3ION_LAYOUT,
    "query_gpsutc": _GPSUTC_LAYOUT,
    "query_bd3utc": _BD3UTC_LAYOUT,
    "query_bestnav": _BESTNAV_LAYOUT,
    "query_bestnavxyz": _BESTNAVXYZ_LAYOUT,
    "query_adrnav": _ADRNAV_LAYOUT,
    "query_adrnavh": _ADRNAV_LAYOUT,
    "query_pppnav": _PPPNAV_LAYOUT,
    "query_sppnav": _SPPNAV_LAYOUT,
    "query_sppnavh": _SPPNAV_LAYOUT,
    "query_stadop": _DOP_LAYOUT,
    "query_adrdop": _DOP_LAYOUT,
    "query_adrdoph": _DOP_LAYOUT,
    "query_agric": _AGRIC_LAYOUT,
    "query_pvtsln": _PVTSLN_LAYOUT,
    "query_hwstatus": _HWSTATUS_LAYOUT,
    "query_agc": _AGC_LAYOUT,
    "query_obsvm": _OBS_META_LAYOUT,
    "query_obsvh": _OBS_META_LAYOUT,
    "query_obsvbase": _OBS_META_LAYOUT,
    "query_obsvmcmp": _OBSVM_CMP_META_LAYOUT,
    "query_mode": _MODE_LAYOUT,
    "query_config": _CONFIG_LAYOUT,
}

_LOG_LINE_RE = re.compile(r"^Лог (\d+)$")
_MASK_LINE_RE = re.compile(r"^MASK (\d+)$")
_MASK_BLOCK_FIELD_RE = re.compile(r"^(.+)__(\d+)$")
_OBS_BLOCK_FIELD_RE = re.compile(r"^(.+)__OBS(\d+)$")


def _sort_log_keys(keys: List[str]) -> List[str]:
    def key_fn(k: str) -> Tuple[int, str]:
        m = _LOG_LINE_RE.match(k)
        if m:
            return (0, int(m.group(1)))
        return (1, k)

    return sorted(keys, key=key_fn)


def _sort_mask_keys(keys: List[str]) -> List[str]:
    def key_fn(k: str) -> Tuple[int, str]:
        m = _MASK_LINE_RE.match(k)
        if m:
            return (0, int(m.group(1)))
        return (1, k)

    return sorted(keys, key=key_fn)


def _build_uniloglist_rows(fields: Dict[str, Any]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    listed: set[str] = set()
    if "Количество" in fields:
        out.append(("section", "Сводка"))
        out.append(("field", "Количество"))
        listed.add("Количество")
    log_keys = _sort_log_keys([k for k in fields if k.startswith("Лог ")])
    if log_keys:
        out.append(("section", "Активные логи"))
        for k in log_keys:
            out.append(("field", k))
            listed.add(k)
    for k in sorted(fields.keys()):
        if k not in listed:
            out.append(("field", k))
    return out


def _build_mask_rows(fields: Dict[str, Any]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    listed: set[str] = set()
    if "Масок" in fields:
        out.append(("section", "Сводка"))
        out.append(("field", "Масок"))
        listed.add("Масок")

    by_mask: Dict[int, List[str]] = {}
    for k in fields.keys():
        m = _MASK_BLOCK_FIELD_RE.match(k)
        if not m:
            continue
        idx = int(m.group(2))
        by_mask.setdefault(idx, []).append(k)
    for idx in sorted(by_mask.keys()):
        out.append(("section", f"Маска {idx}"))
        # Базовый порядок полей внутри блока.
        order = ("Тип", "Угол возвышения", "Система", "PRN", "Значение")
        keys = by_mask[idx]
        ordered_keys: List[str] = []
        for p in order:
            kk = f"{p}__{idx}"
            if kk in keys:
                ordered_keys.append(kk)
        for k in sorted(keys):
            if k not in ordered_keys:
                ordered_keys.append(k)
        for k in ordered_keys:
            out.append(("field", k))
            listed.add(k)
    for k in sorted(fields.keys()):
        if k not in listed:
            out.append(("field", k))
    return out


def _build_obsv_rows(fields: Dict[str, Any]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    listed: set[str] = set()
    if "Число наблюдений" in fields:
        out.append(("section", "Сводка"))
        out.append(("field", "Число наблюдений"))
        listed.add("Число наблюдений")
    if "Формат" in fields:
        out.append(("field", "Формат"))
        listed.add("Формат")
    by_obs: Dict[int, List[str]] = {}
    for k in fields.keys():
        m = _OBS_BLOCK_FIELD_RE.match(k)
        if not m:
            continue
        idx = int(m.group(2))
        by_obs.setdefault(idx, []).append(k)
    for idx in sorted(by_obs.keys()):
        out.append(("section", f"Наблюдение {idx}"))
        keys = by_obs[idx]
        # Стабильный и читаемый порядок: сначала ГНСС/PRN/Сигнал, затем остальное.
        order = ("ГНСС", "PRN", "Сигнал")
        ordered_keys: List[str] = []
        for p in order:
            kk = f"{p}__OBS{idx}"
            if kk in keys:
                ordered_keys.append(kk)
        for k in sorted(keys):
            if k not in ordered_keys:
                ordered_keys.append(k)
        for k in ordered_keys:
            out.append(("field", k))
            listed.add(k)
    for k in sorted(fields.keys()):
        if k not in listed:
            out.append(("field", k))
    return out


def ordered_form_keys(command: Optional[str], fields: Dict[str, Any]) -> List[str]:
    """
    Ключи полей формы в порядке отображения.
    Для команд Data Output с известным шаблоном — логический порядок;
    остальные ключи (в т.ч. новые поля парсера) добавляются в конце по алфавиту.
    """
    if not fields:
        return []
    if not command or command not in FORM_VIEW_QUERY_COMMANDS:
        return sorted(fields.keys())

    if command == "query_uniloglist":
        keys: List[str] = []
        if "Количество" in fields:
            keys.append("Количество")
        log_keys = _sort_log_keys([k for k in fields if k.startswith("Лог ")])
        keys.extend(k for k in log_keys if k in fields)
        seen = set(keys)
        for k in sorted(fields.keys()):
            if k not in seen:
                keys.append(k)
        return keys

    if command == "query_mask":
        keys_m: List[str] = []
        if "Масок" in fields:
            keys_m.append("Масок")
        for idx in sorted({int(m.group(2)) for k in fields.keys() for m in [re.match(_MASK_BLOCK_FIELD_RE, k)] if m}):
            for p in ("Тип", "Угол возвышения", "Система", "PRN", "Значение"):
                kk = f"{p}__{idx}"
                if kk in fields:
                    keys_m.append(kk)
        seen_m = set(keys_m)
        for k in sorted(fields.keys()):
            if k not in seen_m:
                keys_m.append(k)
        return keys_m

    preferred = _DATA_OUTPUT_FORM_FIELD_ORDER.get(command)
    if not preferred:
        return sorted(fields.keys())

    out: List[str] = []
    seen: set[str] = set()
    for k in preferred:
        if k in fields:
            out.append(k)
            seen.add(k)
    for k in sorted(fields.keys()):
        if k not in seen:
            out.append(k)
    return out


def build_form_rows(command: Optional[str], fields: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Строки формы: ('section', заголовок) или ('field', ключ поля в fields).
    Для команд с раскладкой _DATA_OUTPUT_FORM_LAYOUT — вставляются секции;
    иначе — плоский список полей в порядке ordered_form_keys.
    """
    if not fields:
        return []
    if command == "query_uniloglist":
        return _build_uniloglist_rows(fields)
    if command == "query_mask":
        return _build_mask_rows(fields)
    if command in {"query_obsvm", "query_obsvh", "query_obsvbase", "query_obsvmcmp"}:
        if any(_OBS_BLOCK_FIELD_RE.match(k) for k in fields.keys()):
            return _build_obsv_rows(fields)

    layout = _DATA_OUTPUT_FORM_LAYOUT.get(command)
    if not layout:
        return [("field", k) for k in ordered_form_keys(command, fields)]

    out: List[Tuple[str, str]] = []
    listed: set[str] = set()
    pending_section: Optional[str] = None
    for item in layout:
        if isinstance(item, tuple) and len(item) == 2 and item[0] == "section":
            pending_section = item[1]
            continue
        if not isinstance(item, str):
            continue
        key = item
        if key not in fields:
            continue
        if pending_section is not None:
            out.append(("section", pending_section))
            pending_section = None
        out.append(("field", key))
        listed.add(key)
    for k in ordered_form_keys(command, fields):
        if k not in listed:
            out.append(("field", k))
    return out
