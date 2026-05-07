import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog, font as tkfont
import serial.tools.list_ports
from typing import Dict, Any, Optional, List, Tuple
import csv
import json
import inspect
import threading
import queue
import time
from datetime import datetime
from pathlib import Path
from um982_uart import UM982UART
from um982.receiver_profile import (
    apply_profile_diff,
    capture_profile,
    diff_profiles,
    load_profile_json,
    profile_document_to_json,
    default_profiles_dir,
)
from um982.core import _is_tcp_port_spec
from um982.data_output_views import build_form_rows
from um982_commands import get_command_names, get_command_definition
from um982.utils import format_log_period_display, parse_log_period_str

MAX_DATA_ROWS = 1000


def _coerce_log_rate_param(value: Any, *, default: float = 1.0) -> float:
    """Период для LOG/worker: из параметров запроса (int/float/str)."""
    try:
        return parse_log_period_str(value)
    except ValueError:
        return default


# При запросе MASK: столько отдельных полей «MASK N» в форме; больше — одно многострочное «Правила MASK (все записи)».
QUERY_MASK_MAX_INLINE_RULE_ROWS = 120

# Колонка момента приёма пакета в таблице наблюдений (OBSVM/OBSVH/…).
OBS_TABLE_TIME_COLUMN = "Время (приём)"

# Высота области «Параметры команды» подстраивается под число полей (с ограничением и прокруткой).
MAX_PARAMS_INNER_HEIGHT = 360
MIN_PARAMS_PANE_HEIGHT = 72

# Код частоты и система для подписи в combobox (MASK / UNMASK); в команду уходит только код.
_MASK_UNMASK_FREQUENCY_CHOICES: Tuple[Tuple[str, str], ...] = (
    ("L1", "GPS"),
    ("L1CA", "GPS"),
    ("L1C", "GPS"),
    ("L2", "GPS"),
    ("L2C", "GPS"),
    ("L2P", "GPS"),
    ("L5", "GPS"),
    ("B1", "BDS"),
    ("B2", "BDS"),
    ("B3", "BDS"),
    ("B1I", "BDS"),
    ("B2I", "BDS"),
    ("B3I", "BDS"),
    ("BD3B1C", "BDS"),
    ("BD3B2A", "BDS"),
    ("BD3B2B", "BDS"),
    ("R1", "GLO"),
    ("R2", "GLO"),
    ("R3", "GLO"),
    ("E1", "GAL"),
    ("E5A", "GAL"),
    ("E5B", "GAL"),
    ("E6C", "GAL"),
    ("Q1", "QZSS"),
    ("Q2", "QZSS"),
    ("Q5", "QZSS"),
    ("Q1CA", "QZSS"),
    ("Q1C", "QZSS"),
    ("Q2C", "QZSS"),
    ("I5", "IRNSS"),
)

# Имена для LOG на COM — совпадают с ASCII-командами из `um982/data_output` (A/B как в query_*: A=ASCII-вывод, B=бинарный).
# Это не «пункты меню GUI», а строки, которые приёмник ожидает в команде LOG (как в benchmark_data_output_queries / парсерах).
_LOG_MESSAGE_TYPES_RAW: Tuple[str, ...] = (
    "VERSIONA",
    "VERSIONB",
    "BASEINFOA",
    "BASEINFOB",
    "BESTNAVA",
    "BESTNAVB",
    "BESTNAVXYZA",
    "BESTNAVXYZB",
    "ADRNAVA",
    "ADRNAVB",
    "ADRNAVHA",
    "ADRNAVHB",
    "PPPNAVA",
    "PPPNAVB",
    "SPPNAVA",
    "SPPNAVB",
    "SPPNAVHA",
    "SPPNAVHB",
    "STADOPA",
    "STADOPB",
    "ADRDOPA",
    "ADRDOPB",
    "ADRDOPHA",
    "ADRDOPHB",
    # Совместимость со старой опечаткой.
    "ARDDOPHA",
    "ARDDOPHB",
    "PVTSLNA",
    "PVTSLNB",
    "OBSVMA",
    "OBSVMB",
    "OBSVHA",
    "OBSVHB",
    "OBSVMCMPA",
    "OBSVMCMPB",
    "OBSVBASEA",
    "OBSVBASEB",
    "GPSIONA",
    "GPSIONB",
    "GALIONA",
    "GALIONB",
    "BDSIONA",
    "BDSIONB",
    "BD3IONA",
    "BD3IONB",
    "GPSUTCA",
    "GPSUTCB",
    "BD3UTCA",
    "BD3UTCB",
    "AGRICA",
    "AGRICB",
    "HWSTATUSA",
    "HWSTATUSB",
    "AGCA",
    "AGCB",
    "GPGGA",
    "GNGGA",
    "GPRMC",
    "GNRMC",
    "GPGSV",
    "GNGSA",
    "GPGSA",
)
_LOG_MESSAGE_TYPE_CHOICES: Tuple[str, ...] = tuple(
    ["BESTNAVA"] + sorted({x for x in _LOG_MESSAGE_TYPES_RAW if x != "BESTNAVA"})
)
# Ключ в `_log_rate_memory` для режима «все типы из списка» (одна матрица периодов на COM1–COM3).
_LOG_BULK_MEMORY_KEY = "__ALL_TYPES__"
_LOG_MODE_DISPLAY_BY_ID: Dict[str, str] = {
    "single": "Одно сообщение",
}
_LOG_MODE_ID_BY_DISPLAY: Dict[str, str] = {v: k for k, v in _LOG_MODE_DISPLAY_BY_ID.items()}


def _build_log_message_to_stream_key_map() -> Dict[str, Tuple[str, bool]]:
    """
    Имя сообщения из LOG (…A / …B) → (query_*, binary).

    По соглашению Unicore: суффикс A — ASCII-вывод, B — бинарный; это разные команды на проводе,
    подсказки для GUI не должны смешиваться.
    """
    m: Dict[str, Tuple[str, bool]] = {}
    for qid, (name_a, name_b) in (
            ("query_baseinfo", ("BASEINFOA", "BASEINFOB")),
            ("query_bestnav", ("BESTNAVA", "BESTNAVB")),
            ("query_bestnavxyz", ("BESTNAVXYZA", "BESTNAVXYZB")),
            ("query_adrnav", ("ADRNAVA", "ADRNAVB")),
            ("query_adrnavh", ("ADRNAVHA", "ADRNAVHB")),
            ("query_pppnav", ("PPPNAVA", "PPPNAVB")),
            ("query_sppnav", ("SPPNAVA", "SPPNAVB")),
            ("query_sppnavh", ("SPPNAVHA", "SPPNAVHB")),
            ("query_stadop", ("STADOPA", "STADOPB")),
            ("query_adrdop", ("ADRDOPA", "ADRDOPB")),
            ("query_adrdoph", ("ADRDOPHA", "ADRDOPHB")),
            ("query_pvtsln", ("PVTSLNA", "PVTSLNB")),
            ("query_obsvm", ("OBSVMA", "OBSVMB")),
            ("query_obsvh", ("OBSVHA", "OBSVHB")),
            ("query_obsvmcmp", ("OBSVMCMPA", "OBSVMCMPB")),
            ("query_obsvbase", ("OBSVBASEA", "OBSVBASEB")),
            ("query_gpsion", ("GPSIONA", "GPSIONB")),
            ("query_galion", ("GALIONA", "GALIONB")),
            ("query_bdsion", ("BDSIONA", "BDSIONB")),
            ("query_bd3ion", ("BD3IONA", "BD3IONB")),
            ("query_gpsutc", ("GPSUTCA", "GPSUTCB")),
            ("query_bd3utc", ("BD3UTCA", "BD3UTCB")),
            ("query_agric", ("AGRICA", "AGRICB")),
            ("query_hwstatus", ("HWSTATUSA", "HWSTATUSB")),
            ("query_agc", ("AGCA", "AGCB")),
            ("query_version", ("VERSIONA", "VERSIONB")),
    ):
        m[name_a] = (qid, False)
        m[name_b] = (qid, True)
    return m


_LOG_MSG_STREAM_KEY: Dict[str, Tuple[str, bool]] = _build_log_message_to_stream_key_map()
# query_* с опциональным COM в UART (подставляем порт из LOG, если был в команде).
_STREAM_QUERY_WITH_COM: frozenset = frozenset(
    {"query_obsvm", "query_obsvh", "query_obsvmcmp", "query_agric", "query_obsvbase"}
)


def _mask_unmask_frequency_dropdown_values() -> List[str]:
    return [""] + [f"{code} - {sys}" for code, sys in _MASK_UNMASK_FREQUENCY_CHOICES]


def _frequency_token_from_dropdown(value: object) -> str:
    """Из значения «CODE - SYSTEM» извлекает код частоты для протокола."""
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if " - " in s:
        return s.split(" - ", 1)[0].strip()
    return s


def _describe_mask_query_entry(entry: Dict[str, Any]) -> str:
    """Краткое человекочитаемое описание одной записи MASK для таблицы «Текущее сообщение»."""
    t = (entry or {}).get("type", "")
    val = (entry or {}).get("value", "")
    raw = (entry or {}).get("raw", "")
    if t == "threshold":
        th = entry.get("threshold")
        return f"Угол места >= {th:g}°" if th is not None else f"Угол места: {val}"
    if t == "system":
        sys = entry.get("system", val)
        return f"Система: {sys}"
    if t == "prn_mask":
        return f"PRN: {entry.get('system')} #{entry.get('prn')}"
    if raw:
        return f"Запись: {raw[:120]}{'…' if len(raw) > 120 else ''}"
    return str(val) if val else "—"


def _mask_entry_command_like(entry: Dict[str, Any]) -> str:
    """Запись MASK в виде, близком к командам настройки (как отправляли бы в CONFIG MASK)."""
    if not isinstance(entry, dict):
        return str(entry)
    payload = str(entry.get("value") or "").strip()
    if payload:
        return payload
    raw = str(entry.get("raw") or "").strip()
    if raw.startswith("$CONFIG,MASK,"):
        tail = raw[len("$CONFIG,MASK,"):]
        if "*" in tail:
            tail = tail.split("*", 1)[0]
        return tail.strip() or _describe_mask_query_entry(entry)
    return _describe_mask_query_entry(entry)


def _mask_blocks(data: Dict[str, Any]) -> Dict[str, Any]:
    """Нормализованные блоки данных MASK для отображения."""
    entries = data.get("entries") or []
    elevation = sorted(set(data.get("elevation_masks") or []))
    systems = sorted(set(data.get("system_masks") or []))
    prn_masks = data.get("prn_masks") or {}
    prn_lines: List[str] = []
    if isinstance(prn_masks, dict):
        for sys_name in sorted(prn_masks.keys()):
            prns = prn_masks.get(sys_name)
            if not isinstance(prns, list) or not prns:
                continue
            prn_lines.append(f"{sys_name}: {', '.join(str(p) for p in sorted(prns))}")
    rules = [_mask_entry_command_like(e) for e in entries]
    return {
        "entries": entries,
        "elevation": elevation,
        "systems": systems,
        "prn_lines": prn_lines,
        "rules": rules,
    }


def _mask_entry_params(entry: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Параметры одной записи MASK для блочного отображения."""
    if not isinstance(entry, dict):
        return [("Значение", str(entry))]
    t = str(entry.get("type") or "").strip().lower()
    out: List[Tuple[str, str]] = []
    if t == "threshold":
        th = entry.get("threshold")
        out.append(("Тип", "Угол возвышения"))
        if th is not None:
            out.append(("Угол возвышения", f"{float(th):g}°"))
    elif t == "system":
        out.append(("Тип", "Система"))
        out.append(("Система", str(entry.get("system") or entry.get("value") or "")))
    elif t == "prn_mask":
        out.append(("Тип", "PRN"))
        out.append(("Система", str(entry.get("system") or "")))
        out.append(("PRN", str(entry.get("prn") or "")))
    else:
        out.append(("Тип", "Прочее"))
        out.append(("Значение", _mask_entry_command_like(entry)))
    return out


def _normalize_command_row(
        row: Tuple[Any, ...],
) -> Tuple[str, str, str, Optional[str]]:
    """Кортеж (id, имя, краткое описание) или с необязательным подробным текстом для панели «Сообщение»."""
    if len(row) == 3:
        return (row[0], row[1], row[2], None)
    return (row[0], row[1], row[2], row[3])


# Data Output: команды с опциональным trigger ONCHANGED (без него — периодическая выдача по rate)
DATA_OUTPUT_TRIGGER_OPTIONAL = frozenset({
    "query_baseinfo",
    "query_gpsion",
    "query_bdsion",
    "query_bd3ion",
    "query_galion",
    "query_gpsutc",
    "query_bd3utc",
})
TABLE_COMMAND_IDS = frozenset({
    "query_obsvm",
    "query_obsvh",
    "query_obsvbase",
    "query_obsvmcmp",
})

# Порядок категорий в списке и в пункте комбобокса «Все команды».
ALL_COMMAND_UI_CATEGORIES: Tuple[str, ...] = (
    "MODE",
    "CONFIG",
    "Data Output",
    "MASK",
    "System",
)
ALL_COMMANDS_COMBO_LABEL = "Все команды"

# Ключ данных в ответе query_* и в rx-пейлоаде process_rx_buffer: {ключ: данные}
QUERY_COMMAND_DATA_KEY: Dict[str, str] = {
    "query_mode": "mode",
    "query_version": "version",
    "query_config": "config",
    "query_mask": "mask",
    "query_obsvm": "obsvm",
    "query_obsvh": "obsvh",
    "query_obsvmcmp": "obsvmcmp",
    "query_obsvbase": "obsvbase",
    "query_baseinfo": "baseinfo",
    "query_gpsion": "gpsion",
    "query_bdsion": "bdsion",
    "query_bd3ion": "bd3ion",
    "query_galion": "galion",
    "query_gpsutc": "gpsutc",
    "query_bd3utc": "bd3utc",
    "query_adrnav": "adrnav",
    "query_adrnavh": "adrnavh",
    "query_pppnav": "pppnav",
    "query_sppnav": "sppnav",
    "query_sppnavh": "sppnavh",
    "query_stadop": "stadop",
    "query_adrdop": "adrdop",
    "query_adrdoph": "adrdoph",
    "query_agric": "agric",
    "query_pvtsln": "pvtsln",
    "query_uniloglist": "uniloglist",
    "query_bestnav": "bestnav",
    "query_bestnavxyz": "bestnavxyz",
    "query_hwstatus": "hwstatus",
    "query_agc": "agc",
}

SYSTEM_FREQ_NAMES = {
    0: "L1",
    1: "L2",
    2: "L5",
    3: "B1",
    4: "B2",
    5: "B3",
    6: "E1",
    7: "E5a",
    8: "E5b",
    9: "R1",
    10: "R2",
}


def _system_freq_str(system_freq: int) -> str:
    name = SYSTEM_FREQ_NAMES.get(system_freq)
    return f"{system_freq} ({name})" if name else str(system_freq)


def _visible_paned_kwargs(orient: str) -> Dict[str, Any]:
    """Классический panedwindow: заметная ручка и полоса-разделитель (не ttk-тема)."""
    return {
        "orient": orient,
        "sashwidth": 6,
        "sashrelief": tk.GROOVE,
        "sashpad": 2,
        "showhandle": True,
        "handlesize": 9,
        "handlepad": 8,
        "opaqueresize": True,
        "sashcursor": "sb_h_double_arrow" if orient == tk.HORIZONTAL else "sb_v_double_arrow",
    }


class UM982GUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("UM982 Control Panel")
        self.root.geometry("1200x800")

        self.device: Optional[UM982UART] = None
        self.current_command = None
        # (переменная, виджет ввода, meta: {"star": tk.Label | None})
        self.param_widgets: Dict[str, Tuple[Any, tk.Widget, Dict[str, Any]]] = {}
        self._result_queue: queue.Queue = queue.Queue()
        self._serial_io_lock = threading.Lock()
        self._receiver_stop = threading.Event()
        self._receiver_thread: Optional[threading.Thread] = None
        self._poll_after_id: Optional[str] = None
        self._selection_fetch_after_id: Optional[str] = None
        self._data_output_resend_after_id: Optional[str] = None
        # (parse_binary, parse_ascii) для process_rx_buffer — в Data Output без смешивания форматов
        self._receiver_parse_mode: Tuple[bool, bool] = (True, True)
        # В «Data Output» — только выбранная query_*: не гоняем тяжёлые parse_* по чужим кадрам в потоке
        self._rx_stream_accept: Optional[frozenset] = None
        self._receiver_flush_buf: bool = False
        self._category_switch_in_progress: bool = False
        # Панель System → LOG (матрица портов, UNILOGLIST, запоминание периодов по сообщению)
        self._log_message_var: Optional[tk.StringVar] = None
        self._log_rate_vars: Dict[str, tk.StringVar] = {}
        self._log_status_labels: Dict[str, ttk.Label] = {}
        self._log_cached_unilog_logs: Optional[List[Dict[str, Any]]] = None
        self._log_rate_memory: Dict[str, Dict[str, float]] = {}
        self._log_msg_trace_after: Optional[str] = None
        self._log_message_trace_installed: bool = False
        self._log_ui_mode: str = "single"
        self._log_mode_display_var: Optional[tk.StringVar] = None
        self._log_msg_combo_widget: Optional[ttk.Combobox] = None
        self._log_active_memory_key: Optional[str] = None
        # После LOG: подсказка rate/COM для (query_*, ASCII|binary); A/B на проводе — разные ключи.
        self._log_stream_output_hint: Dict[Tuple[str, bool], Dict[str, Any]] = {}
        self._log_stream_hint_rollback: Optional[Dict[Tuple[str, bool], Dict[str, Any]]] = None
        self._profile_busy: bool = False
        self._profile_action_buttons: List[ttk.Button] = []
        self._profile_resume_receiver: bool = False
        self._command_to_category: Dict[str, str] = {}

        self.create_widgets()
        self.load_commands()
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def create_widgets(self) -> None:
        style = ttk.Style()
        style.configure("Treeview", font=("Arial", 11))
        style.configure("Treeview.Heading", font=("Arial", 11, "bold"))
        style.configure("TCombobox", font=("Arial", 11))
        style.configure("TEntry", font=("Arial", 11))

        connection_frame = ttk.Frame(self.root, padding="10")
        connection_frame.pack(fill=tk.X)

        ttk.Label(connection_frame, text="Порт / TCP:").pack(side=tk.LEFT, padx=5)
        self.port_var = tk.StringVar()
        self.port_combo = ttk.Combobox(connection_frame, textvariable=self.port_var, width=28, font=("Arial", 11))
        self.port_combo.pack(side=tk.LEFT, padx=5)
        self.refresh_ports()

        ttk.Label(connection_frame, text="Скорость:").pack(side=tk.LEFT, padx=5)
        self.baudrate_var = tk.StringVar(value="460800")
        baudrate_combo = ttk.Combobox(
            connection_frame,
            textvariable=self.baudrate_var,
            values=["9600", "19200", "38400", "57600", "115200", "230400", "460800", "921600"],
            width=10,
            font=("Arial", 11),
        )
        baudrate_combo.pack(side=tk.LEFT, padx=5)

        self.connect_btn = ttk.Button(connection_frame, text="Подключиться", command=self.connect_device)
        self.connect_btn.pack(side=tk.LEFT, padx=5)

        self.disconnect_btn = ttk.Button(connection_frame, text="Отключиться", command=self.disconnect_device,
                                         state=tk.DISABLED)
        self.disconnect_btn.pack(side=tk.LEFT, padx=5)

        ttk.Button(connection_frame, text="Обновить порты", command=self.refresh_ports).pack(side=tk.LEFT, padx=5)

        self.status_label = ttk.Label(connection_frame, text="Не подключено", foreground="red")
        self.status_label.pack(side=tk.LEFT, padx=20)

        profile_row = ttk.LabelFrame(self.root, text="Профиль приёмника (JSON)", padding="6")
        profile_row.pack(fill=tk.X, padx=10, pady=(0, 2))
        pf_hint = (
            "Снять снимок с приёмника (CONFIG, MASK, UNILOGLIST), сохранить или сравнить с файлом, "
            "применить недостающие команды. Операции в фоне, порт блокируется только на время обмена."
        )
        ttk.Label(profile_row, text=pf_hint, wraplength=900, justify=tk.LEFT, font=("Arial", 9)).pack(
            anchor=tk.W, fill=tk.X, pady=(0, 4)
        )
        pf_btns = ttk.Frame(profile_row)
        pf_btns.pack(fill=tk.X)
        for label, handler in (
                ("Снять с приёмника и сохранить…", self.profile_export_json),
                ("Сравнить файл с приёмником…", self.profile_compare_json),
                ("Применить из файла…", self.profile_apply_json),
        ):
            b = ttk.Button(pf_btns, text=label, command=handler, state=tk.DISABLED)
            b.pack(side=tk.LEFT, padx=4)
            self._profile_action_buttons.append(b)

        run_frame = ttk.Frame(self.root, padding="5")
        run_frame.pack(fill=tk.X, padx=10, pady=(5, 0))
        self.run_frame = run_frame

        self.data_output_hint_label = ttk.Label(
            run_frame,
            # text=(
            #     "LOG по портам — в «System» → «LOG». После подключения порт читается постоянно; "
            #     "при выборе пункта отправляется команда подписки («Формат сообщения»: ASCII или Бинарный). "
            #     "В категории «Data Output» фоновый разбор потока совпадает с выбранным форматом "
            #     "(ASCII — только строки #…, бинарный — только кадры 0xAA 0x44 0xB5). "
            #     "В других категориях разбираются оба типа кадров."
            # ),
            wraplength=620,
            justify=tk.LEFT,
            foreground="gray",
            font=("Arial", 10),
        )

        self.run_btn = ttk.Button(run_frame, text="▶ Запустить команду", command=self.run_command, state=tk.DISABLED)
        self.run_btn.pack(side=tk.RIGHT, padx=5)

        self.data_output_format_var = tk.StringVar(value="ASCII")
        self.data_output_onchanged_var = tk.BooleanVar(value=False)
        self.data_output_options_frame = ttk.Frame(run_frame)
        self._data_output_binary_row = ttk.Frame(self.data_output_options_frame)
        self._data_output_onchanged_row = ttk.Frame(self.data_output_options_frame)
        ttk.Label(self._data_output_binary_row, text="Формат сообщения:").pack(side=tk.LEFT, padx=(0, 6))
        ttk.Radiobutton(
            self._data_output_binary_row,
            text="ASCII",
            value="ASCII",
            variable=self.data_output_format_var,
            command=self._on_data_output_option_changed,
        ).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Radiobutton(
            self._data_output_binary_row,
            text="Бинарный",
            value="Бинарный",
            variable=self.data_output_format_var,
            command=self._on_data_output_option_changed,
        ).pack(side=tk.LEFT)
        ttk.Checkbutton(
            self._data_output_onchanged_row,
            text="ONCHANGED",
            variable=self.data_output_onchanged_var,
            command=self._on_data_output_option_changed,
        ).pack(side=tk.LEFT)

        self.main_pane = tk.PanedWindow(self.root, **_visible_paned_kwargs(tk.HORIZONTAL))
        self.main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        left_frame = ttk.Frame(self.main_pane)
        self.main_pane.add(left_frame, stretch="always")

        ttk.Label(left_frame, text="Команды", font=("Arial", 14, "bold")).pack(pady=5)

        category_frame = ttk.Frame(left_frame)
        category_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(category_frame, text="Категория:").pack(side=tk.LEFT, padx=(0, 5))
        self.category_var = tk.StringVar(value="MODE")
        self.category_combo = ttk.Combobox(
            category_frame,
            textvariable=self.category_var,
            values=(*ALL_COMMAND_UI_CATEGORIES, ALL_COMMANDS_COMBO_LABEL),
            state="readonly",
            width=18,
            font=("Arial", 11),
        )
        self.category_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.category_combo.bind("<<ComboboxSelected>>", self._on_category_changed)

        filter_frame = ttk.Frame(left_frame)
        filter_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(filter_frame, text="Фильтр:").pack(side=tk.LEFT)
        self.filter_var = tk.StringVar()
        self.filter_var.trace('w', self.filter_commands)
        filter_entry = ttk.Entry(filter_frame, textvariable=self.filter_var, font=("Arial", 11))
        filter_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        list_frame = ttk.Frame(left_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.command_listbox = tk.Listbox(
            list_frame,
            yscrollcommand=scrollbar.set,
            font=("Arial", 12),
            selectbackground="#2563eb",
            selectforeground="white",
            highlightthickness=2,
            activestyle="dotbox",
            exportselection=False,
        )
        self.command_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.command_listbox.bind('<<ListboxSelect>>', self.on_command_select)

        scrollbar.config(command=self.command_listbox.yview)

        right_container = tk.PanedWindow(self.main_pane, **_visible_paned_kwargs(tk.VERTICAL))
        self.main_pane.add(right_container, stretch="always")
        self.right_vertical_pane = right_container
        self._params_pane_hidden = False
        self._data_pane_hidden = False
        self._table_pane_hidden = False

        self.params_frame_container = ttk.Frame(right_container)
        right_container.add(self.params_frame_container, stretch="always")

        self.params_header = ttk.Frame(self.params_frame_container)
        self.params_header.pack(fill=tk.X)

        self.params_toggle_btn = ttk.Button(self.params_header, text="▼ Параметры команды", command=self.toggle_params)
        self.params_toggle_btn.pack(side=tk.LEFT, padx=5, pady=5)

        params_content_frame = ttk.Frame(self.params_frame_container)
        params_content_frame.pack(fill=tk.BOTH, expand=True)

        self.params_canvas = tk.Canvas(params_content_frame)
        self.params_scrollbar = ttk.Scrollbar(params_content_frame, orient="vertical", command=self.params_canvas.yview)
        self.params_scrollable_frame = ttk.Frame(self.params_canvas)

        self.params_scrollable_frame.bind(
            "<Configure>",
            lambda e: self.params_canvas.configure(scrollregion=self.params_canvas.bbox("all"))
        )

        self._params_canvas_window = self.params_canvas.create_window(
            (0, 0), window=self.params_scrollable_frame, anchor="nw"
        )

        def _params_canvas_configure(event: tk.Event) -> None:
            if event.widget == self.params_canvas and event.width > 1:
                self.params_canvas.itemconfigure(self._params_canvas_window, width=event.width)

        self.params_canvas.bind("<Configure>", _params_canvas_configure)
        self.params_canvas.configure(yscrollcommand=self.params_scrollbar.set)

        self.params_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.params_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        def _on_mousewheel(event: tk.Event) -> None:
            self.params_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        self.params_canvas.bind_all("<MouseWheel>", _on_mousewheel)

        self.params_expanded = True
        self.params_content_frame = params_content_frame

        self.data_frame_container = ttk.Frame(right_container)
        right_container.add(self.data_frame_container, stretch="always")

        self.data_header = ttk.Frame(self.data_frame_container)
        self.data_header.pack(fill=tk.X)

        self.data_toggle_btn = ttk.Button(
            self.data_header,
            text="▼ Текущее сообщение",
            command=self.toggle_data,
        )
        self.data_toggle_btn.pack(side=tk.LEFT, padx=5, pady=5)

        ttk.Button(self.data_header, text="Очистить панель", command=self.clear_data_table).pack(side=tk.LEFT, padx=5)

        self.data_content_frame = ttk.Frame(self.data_frame_container)
        self.data_content_frame.pack(fill=tk.BOTH, expand=True)

        self._message_title_var = tk.StringVar(value="")
        self.command_help_frame = ttk.Frame(self.data_content_frame)
        self._command_help_wraplength = 680
        self._message_title_row = ttk.Frame(self.data_content_frame)
        self._message_title_row.pack(fill=tk.X, padx=4, pady=(0, 4))
        ttk.Label(self._message_title_row, text="Текущее сообщение:", font=("Arial", 11, "bold")).pack(side=tk.LEFT)
        ttk.Label(self._message_title_row, textvariable=self._message_title_var, font=("Arial", 11)).pack(side=tk.LEFT,
                                                                                                          padx=(6, 0))

        self.message_form_outer = ttk.Frame(self.data_content_frame)
        self.message_form_canvas = tk.Canvas(self.message_form_outer, highlightthickness=0)
        self.message_form_scroll = ttk.Scrollbar(self.message_form_outer, orient="vertical",
                                                 command=self.message_form_canvas.yview)
        self.message_form_inner = ttk.Frame(self.message_form_canvas)
        self.message_form_inner.bind(
            "<Configure>",
            lambda e: self.message_form_canvas.configure(scrollregion=self.message_form_canvas.bbox("all")),
        )
        self._message_form_window = self.message_form_canvas.create_window((0, 0), window=self.message_form_inner,
                                                                           anchor="nw")

        def _message_canvas_configure(event: tk.Event) -> None:
            self.message_form_canvas.itemconfigure(self._message_form_window, width=event.width)

        self.message_form_canvas.bind("<Configure>", _message_canvas_configure)
        self.message_form_canvas.configure(yscrollcommand=self.message_form_scroll.set)
        self.message_form_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.message_form_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        def _on_message_form_wheel(event: tk.Event) -> None:
            self.message_form_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        self.message_form_canvas.bind("<Enter>", lambda e: self.message_form_canvas.focus_set())
        self.message_form_canvas.bind("<MouseWheel>", _on_message_form_wheel)

        self.table_frame_container = ttk.Frame(right_container)
        right_container.add(self.table_frame_container, stretch="always")

        self.table_header = ttk.Frame(self.table_frame_container)
        self.table_header.pack(fill=tk.X)

        self.table_toggle_btn = ttk.Button(
            self.table_header,
            text="▶ Таблица",
            command=self.toggle_table,
        )
        self.table_toggle_btn.pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(self.table_header, text="Очистить таблицу", command=self.clear_data_table).pack(side=tk.LEFT, padx=5)
        self.export_table_btn = ttk.Button(
            self.table_header,
            text="Экспорт таблицы...",
            command=self.export_data_table,
            state=tk.DISABLED,
        )
        self.export_table_btn.pack(side=tk.LEFT, padx=5)

        self.table_content_frame = ttk.Frame(self.table_frame_container)
        self.table_content_frame.pack(fill=tk.BOTH, expand=True)

        self.data_tree_frame = ttk.Frame(self.table_content_frame)

        self.data_tree = ttk.Treeview(self.data_tree_frame, columns=(), show="headings", height=10)
        self.data_tree.heading("#0", text="#")
        self.data_tree.column("#0", width=40, anchor=tk.CENTER, stretch=tk.NO)

        self.data_scrollbar_x = ttk.Scrollbar(self.data_tree_frame, orient="horizontal", command=self.data_tree.xview)
        self.data_scrollbar_y = ttk.Scrollbar(self.data_tree_frame, orient="vertical", command=self.data_tree.yview)
        self.data_tree.configure(xscrollcommand=self.data_scrollbar_x.set, yscrollcommand=self.data_scrollbar_y.set)

        tree_center_frame = ttk.Frame(self.data_tree_frame)
        tree_center_frame.pack(expand=True, fill=tk.BOTH)

        tree_inner_frame = ttk.Frame(tree_center_frame)
        tree_inner_frame.pack(expand=True, fill=tk.BOTH, pady=4)

        tree_horizontal = ttk.Frame(tree_inner_frame)
        tree_horizontal.pack(fill=tk.BOTH, expand=True)

        self.data_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.data_scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)

        self.data_scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X)

        self.data_tree.tag_configure("obs_sep", background="#e8e8e8", foreground="#555555")

        self.data_columns = []
        self.data_rows = []
        self.data_expanded = True
        self.table_expanded = False
        self.table_content_frame.pack_forget()
        self._data_empty_default_text = (
            "Поля текущего сообщения появятся при приёме потока или после одиночного запроса "
            "(категория «Data Output», выберите тип). Таблица вынесена в отдельный раздел ниже."
        )
        # Пока False: в «Data Output» не показываем панель описания команды и текст пустой формы (строки выше и в
        # commands_by_category не удаляются — снова включить вывод: установить True).
        self._show_data_output_help_text: bool = False
        self.data_empty_label = ttk.Label(
            self.data_content_frame,
            text=self._data_empty_default_text,
            wraplength=720,
            justify=tk.LEFT,
            foreground="gray",
            font=("Arial", 11),
        )
        self.data_empty_label.pack(pady=20)
        self.table_empty_label = ttk.Label(
            self.table_content_frame,
            text="Таблица наблюдений появится здесь для OBSVM / OBSVH / OBSVBASE / OBSVMCMP.",
            wraplength=720,
            justify=tk.LEFT,
            foreground="gray",
            font=("Arial", 11),
        )
        self.table_empty_label.pack(pady=20)
        self.log_frame_container = ttk.Frame(right_container)
        right_container.add(self.log_frame_container, stretch="always")

        # Заголовок лога с кнопкой сворачивания
        self.log_header = ttk.Frame(self.log_frame_container)
        self.log_header.pack(fill=tk.X)

        self.log_toggle_btn = ttk.Button(self.log_header, text="▼ Лог", command=self.toggle_log)
        self.log_toggle_btn.pack(side=tk.LEFT, padx=5, pady=5)

        ttk.Button(self.log_header, text="Очистить", command=self.clear_results).pack(side=tk.LEFT, padx=5)
        ttk.Button(self.log_header, text="Экспорт...", command=self.export_results).pack(side=tk.LEFT, padx=5)

        # Контейнер для лога
        self.log_content_frame = ttk.Frame(self.log_frame_container)
        self.log_content_frame.pack(fill=tk.BOTH, expand=True)

        self.results_text = scrolledtext.ScrolledText(self.log_content_frame, height=10, font=("Courier", 11))
        self.results_text.pack(fill=tk.BOTH, expand=True)

        # Лог по умолчанию развернут
        self.log_expanded = True

        self.root.after_idle(self._init_layout_idle)

    def _init_layout_idle(self) -> None:
        self._init_main_pane_sash()
        self._redistribute_right_vertical_panes()

    def _init_main_pane_sash(self) -> None:
        """Начальная позиция вертикального разделителя (~как weight 1:2: левая колонка уже)."""
        try:
            self.root.update_idletasks()
            w = self.main_pane.winfo_width()
            if w > 1:
                self.main_pane.sash_place(0, max(180, w // 3), 0)
        except tk.TclError:
            pass

    def _sync_params_pane_height(self) -> None:
        """Совместимость: перераспределить высоты правой колонки (параметры / сообщение / лог)."""
        self._redistribute_right_vertical_panes()

    def _redistribute_right_vertical_panes(self) -> None:
        """
        После сворачивания блока по кнопке освободившееся место отдаётся развёрнутым панелям
        (положения всех горизонтальных разделителей PanedWindow).
        """
        pw = self.right_vertical_pane
        try:
            pane_paths = pw.panes()
        except tk.TclError:
            return
        n = len(pane_paths)
        if n < 1:
            return
        try:
            self.root.update_idletasks()
        except tk.TclError:
            return
        total_h = pw.winfo_height()
        if total_h <= 1:
            self.root.after(80, self._redistribute_right_vertical_panes)
            return

        ordered: List[tk.Widget] = []
        for p in pane_paths:
            w = None
            for cand in (
                    self.params_frame_container,
                    self.data_frame_container,
                    self.table_frame_container,
                    self.log_frame_container,
            ):
                if p is cand or str(p) == str(cand):
                    w = cand
                    break
            if w is None:
                return
            ordered.append(w)

        def is_expanded(w: tk.Widget) -> bool:
            if w is self.params_frame_container:
                return bool(self.params_expanded) and not getattr(
                    self, "_params_pane_hidden", False
                )
            if w is self.data_frame_container:
                return bool(self.data_expanded)
            if w is self.table_frame_container:
                return bool(self.table_expanded)
            if w is self.log_frame_container:
                return bool(self.log_expanded)
            return True

        def header_of(w: tk.Widget) -> tk.Widget:
            if w is self.params_frame_container:
                return self.params_header
            if w is self.data_frame_container:
                return self.data_header
            if w is self.table_frame_container:
                return self.table_header
            return self.log_header

        sash_gap = max(8, (n - 1) * 6)
        usable = max(1, total_h - sash_gap)

        bases: List[int] = []
        expanded_flags = [is_expanded(w) for w in ordered]
        for w in ordered:
            hh = max(1, header_of(w).winfo_reqheight())
            if not is_expanded(w):
                bases.append(max(24, hh + 4))
                continue
            if w is self.params_frame_container:
                inner = max(self.params_scrollable_frame.winfo_reqheight(), 28)
                inner = min(inner, MAX_PARAMS_INNER_HEIGHT)
                cap = max(MIN_PARAMS_PANE_HEIGHT, int(total_h * 0.72))
                bases.append(
                    max(MIN_PARAMS_PANE_HEIGHT, min(hh + inner + 8, cap))
                )
            else:
                try:
                    raw = max(w.winfo_reqheight(), hh + 48)
                except tk.TclError:
                    raw = hh + 120
                bases.append(max(56, min(raw, int(total_h * 0.48))))

        s = sum(bases)
        targets = bases[:]
        remaining = usable - s
        exp_idx = [i for i in range(n) if expanded_flags[i]]
        if remaining > 0 and exp_idx:
            q, r = divmod(remaining, len(exp_idx))
            for j, i in enumerate(exp_idx):
                targets[i] += q + (1 if j < r else 0)
        elif remaining > 0:
            targets[-1] += remaining
        elif remaining < 0 and s > 0:
            scale = usable / s
            floors = [24 if not expanded_flags[i] else 40 for i in range(n)]
            targets = [max(floors[i], int(bases[i] * scale)) for i in range(n)]
            adj = usable - sum(targets)
            if adj != 0:
                if exp_idx:
                    li = exp_idx[-1]
                    targets[li] = max(floors[li], targets[li] + adj)
                else:
                    targets[-1] = max(floors[-1], targets[-1] + adj)

        y = 0
        for i in range(n - 1):
            y += targets[i]
            try:
                pw.sash_place(i, 0, int(y))
            except tk.TclError:
                pass

    def refresh_ports(self) -> None:
        """Обновление списка доступных портов (serial + подсказка TCP)"""
        ports = [port.device for port in serial.tools.list_ports.comports()]
        self.port_combo['values'] = ports + ["localhost:5000 (TCP)", "tcp://host:port"]
        if ports and not self.port_var.get():
            self.port_var.set(ports[0])

    def connect_device(self) -> None:
        port = self.port_var.get().strip()
        if port.endswith(" (TCP)"):
            port = port.replace(" (TCP)", "").strip()
        if port == "tcp://host:port":
            messagebox.showinfo("TCP",
                                "Введите адрес в формате host:port или tcp://host:port\nНапример: localhost:5000")
            return
        if not port:
            messagebox.showerror("Ошибка", "Укажите порт (serial или TCP host:port)")
            return

        is_tcp = _is_tcp_port_spec(port)
        try:
            baudrate = int(self.baudrate_var.get()) if not is_tcp else 115200
        except ValueError:
            if not is_tcp:
                messagebox.showerror("Ошибка", "Неверная скорость")
                return
            baudrate = 115200

        try:
            self.device = UM982UART(port, baudrate=baudrate, timeout=2.0)
            if self.device.connect():
                if self.device._is_tcp:
                    self.status_label.config(text=f"Подключено: {port} (TCP)", foreground="green")
                    self.log_result(f"Подключено к {port} (TCP)\n")
                else:
                    self.status_label.config(text=f"Подключено: {port} @ {baudrate}", foreground="green")
                    self.log_result(f"Подключено к {port} на скорости {baudrate} бод\n")
                self.connect_btn.config(state=tk.DISABLED)
                self.disconnect_btn.config(state=tk.NORMAL)
                self.run_btn.config(state=tk.NORMAL)
                self._refresh_run_button_label()
                self._start_receiver()
                self._refresh_profile_buttons_state()
            else:
                messagebox.showerror("Ошибка", "Не удалось подключиться к устройству")
                self.device = None
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка подключения: {str(e)}")
            self.device = None

    def disconnect_device(self) -> None:
        self._stop_receiver()
        if self.device:
            self.device.disconnect()
            self.device = None
        self.status_label.config(text="Не подключено", foreground="red")
        self.connect_btn.config(state=tk.NORMAL)
        self.disconnect_btn.config(state=tk.DISABLED)
        self.run_btn.config(state=tk.DISABLED)
        self.log_result("Отключено от устройства\n")
        self._log_stream_output_hint.clear()
        self._log_stream_hint_rollback = None
        self._refresh_profile_buttons_state()

    def _receiver_profiles_dir(self) -> Path:
        return default_profiles_dir()

    def _refresh_profile_buttons_state(self) -> None:
        for b in self._profile_action_buttons:
            if self._profile_busy or not self.device:
                b.config(state=tk.DISABLED)
            else:
                b.config(state=tk.NORMAL)

    def _set_profile_busy(self, busy: bool) -> None:
        self._profile_busy = busy
        self._refresh_profile_buttons_state()

    def _pause_receiver_for_profile(self) -> None:
        """На время операций профиля останавливаем фоновый reader, чтобы не конкурировать за COM."""
        self._profile_resume_receiver = bool(self._receiver_thread and self._receiver_thread.is_alive())
        if self._profile_resume_receiver:
            self._stop_receiver()

    def _resume_receiver_after_profile(self) -> None:
        if self._profile_resume_receiver and self.device:
            try:
                if self.device.serial_conn and self.device.serial_conn.is_open:
                    self._start_receiver()
            except Exception:
                pass
        self._profile_resume_receiver = False

    def _profile_require_device(self) -> bool:
        if not self.device:
            messagebox.showinfo("Профиль", "Сначала подключитесь к приёмнику.")
            return False
        try:
            if not self.device.serial_conn or not self.device.serial_conn.is_open:
                messagebox.showinfo("Профиль", "Нет активного соединения с приёмником.")
                return False
        except Exception:
            return False
        return True

    def profile_export_json(self) -> None:
        if not self._profile_require_device():
            return
        path = filedialog.asksaveasfilename(
            title="Сохранить профиль приёмника",
            initialdir=str(self._receiver_profiles_dir()),
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("Все файлы", "*.*")],
        )
        if not path:
            return
        out_path = Path(path)
        if out_path.suffix.lower() != ".json":
            out_path = out_path.with_suffix(".json")
        self._pause_receiver_for_profile()
        self._set_profile_busy(True)
        dev = self.device

        def worker() -> None:
            err: Optional[str] = None
            target_path = out_path
            try:
                with self._serial_io_lock:
                    doc = capture_profile(dev)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.write_text(profile_document_to_json(doc), encoding="utf-8")
                if not target_path.exists():
                    raise RuntimeError(f"Файл не создан: {target_path}")
                if target_path.stat().st_size <= 0:
                    raise RuntimeError(f"Файл создан пустым: {target_path}")
            except Exception as e:
                err = f"{e} (путь: {target_path})"
            self._result_queue.put(("profile_export_done", err, str(target_path)))

        threading.Thread(target=worker, daemon=True).start()

    def _profile_export_done(self, err: Optional[str], path: str) -> None:
        self._set_profile_busy(False)
        self._resume_receiver_after_profile()
        if err:
            self.log_result(f"Профиль: ошибка сохранения: {err}\n")
            messagebox.showerror("Профиль", err)
        else:
            self.log_result(f"Профиль сохранён в JSON: {path}\n")
            messagebox.showinfo("Профиль", f"Профиль успешно сохранён:\n{path}")

    def profile_compare_json(self) -> None:
        if not self._profile_require_device():
            return
        path = filedialog.askopenfilename(
            title="Файл профиля для сравнения",
            initialdir=str(self._receiver_profiles_dir()),
            filetypes=[("JSON", "*.json"), ("Все файлы", "*.*")],
        )
        if not path:
            return
        self._pause_receiver_for_profile()
        self._set_profile_busy(True)
        dev = self.device

        def worker() -> None:
            err: Optional[str] = None
            diff: Optional[Dict[str, Any]] = None
            try:
                saved = load_profile_json(Path(path))
                with self._serial_io_lock:
                    live = capture_profile(dev)
                diff = diff_profiles(live, saved)
            except Exception as e:
                err = str(e)
            self._result_queue.put(("profile_compare_done", err, path, diff))

        threading.Thread(target=worker, daemon=True).start()

    def _profile_compare_done(self, err: Optional[str], path: str, diff: Optional[Dict[str, Any]]) -> None:
        self._set_profile_busy(False)
        self._resume_receiver_after_profile()
        if err:
            messagebox.showerror("Профиль", err)
            return
        win = tk.Toplevel(self.root)
        win.title(f"Сравнение: {path}")
        win.geometry("900x560")
        body = ttk.Frame(win, padding=8)
        body.pack(fill=tk.BOTH, expand=True)
        st = scrolledtext.ScrolledText(body, width=100, height=26, font=("Courier New", 10))
        st.pack(fill=tk.BOTH, expand=True)
        st.insert("1.0", json.dumps(diff, ensure_ascii=False, indent=2) if diff else "{}")
        st.config(state=tk.DISABLED)
        ttk.Button(body, text="Закрыть", command=win.destroy).pack(pady=(8, 0))
        self.log_result(f"Профиль: сравнение с «{path}» (см. окно).\n")

    def profile_apply_json(self) -> None:
        if not self._profile_require_device():
            return
        path = filedialog.askopenfilename(
            title="Файл профиля для применения",
            initialdir=str(self._receiver_profiles_dir()),
            filetypes=[("JSON", "*.json"), ("Все файлы", "*.*")],
        )
        if not path:
            return
        if not messagebox.askyesno(
                "Профиль",
                "Отправить на приёмник команды из файла, которых нет в текущем снимке?\n\n"
                "Будут применены: CONFIG (кроме FRESET/RESET), MASK, LOG.\n"
                "Убедитесь, что файл получен из доверенного источника.",
        ):
            return
        unlog_extra = messagebox.askyesno(
            "UNLOG",
            "Снять вывод LOG, который есть на приёмнике, но отсутствует в файле?\n\n"
            "«Нет» — только добавить недостающие LOG из файла.",
            default=messagebox.NO,
        )
        self._pause_receiver_for_profile()
        self._set_profile_busy(True)
        dev = self.device
        file_path = path

        def worker(ue: bool = unlog_extra, fp: str = file_path) -> None:
            err: Optional[str] = None
            apply_res: Optional[Dict[str, Any]] = None
            try:
                saved = load_profile_json(Path(fp))
                with self._serial_io_lock:
                    live = capture_profile(dev)
                    d = diff_profiles(live, saved)
                    apply_res = apply_profile_diff(
                        dev,
                        d,
                        apply_config=True,
                        apply_mask=True,
                        apply_logs=True,
                        unlog_extra=ue,
                    )
            except Exception as e:
                err = str(e)
            self._result_queue.put(("profile_apply_done", err, apply_res))

        threading.Thread(target=worker, daemon=True).start()

    def _profile_apply_done(self, err: Optional[str], apply_res: Optional[Dict[str, Any]]) -> None:
        self._set_profile_busy(False)
        self._resume_receiver_after_profile()
        if err:
            messagebox.showerror("Профиль", err)
            self.log_result(f"Профиль: ошибка применения: {err}\n")
            return
        if not apply_res:
            return
        if apply_res.get("errors"):
            self.log_result("Профиль: применение завершилось с ошибками:\n")
            for line in apply_res["errors"]:
                self.log_result(f"  {line}\n")
            messagebox.showwarning("Профиль", "Часть команд завершилась с ошибкой. См. журнал.")
        else:
            self.log_result("Профиль: команды из файла применены без сообщённых ошибок.\n")
            messagebox.showinfo("Профиль", "Готово. Проверьте журнал при необходимости.")

    def _active_command_category(self) -> str:
        """Категория для логики (панели, парсер, авто-запрос). В «Все команды» — по id выбранной команды."""
        if self.category_var.get() == ALL_COMMANDS_COMBO_LABEL:
            return self._command_to_category.get(self.current_command or "", "MODE")
        return self.category_var.get()

    def _refresh_run_button_label(self) -> None:
        if self._active_command_category() == "Data Output":
            self.run_btn.config(text="📥 Одиночный запрос данных")
        elif self.current_command == "log":
            self.run_btn.config(text="▶ Применить LOG на COM1, COM2 и COM3")
        else:
            self.run_btn.config(text="▶ Запустить команду")

    def _vertical_pane_contains(self, widget: tk.Widget) -> bool:
        try:
            wpath = str(widget)
            for p in self.right_vertical_pane.panes():
                if str(p) == wpath:
                    return True
        except tk.TclError:
            pass
        return False

    def _ensure_data_pane_in_vertical_pane(self) -> None:
        """Вернуть панель «Текущее сообщение» в вертикальный PanedWindow (перед логом), если её убрали."""
        if self._vertical_pane_contains(self.data_frame_container):
            self._data_pane_hidden = False
            return
        try:
            self.right_vertical_pane.add(
                self.data_frame_container,
                before=self.log_frame_container,
                stretch="always",
            )
        except tk.TclError:
            pass
        self._data_pane_hidden = False

    def _obsv_table_enabled_for_current_command(self) -> bool:
        cmd = self.current_command or ""
        return cmd in {"query_obsvm", "query_obsvh", "query_obsvbase", "query_obsvmcmp"}

    def _ensure_table_pane_in_vertical_pane(self) -> None:
        if self._vertical_pane_contains(self.table_frame_container):
            self._table_pane_hidden = False
            return
        try:
            self.right_vertical_pane.add(
                self.table_frame_container,
                before=self.log_frame_container,
                stretch="always",
            )
        except tk.TclError:
            pass
        self._table_pane_hidden = False

    def _sync_table_pane_visibility(self) -> None:
        if self._obsv_table_enabled_for_current_command():
            self._ensure_table_pane_in_vertical_pane()
            return
        self._clear_tree_data()
        try:
            self.data_tree_frame.pack_forget()
            self.table_empty_label.pack_forget()
        except Exception:
            pass
        if self._vertical_pane_contains(self.table_frame_container):
            try:
                self.right_vertical_pane.forget(self.table_frame_container)
            except tk.TclError:
                pass
        self._table_pane_hidden = True

    def _data_message_pane_hidden_for_current_command(self) -> bool:
        """Панель «Текущее сообщение» только для запросов режима/конфига; для установок — скрыта."""
        cat = self._active_command_category()
        cmd = self.current_command or ""
        if cat == "MODE" and cmd.startswith("set_mode_"):
            return True
        if cat in ("CONFIG", "System"):
            return True
        return False

    def _hide_data_pane_for_mode_set_commands(self) -> None:
        """MODE set / CONFIG set: убрать панель «Текущее сообщение» — смотрите лог."""
        if not self._data_message_pane_hidden_for_current_command():
            return
        if getattr(self, "_data_pane_hidden", False):
            return
        if not self._vertical_pane_contains(self.data_frame_container):
            self._data_pane_hidden = True
            return
        self.clear_data_table()
        try:
            self.right_vertical_pane.forget(self.data_frame_container)
        except tk.TclError:
            pass
        self._data_pane_hidden = True

    def _sync_data_output_panel_layout(self) -> None:
        """
        Для «Data Output» не показываем панель параметров (порты/частота — только System → LOG).
        Краткая подсказка — в строке с кнопкой одиночного запроса.
        Для установки MODE и для пунктов CONFIG кроме «Запрос конфигурации» панель «Текущее сообщение» скрыта.
        """
        is_data_output = self._active_command_category() == "Data Output"
        if is_data_output:
            self._ensure_data_pane_in_vertical_pane()
            if self.log_expanded:
                self.log_content_frame.pack_forget()
                self.log_toggle_btn.config(text="▶ Лог")
                self.log_expanded = False
        if is_data_output:
            if not self._params_pane_hidden:
                self.right_vertical_pane.forget(self.params_frame_container)
                self._params_pane_hidden = True
            self.data_output_hint_label.pack(side=tk.LEFT, padx=(0, 12), fill=tk.X, expand=True, before=self.run_btn)
            self.data_output_options_frame.pack(side=tk.LEFT, padx=(0, 8), before=self.run_btn)
            self._update_data_output_options_visibility()
        else:
            self.data_output_hint_label.pack_forget()
            self.data_output_options_frame.pack_forget()
            if self._params_pane_hidden:
                self._ensure_data_pane_in_vertical_pane()
                self.right_vertical_pane.add(
                    self.params_frame_container,
                    before=self.data_frame_container,
                    stretch="always",
                )
                self._params_pane_hidden = False
                self.root.after_idle(self._sync_params_pane_height)
        if self._data_message_pane_hidden_for_current_command():
            self._hide_data_pane_for_mode_set_commands()
        elif not is_data_output:
            self._ensure_data_pane_in_vertical_pane()
        self._sync_table_pane_visibility()
        self._sync_receiver_parse_mode()

    def _is_data_output_query_id(self, cmd: Optional[str]) -> bool:
        """Команда относится к категории Data Output (по id), независимо от выбранной вкладки категорий."""
        c = (cmd or "").strip()
        return bool(c.startswith("query_") and self._command_to_category.get(c, "") == "Data Output")

    def _sync_receiver_parse_mode(self) -> None:
        """
        Режим (parse_binary, parse_ascii) для process_rx_buffer.

        В «Data Output» фильтр совпадает с «Формат сообщения»: иначе при ASCII на приёмнике всё ещё
        могли бы приходить бинарные кадры (другой LOG / старая подписка), и панель показывала бы binary.
        Вне «Data Output» оставляем оба парсера — удобно для смешанного потока при отладке.
        """
        prev = self._receiver_parse_mode
        if self._is_data_output_query_id(self.current_command):
            use_binary = self.data_output_format_var.get() == "Бинарный"
            self._receiver_parse_mode = (use_binary, not use_binary)
        else:
            self._receiver_parse_mode = (True, True)
        if self._receiver_parse_mode != prev:
            self._receiver_flush_buf = True
        if self._is_data_output_query_id(self.current_command):
            c = self.current_command
            self._rx_stream_accept = frozenset({c}) if (c and str(c).startswith("query_")) else None
        else:
            self._rx_stream_accept = None

    def _build_data_output_params(self) -> Dict[str, Any]:
        """Параметры binary/trigger для текущей команды Data Output (из чекбоксов)."""
        cmd = self.current_command
        params: Dict[str, Any] = {}
        if not cmd or not cmd.startswith("query_"):
            return params
        if cmd != "query_uniloglist":
            params["binary"] = self.data_output_format_var.get() == "Бинарный"
        if cmd in DATA_OUTPUT_TRIGGER_OPTIONAL:
            if self.data_output_onchanged_var.get():
                params["trigger"] = "ONCHANGED"
        # Для одиночного запроса в Data Output не подставляем period/rate из LOG:
        # пользователь ожидает короткий query без frequency.
        params["_skip_log_stream_hint"] = True
        # UNILOGLIST — штатный query_uniloglist без raw.
        # ASCII и binary: одиночный запрос — raw строка LOG без периода на проводе (разбор в GUI-приёмнике).
        # Для части binary кадров peel в gui_rx может не снять кадр, если длина в заголовке не совпадает с фактом.
        # PVTSLN (A/B) — в воркере отдельно: query с периодом + точечный UNLOG (до raw не доходит).
        if cmd == "query_uniloglist":
            params["_data_output_raw_once"] = False
        else:
            params["_data_output_raw_once"] = True
        return params

    def _has_streaming_log_for_query(self, cmd_id: str, params: Dict[str, Any]) -> bool:
        """Есть ли активная LOG-подсказка для потока этой query-команды (тогда одиночный запрос не нужен)."""
        if not cmd_id.startswith("query_"):
            return False
        is_bin = bool(params.get("binary"))
        return (cmd_id, is_bin) in self._log_stream_output_hint

    def _build_data_output_oneshot_command(self, cmd_id: str, params: Dict[str, Any]) -> Optional[str]:
        """Собрать raw-команду Data Output без period/rate (one-shot)."""
        is_bin = bool(params.get("binary"))
        msg_name: Optional[str] = None
        for m, meta in _LOG_MSG_STREAM_KEY.items():
            if meta == (cmd_id, is_bin):
                msg_name = m
                break
        if not msg_name:
            return None
        parts: List[str] = [msg_name]
        port = str(params.get("port") or "").strip().upper()
        if port:
            parts.append(port)
        tr = str(params.get("trigger") or "").strip().upper()
        if tr == "ONCHANGED":
            parts.append("ONCHANGED")
        elif cmd_id == "query_obsvbase":
            parts.append("ONCHANGED")
        return " ".join(parts)

    def _apply_log_stream_hint_to_query_params(self, cmd_id: str, params: Dict[str, Any]) -> None:
        """
        После успешного LOG подставить в параметры query_* тот же период и COM, что ушли на приёмник
        (например OBSVMA COM1 10). Вызывается для любой категории и из воркера — на провод уходит согласованно с LOG.
        """
        if not cmd_id.startswith("query_"):
            return
        is_bin = bool(params.get("binary"))
        hint = self._log_stream_output_hint.get((cmd_id, is_bin))
        if not hint or not isinstance(hint, dict):
            return
        if cmd_id == "query_obsvbase":
            p = hint.get("port")
            if p:
                params["port"] = str(p).strip().upper()
            return
        if hint.get("rate") is not None:
            try:
                r = parse_log_period_str(hint["rate"])
                if r > 0:
                    params["rate"] = r
            except ValueError:
                pass
        if cmd_id in _STREAM_QUERY_WITH_COM and hint.get("port"):
            params["port"] = str(hint["port"]).strip().upper()

    def _stream_log_register_hint(self, message: str, port: Optional[str], rate: Any) -> None:
        """Запомнить период/порт из LOG для query_* с тем же форматом (ASCII / binary), что в имени …A/…B."""
        meta = _LOG_MSG_STREAM_KEY.get((message or "").strip().upper())
        if not meta:
            return
        qid, is_bin = meta
        sk: Tuple[str, bool] = (qid, is_bin)
        port_u: Optional[str] = None
        if port:
            p = str(port).strip().upper()
            if p in ("COM1", "COM2", "COM3"):
                port_u = p
        if qid == "query_obsvbase":
            if not port_u:
                return
            self._log_stream_output_hint[sk] = {"port": port_u}
            return
        try:
            rf = parse_log_period_str(rate)
        except ValueError:
            return
        if rf <= 0:
            return
        entry: Dict[str, Any] = {"rate": rf}
        # Порт сохраняем всегда, если был в LOG — иначе UNLOG COMn без имени сообщения не снимает
        # подсказки для BESTNAV и др. (раньше порт клали только для _STREAM_QUERY_WITH_COM).
        if port_u:
            entry["port"] = port_u
        self._log_stream_output_hint[sk] = entry

    def _stream_log_unregister_hint(self, message: Optional[str] = None, port: Optional[str] = None) -> None:
        """
        Снять подсказки потока после UNLOG.
        - без message/port: очистить все;
        - только message: убрать этот тип для всех портов;
        - только port: убрать всё для порта;
        - message+port: убрать только конкретное сочетание.
        """
        msg_u = str(message or "").strip().upper()
        port_u = str(port or "").strip().upper()
        by_message: Optional[Tuple[str, bool]] = _LOG_MSG_STREAM_KEY.get(msg_u) if msg_u else None
        if not msg_u and not port_u:
            self._log_stream_output_hint.clear()
            return
        to_delete: List[Tuple[str, bool]] = []
        for k, entry in self._log_stream_output_hint.items():
            if not isinstance(entry, dict):
                continue
            if by_message is not None and k != by_message:
                continue
            if port_u:
                eport = str(entry.get("port") or "").strip().upper()
                # Пустой порт в подсказке = LOG без COM в команде / старый формат — снимаем при UNLOG COMn.
                if eport and eport != port_u:
                    continue
            to_delete.append(k)
        for k in to_delete:
            self._log_stream_output_hint.pop(k, None)

    def _suspend_binary_log_streams_for_config(self) -> List[Tuple[str, Optional[str], float]]:
        """
        Временно снять активные binary LOG-потоки перед CONFIG*, чтобы бинарный шум не топил ASCII-ответ.
        Возвращает список для восстановления: (message, port, rate).
        """
        inv: Dict[Tuple[str, bool], str] = {v: k for k, v in _LOG_MSG_STREAM_KEY.items()}
        suspended: List[Tuple[str, Optional[str], float]] = []
        for key, hint in list(self._log_stream_output_hint.items()):
            if not isinstance(key, tuple) or len(key) != 2:
                continue
            qid, is_bin = key[0], bool(key[1])
            if not is_bin:
                continue
            if not isinstance(hint, dict):
                continue
            msg = inv.get((qid, True))
            if not msg:
                continue
            rate = hint.get("rate")
            try:
                rate_f = parse_log_period_str(rate if rate is not None else 1.0)
            except ValueError:
                rate_f = 1.0
            port_raw = hint.get("port")
            port_u = str(port_raw).strip().upper() if port_raw else None
            if not self.device.unlog(port=port_u, message=msg).get("error"):
                suspended.append((msg, port_u, rate_f))
                self._stream_log_unregister_hint(message=msg, port=port_u)
        return suspended

    def _resume_binary_log_streams_after_config(self, suspended: List[Tuple[str, Optional[str], float]]) -> None:
        """Восстановить binary LOG-потоки после CONFIG*."""
        for msg, port_u, rate_f in suspended:
            r = self.device.log(message=msg, port=port_u, rate=rate_f)
            if not r.get("error"):
                self._stream_log_register_hint(msg, port_u, rate_f)

    def _stream_log_hints_from_log_params(self, params: Dict[str, Any]) -> None:
        """Обновить подсказки для query_* из параметров LOG (как на провод уйдёт OBSVMA COM1 N)."""
        if not params:
            return
        if params.get("log_apply_one"):
            self._stream_log_register_hint(
                str(params.get("message") or ""),
                str(params.get("port") or ""),
                _coerce_log_rate_param(params.get("rate"), default=1.0),
            )
            return
        if params.get("log_apply_all_on_port"):
            port_u = str(params.get("port") or "").strip().upper()
            rate_ap = _coerce_log_rate_param(params.get("rate"), default=1.0)
            for m in params.get("messages") or _LOG_MESSAGE_TYPE_CHOICES:
                self._stream_log_register_hint(str(m).strip().upper(), port_u, rate_ap)
            return
        if params.get("log_all_listed"):
            msgs = [str(x).strip().upper() for x in (params.get("messages") or _LOG_MESSAGE_TYPE_CHOICES)]
            rates = params.get("rates") or {}
            for port_l in ("COM1", "COM2", "COM3"):
                r_l = _coerce_log_rate_param(rates.get(port_l, 1), default=1.0)
                if r_l <= 0:
                    continue
                for m in msgs:
                    self._stream_log_register_hint(m, port_l, r_l)
            return
        if params.get("log_all_ports"):
            msg = str(params.get("message") or "").strip()
            rates = params.get("rates") or {}
            for port in ("COM1", "COM2", "COM3"):
                r = _coerce_log_rate_param(rates.get(port, 1), default=1.0)
                if r <= 0:
                    continue
                self._stream_log_register_hint(msg, port, r)
            return
        msg = str(params.get("message") or "BESTNAVA").strip()
        pr = params.get("port")
        p = str(pr).strip().upper() if pr is not None and str(pr).strip() else ""
        rti = _coerce_log_rate_param(params.get("rate", 1), default=1.0)
        self._stream_log_register_hint(msg, p or None, rti)

    def _begin_log_stream_hint_transaction(self, params: Dict[str, Any]) -> None:
        """Перед отправкой LOG: снимок подсказок и оптимистическое обновление (чтобы query_* сразу совпадали с панелью)."""
        self._log_stream_hint_rollback = dict(self._log_stream_output_hint)
        self._stream_log_hints_from_log_params(params)

    def _commit_log_stream_hint_transaction(self) -> None:
        self._log_stream_hint_rollback = None

    def _rollback_log_stream_hint_transaction(self) -> None:
        rb = self._log_stream_hint_rollback
        self._log_stream_hint_rollback = None
        if rb is None:
            return
        self._log_stream_output_hint.clear()
        if rb:
            self._log_stream_output_hint.update(rb)

    def _update_data_output_options_visibility(self) -> None:
        """Показать только чекбоксы, применимые к выбранной команде."""
        cmd = self.current_command
        show_binary = bool(cmd and cmd.startswith("query_") and cmd != "query_uniloglist")
        show_onchanged = bool(cmd in DATA_OUTPUT_TRIGGER_OPTIONAL)
        if show_binary:
            self._data_output_binary_row.pack(side=tk.LEFT, padx=(0, 10))
        else:
            self._data_output_binary_row.pack_forget()
        if show_onchanged:
            self._data_output_onchanged_row.pack(side=tk.LEFT, padx=(0, 0))
        else:
            self._data_output_onchanged_row.pack_forget()

    def _on_data_output_option_changed(self) -> None:
        """Смена опций Data Output: только режим парсинга, без авто-отправки команд."""
        self._sync_receiver_parse_mode()
        if self._data_output_resend_after_id:
            try:
                self.root.after_cancel(self._data_output_resend_after_id)
            except Exception:
                pass
            self._data_output_resend_after_id = None

    def _do_resend_data_output_stream(self) -> None:
        # Автопереотправка отключена: поток настраивается вручную через LOG.
        self._data_output_resend_after_id = None
        return

    def _apply_query_data_to_panel(self, command: str, container: dict) -> None:
        """Обновить форму/таблицу из распарсенных данных query_* (ответ one-shot или rx-пейлоад)."""
        if getattr(self, "_data_pane_hidden", False):
            return
        dk = QUERY_COMMAND_DATA_KEY.get(command)
        if not dk or dk not in container:
            return
        data = container[dk]
        if data is None or not isinstance(data, dict):
            return
        try:
            formatted = self.format_data_for_table(command, data)
        except Exception:
            return
        if not formatted:
            return
        self.populate_data_table(formatted, source_command=command)

    def _apply_rx_stream_to_data_panel(self, command: str, payload: dict) -> None:
        """Фоновые кадры: обновлять панель только в категории «Data Output» и для выбранной команды."""
        if self._active_command_category() != "Data Output":
            return
        if command != self.current_command:
            return
        self._apply_query_data_to_panel(command, payload)

    def _start_receiver(self) -> None:
        """Фоновое чтение порта и разбор входящих кадров Unicore."""
        self._stop_receiver()
        self._sync_receiver_parse_mode()
        self._receiver_stop.clear()
        self._receiver_thread = threading.Thread(target=self._receiver_worker, daemon=True)
        self._receiver_thread.start()

    def _stop_receiver(self) -> None:
        self._receiver_stop.set()
        if self._receiver_thread and self._receiver_thread.is_alive():
            self._receiver_thread.join(timeout=2.5)
        self._receiver_thread = None

    def _receiver_worker(self) -> None:
        from um982.gui_rx import process_rx_buffer

        buf = bytearray()
        while not self._receiver_stop.is_set():
            dev = self.device
            if not dev:
                break
            try:
                if not dev.serial_conn or not getattr(dev.serial_conn, "is_open", False):
                    time.sleep(0.2)
                    continue
            except Exception:
                time.sleep(0.2)
                continue
            chunk = b""
            try:
                with self._serial_io_lock:
                    chunk = dev.read_response(timeout=0.25)
            except Exception:
                chunk = b""
            if self._receiver_flush_buf:
                buf.clear()
                self._receiver_flush_buf = False
            if chunk:
                buf.extend(chunk)
                if len(buf) > 800_000:
                    del buf[:400_000]
            pb, pa = self._receiver_parse_mode
            accept = getattr(self, "_rx_stream_accept", None)
            updates = process_rx_buffer(buf, parse_binary=pb, parse_ascii=pa, accept_commands=accept)
            for cmd, payload in updates:
                self._result_queue.put(("rx_update", cmd, payload))
        # очередь может ещё обрабатываться главным потоком

    def load_commands(self) -> None:
        self.commands_by_category = {
            "MODE": [],
            "CONFIG": [],
            "Data Output": [],
            "MASK": [],
            "System": []
        }

        mode_commands = [
            _normalize_command_row(t)
            for t in [
                ("query_mode", "MODE", "Запрос текущего режима"),
                (
                    "set_mode_rover",
                    "MODE ROVER",
                    "Установить режим ROVER",
                    "§3.6 MODE ROVER: сценарий (UAV / SURVEY / AUTOMOTIVE) и подрежим "
                    "(FORMATION, MOW…). Оба поля можно оставить пустыми — режим по умолчанию для модели.",
                ),
                ("set_mode_base", "MODE BASE", "Установить режим BASE"),
                (
                    "set_mode_base_time",
                    "MODE BASE TIME",
                    "Установить режим BASE с самооптимизацией",
                    "Режим базы с подстройкой координат по времени/движению. Параметры в панели слева опциональны.",
                ),
                (
                    "set_mode_heading2",
                    "MODE HEADING2",
                    "Установить режим HEADING2",
                    "§3.7: опционально FIXLENGTH, VARIABLELENGTH, STATIC, LOWDYNAMIC, TRACTOR. "
                    "Пустой вариант — MODE HEADING2 без суффикса (режим по умолчанию в мануале для фикс. базы).",
                ),
            ]
        ]
        self.commands_by_category["MODE"] = mode_commands

        config_commands = [_normalize_command_row(("query_config", "CONFIG", "Запрос конфигурации устройства"))]

        try:
            config_names = get_command_names()
            for cmd_name in config_names:
                if cmd_name not in ["MASK", "UNMASK"]:
                    config_commands.append(
                        _normalize_command_row(
                            (f"config_command:{cmd_name}", f"CONFIG {cmd_name}", f"Настройка {cmd_name}")
                        )
                    )
        except Exception:
            # Игнорируем ошибки при загрузке команд из системы регистрации
            pass

        self.commands_by_category["CONFIG"] = config_commands

        dataoutput_commands = [
            _normalize_command_row(t)
            for t in [
                ("query_version", "VERSION", "Запрос версии устройства"),
                (
                    "query_obsvm",
                    "OBSVM",
                    "Наблюдения главной антенны",
                    "Сырые или сжатые измерения по спутникам для главной антенны. Периодическая выдача на COM настраивается в «System» → «LOG».",
                ),
                (
                    "query_obsvh",
                    "OBSVH",
                    "Наблюдения ведомой антенны",
                    "Аналогично OBSVM, для второй (ведомой) антенны в режиме heading/базы.",
                ),
                ("query_obsvmcmp", "OBSVMCMP", "Сжатые наблюдения"),
                ("query_obsvbase", "OBSVBASE", "Наблюдения базовой станции"),
                ("query_baseinfo", "BASEINFO", "Информация о базовой станции"),
                ("query_gpsion", "GPSION", "Параметры ионосферы GPS"),
                ("query_bdsion", "BDSION", "Параметры ионосферы BDS"),
                ("query_bd3ion", "BD3ION", "Параметры ионосферы BDS-3"),
                ("query_galion", "GALION", "Параметры ионосферы Galileo"),
                ("query_gpsutc", "GPSUTC", "Перевод времени GPS в UTC"),
                ("query_bd3utc", "BD3UTC", "Перевод времени BDS-3 в UTC"),
                (
                    "query_adrnav",
                    "ADRNAV",
                    "RTK позиция и скорость (главная антенна)",
                    "Решение ADR/RTK для основной антенны: координаты, скорость и связанные поля в одном сообщении.",
                ),
                (
                    "query_adrnavh",
                    "ADRNAVH",
                    "RTK позиция и скорость (ведомая антенна)",
                    "То же, что ADRNAV, для ведомой антенны.",
                ),
                ("query_pppnav", "PPPNAV", "Позиция PPP решения"),
                (
                    "query_sppnav",
                    "SPPNAV",
                    "SPP позиция и скорость (главная антенна)",
                    "Стандартная точность (SPP) без RTK для главной антенны.",
                ),
                (
                    "query_sppnavh",
                    "SPPNAVH",
                    "SPP позиция и скорость (ведомая антенна)",
                    "Стандартная точность (SPP) для ведомой антенны.",
                ),
                ("query_stadop", "STADOP", "DOP для решения BESTNAV"),
                (
                    "query_adrdop",
                    "ADRDOP",
                    "DOP для решения ADRNAV",
                    "Документация, раздел 7.3.36: команды ADRDOPA/ADRDOPB, message id 953 (в прошивках иногда 963).",
                ),
                (
                    "query_adrdoph",
                    "ADRDOPH",
                    "DOP для ADRNAVH (ведомая антенна)",
                    "Лог 2121 (DOP по решению ADRNAVH).",
                ),
                ("query_agric", "AGRIC", "AGRIC данные"),
                ("query_pvtsln", "PVTSLN", "PVTSLN данные"),
                ("query_uniloglist", "UNILOGLIST", "Список активных логов"),
                (
                    "query_bestnav",
                    "BESTNAV",
                    "Лучшая позиция и скорость",
                    "Итоговое навигационное решение (часто используется как основной поток позиции).",
                ),
                ("query_bestnavxyz", "BESTNAVXYZ", "Лучшая позиция и скорость (ECEF X,Y,Z)"),
                ("query_hwstatus", "HWSTATUS", "Статус оборудования"),
                ("query_agc", "AGC", "Автоматическая регулировка усиления"),
            ]
        ]
        self.commands_by_category["Data Output"] = dataoutput_commands

        mask_commands = [_normalize_command_row(("query_mask", "MASK", "Запрос конфигурации MASK"))]

        try:
            config_names = get_command_names()
            for cmd_name in config_names:
                if cmd_name == "MASK":
                    mask_commands.append(
                        _normalize_command_row((f"config_command:{cmd_name}", "MASK", "Настройка маскирования"))
                    )
                elif cmd_name == "UNMASK":
                    mask_commands.append(
                        _normalize_command_row((f"config_command:{cmd_name}", "UNMASK", "Снятие маскирования"))
                    )
        except Exception:
            # Игнорируем ошибки при загрузке команд из системы регистрации
            pass

        self.commands_by_category["MASK"] = mask_commands

        system_commands = [
            _normalize_command_row(t)
            for t in [
                (
                    "log",
                    "LOG",
                    "Включить вывод сообщения на порт",
                    "Команда LOG задаёт тип сообщения (например BESTNAVA, GPGGA) и частоту выдачи на COM-порт. "
                    "Первый аргумент — имя типа сообщения; это обязательная часть синтаксиса приёмника. "
                    "Чтобы сохранить настройки после перезагрузки, выполните SAVECONFIG.",
                ),
                (
                    "unlog",
                    "UNLOG",
                    "Остановить вывод сообщений",
                    "Отключает выбранное сообщение на порту или все сообщения на порту (если поле сообщения пусто). "
                    "После UNLOG связь с хостом может «пропасть», пока снова не включён вывод (LOG / Restore output).",
                ),
                (
                    "freset",
                    "FRESET",
                    "Полный сброс NVM и перезапуск",
                    "FRESET очищает энергонезависимую память (NVM) и перезагружает модуль. Все пользовательские "
                    "настройки сбрасываются. После перезапуска скорости UART обычно возвращаются к 115200 бод — "
                    "подключайтесь с этой скоростью, если не меняли её вручную сразу после старта.",
                ),
                (
                    "reset",
                    "RESET",
                    "Перезапуск с выборочной очисткой данных",
                    "Без параметров — только перезагрузка. Со списком через пробел можно очистить отдельные данные: "
                    "EPHEM, ALMANAC, IONUTC, POSITION, XOPARAM, ALL. Это не то же самое, что FRESET (полная очистка NVM).",
                ),
                (
                    "saveconfig",
                    "SAVECONFIG",
                    "Сохранить конфигурацию в NVM",
                    "Записывает текущую конфигурацию в энергонезависимую память, в том числе настройки LOG по портам "
                    "и частотам, если вы их уже задали.",
                ),
            ]
        ]
        self.commands_by_category["System"] = system_commands

        self.all_commands = []
        self._command_meta: Dict[str, Tuple[str, Optional[str]]] = {}
        self._command_to_category: Dict[str, str] = {}
        for category, commands in self.commands_by_category.items():
            for cmd in commands:
                self.all_commands.append(cmd)
                self._command_meta[cmd[0]] = (cmd[2], cmd[3])
                self._command_to_category[cmd[0]] = category

        self.update_command_list()
        self._refresh_run_button_label()
        self._sync_data_output_panel_layout()

    def filter_commands(self, *args: Any) -> None:
        self.update_command_list()

    def _filtered_flat_commands(self, filter_text: str) -> List[Tuple[str, str, str, Optional[str], str]]:
        """Все команды подряд; пятый элемент кортежа — категория (для подписи «категория · …»)."""
        ft = (filter_text or "").strip().lower()
        rows: List[Tuple[str, str, str, Optional[str], str]] = []
        for cat in ALL_COMMAND_UI_CATEGORIES:
            for cmd_id, display_name, summary, detail in self.commands_by_category.get(cat, []):
                rows.append((cmd_id, display_name, summary, detail, cat))
        if not ft:
            return rows
        out: List[Tuple[str, str, str, Optional[str], str]] = []
        for cmd_id, display_name, summary, detail, cat in rows:
            if (
                    ft in display_name.lower()
                    or ft in summary.lower()
                    or (detail and ft in detail.lower())
                    or ft in cmd_id.lower()
                    or ft in cat.lower()
            ):
                out.append((cmd_id, display_name, summary, detail, cat))
        return out

    def _on_category_changed(self, event: Optional[tk.Event] = None) -> None:
        """Смена категории: синхронизировать выбранную команду с новым списком и подписку на приёмник."""
        self._category_switch_in_progress = True
        try:
            self.update_command_list()
        finally:
            self._category_switch_in_progress = False
        self._refresh_run_button_label()
        self._sync_data_output_panel_layout()

    def update_command_list(self) -> None:
        self.command_listbox.delete(0, tk.END)
        selected_category = self.category_var.get()
        filter_text = self.filter_var.get().strip().lower()

        if selected_category == ALL_COMMANDS_COMBO_LABEL:
            filtered_rows = self._filtered_flat_commands(filter_text)
            for _cmd_id, display_name, _s, _d, _cat in filtered_rows:
                self.command_listbox.insert(tk.END, display_name)
            ids = [c[0] for c in filtered_rows]
            need_first = bool(filtered_rows and (self.current_command is None or self.current_command not in ids))
            if need_first:
                prev_cmd = self.current_command
                new_command = filtered_rows[0][0]
                if prev_cmd != new_command:
                    self.clear_data_table()
                    self._receiver_flush_buf = True
                self.current_command = new_command
                self.command_listbox.selection_clear(0, tk.END)
                self.command_listbox.selection_set(0)
                self.command_listbox.activate(0)
                self.command_listbox.see(0)
                self.show_command_params()
                self._update_data_output_options_visibility()
                self._sync_receiver_parse_mode()
                self._refresh_run_button_label()
                self._schedule_selection_fetch()
            elif self.current_command:
                for i, (cmd_id, _, _, _, _) in enumerate(filtered_rows):
                    if cmd_id == self.current_command:
                        self.command_listbox.selection_set(i)
                        self.command_listbox.see(i)
                        self.command_listbox.activate(i)
                        break
        else:
            commands = self.commands_by_category.get(selected_category, [])
            filtered_commands = [
                (cmd_id, display_name, summary, detail)
                for cmd_id, display_name, summary, detail in commands
                if not filter_text
                   or filter_text in display_name.lower()
                   or filter_text in summary.lower()
                   or (detail and filter_text in detail.lower())
            ]
            for _cmd_id, display_name, _s, _d in filtered_commands:
                self.command_listbox.insert(tk.END, display_name)
            ids = [c[0] for c in filtered_commands]
            need_first = bool(
                filtered_commands and (self.current_command is None or self.current_command not in ids)
            )
            if need_first:
                prev_cmd = self.current_command
                new_command = filtered_commands[0][0]
                if prev_cmd != new_command:
                    self.clear_data_table()
                    self._receiver_flush_buf = True
                self.current_command = new_command
                self.command_listbox.selection_clear(0, tk.END)
                self.command_listbox.selection_set(0)
                self.command_listbox.activate(0)
                self.command_listbox.see(0)
                self.show_command_params()
                self._update_data_output_options_visibility()
                self._sync_receiver_parse_mode()
                self._refresh_run_button_label()
            elif self.current_command:
                for i, (cmd_id, _, _, _) in enumerate(filtered_commands):
                    if cmd_id == self.current_command:
                        self.command_listbox.selection_set(i)
                        self.command_listbox.see(i)
                        self.command_listbox.activate(i)
                        break
        self._refresh_command_help_panel()

    def on_command_select(self, event: tk.Event) -> None:
        if getattr(self, "_category_switch_in_progress", False):
            return
        selection = self.command_listbox.curselection()
        if not selection:
            return
        index = selection[0]
        sel_cat = self.category_var.get()
        ft = self.filter_var.get().strip().lower()

        if sel_cat == ALL_COMMANDS_COMBO_LABEL:
            filtered_rows = self._filtered_flat_commands(ft)
            if index >= len(filtered_rows):
                return
            new_command = filtered_rows[index][0]
        else:
            commands = self.commands_by_category.get(sel_cat, [])
            filtered_commands = [
                c
                for c in commands
                if not ft
                   or ft in c[1].lower()
                   or ft in c[2].lower()
                   or (c[3] and ft in c[3].lower())
            ]
            if index >= len(filtered_commands):
                return
            new_command = filtered_commands[index][0]

        if new_command != self.current_command:
            self.clear_data_table()
            self._receiver_flush_buf = True
        self.current_command = new_command
        self.show_command_params()
        self._update_data_output_options_visibility()
        self._sync_receiver_parse_mode()
        self._refresh_run_button_label()
        self._schedule_selection_fetch()
        self._refresh_command_help_panel()

    def _refresh_command_help_panel(self) -> None:
        """Краткое и подробное описание выбранной команды — над полями «Текущее сообщение»."""
        for w in self.command_help_frame.winfo_children():
            w.destroy()
        try:
            self.command_help_frame.pack_forget()
        except tk.TclError:
            pass
        cid = self.current_command
        if not cid:
            return
        if getattr(self, "_data_pane_hidden", False):
            return
        if self._active_command_category() == "Data Output" and not self._show_data_output_help_text:
            return
        meta = getattr(self, "_command_meta", {}).get(cid)
        if not meta:
            return
        summary, detail = meta[0], meta[1]
        parts: List[str] = []
        if summary and str(summary).strip():
            parts.append(str(summary).strip())
        if detail and str(detail).strip():
            parts.append(str(detail).strip())
        text = "\n\n".join(parts)
        if not text:
            return
        try:
            self.data_content_frame.update_idletasks()
            cw = self.data_content_frame.winfo_width()
            if cw > 32:
                self._command_help_wraplength = max(280, cw - 24)
        except tk.TclError:
            pass
        ttk.Label(
            self.command_help_frame,
            text=text,
            wraplength=self._command_help_wraplength,
            justify=tk.LEFT,
            foreground="gray",
            font=("Arial", 10),
        ).pack(anchor=tk.W, padx=4, pady=(0, 6))
        self.command_help_frame.pack(fill=tk.X, padx=0, pady=(0, 2), after=self._message_title_row)

    def _schedule_selection_fetch(self) -> None:
        """После выбора команды в списке — отложенный однократный запрос к приёмнику (без периодики)."""
        if self._selection_fetch_after_id:
            self.root.after_cancel(self._selection_fetch_after_id)
            self._selection_fetch_after_id = None
        self._selection_fetch_after_id = self.root.after(150, self._run_selection_fetch)

    def _selection_fetch_auto_enabled(self, cmd_id: str, category: str) -> bool:
        """
        Разрешён ли авто-запрос при выборе пункта в списке (без нажатия «Запустить»).

        MODE / CONFIG / MASK: только «первые» read-only запросы (режим / CONFIG / MASK).
        System: никогда (все пункты меняют состояние или уже в blocked).
        Data Output: все query_* (одиночный запрос при выборе), кроме уже активных через LOG.
        """
        if not cmd_id.startswith("query_"):
            return False
        if category == "Data Output":
            return True
        if category == "MODE":
            return cmd_id == "query_mode"
        if category == "CONFIG":
            return cmd_id == "query_config"
        if category == "MASK":
            return cmd_id == "query_mask"
        if category == "System":
            return False
        return False

    def _selection_fetch_blocked_commands(self) -> frozenset:
        """Команды без авто-запроса при выборе: не возвращают данные и/или меняют состояние приёмника."""
        return frozenset({
            "restore_output", "log", "unlog", "freset", "reset", "saveconfig",
        })

    def _resolve_selection_fetch(self, cmd_id: str) -> Optional[Tuple[str, Dict[str, Any], Optional[str]]]:
        """
        Какую команду выполнить для обновления данных при выборе пункта.
        Авто-запрос только у явных query_* (в т.ч. query_config, query_mask, query_mode).
        Пункты CONFIG … / MASK / UNMASK (config_command:*) при выборе не дергают приёмник —
        конфигурацию смотрите отдельным пунктом «Запрос конфигурации…» и кнопкой запуска.
        Возвращает (execute_command, params, ui_context) или None.
        """
        if cmd_id in self._selection_fetch_blocked_commands():
            return None
        if cmd_id.startswith("set_mode_"):
            return None
        if cmd_id.startswith("config_command:"):
            return None
        if cmd_id.startswith("query_"):
            cat = self._active_command_category()
            if not self._selection_fetch_auto_enabled(cmd_id, cat):
                return None
            if self._is_data_output_query_id(cmd_id):
                return (cmd_id, self._build_data_output_params(), None)
            p = dict(self.get_params())
            self._apply_log_stream_hint_to_query_params(cmd_id, p)
            return (cmd_id, p, None)
        return None

    def _run_selection_fetch(self) -> None:
        self._selection_fetch_after_id = None
        if not self.device:
            return
        try:
            if not self.device.serial_conn or not self.device.serial_conn.is_open:
                return
        except Exception:
            return
        cmd_id = self.current_command
        resolved = self._resolve_selection_fetch(cmd_id)
        if not resolved:
            return
        execute_cmd, params, ui_ctx = resolved
        if self._is_data_output_query_id(cmd_id) and self._has_streaming_log_for_query(execute_cmd, params):
            self.log_result(
                f"\n{'─' * 50}\n"
                "STREAM FROM LOG DETECTED -> PARSE ONLY (команда повторно не отправляется)\n"
                f"{'─' * 50}\n"
            )
            self._sync_receiver_parse_mode()
            return
        _sel_title = (
            "Одиночный запрос при выборе в списке"
            if self._is_data_output_query_id(cmd_id)
            else "Авто-запрос с приёмника при выборе в списке"
        )
        self.log_result(
            f"\n{'─' * 50}\n"
            f"{_sel_title}"
            f"{f' ({ui_ctx})' if ui_ctx else ''}\n"
            f"{'─' * 50}\n"
        )
        self.run_btn.config(state=tk.DISABLED, text="Выполняется...")
        thread = threading.Thread(
            target=self._oneshot_worker,
            args=(execute_cmd, params),
            kwargs={"from_selection": True, "ui_context": ui_ctx},
            daemon=True,
        )
        thread.start()
        self._schedule_poll()

    def show_command_params(self) -> None:
        self._sync_data_output_panel_layout()
        for widget in self.params_scrollable_frame.winfo_children():
            widget.destroy()
        self.param_widgets.clear()

        if not self.current_command:
            if self.params_expanded:
                self.toggle_params()
            self._update_data_empty_hint()
            self.root.after_idle(self._sync_params_pane_height)
            return

        if self._active_command_category() == "Data Output":
            self._update_data_empty_hint()
            return

        widgets_before = len(self.params_scrollable_frame.winfo_children())

        if self.current_command.startswith("config_command:"):
            cmd_type = self.current_command.split(":")[1]
            self.show_config_command_params(cmd_type)
        elif self.current_command.startswith("query_"):
            self.show_query_command_params(self.current_command)
        elif self.current_command.startswith("set_mode_"):
            self.show_mode_command_params(self.current_command)
        elif self.current_command in ("restore_output", "log", "unlog", "freset", "reset", "saveconfig"):
            self.show_system_command_params(self.current_command)
        else:
            ttk.Label(self.params_scrollable_frame, text="Параметры не определены").pack(pady=10)

        widgets_after = len(self.params_scrollable_frame.winfo_children())
        has_params = widgets_after > widgets_before and (
                len(self.param_widgets) > 0 or self.current_command == "log"
        )

        if not has_params and self.params_expanded:
            self.toggle_params()
        elif has_params and not self.params_expanded:
            self.toggle_params()

        self._update_data_empty_hint()
        self.root.after_idle(self._sync_params_pane_height)

    def _execute_config_standalone(self, params: Dict[str, Any]) -> dict:
        """
        CONFIG STANDALONE (§4.7): подкоманда DISABLE / ENABLE и вариант DEFAULT | COORDS | TIME
        (GUI-ключ standalone_variant не передаётся в um982_commands).
        """
        variant = str(params.get("standalone_variant", "")).strip().upper()
        sub = str(params.get("subcommand", "ENABLE")).strip().upper()
        base = {k: v for k, v in params.items() if k not in ("standalone_variant",)}

        if sub == "DISABLE":
            return self.device.config_command("STANDALONE", subcommand="DISABLE")

        if sub != "ENABLE":
            return self.device.config_command("STANDALONE",
                                              **{k: v for k, v in base.items() if k != "standalone_variant"})

        if not variant:
            return {
                "error": (
                    "При ENABLE выберите «Тип настройки»: DEFAULT (без чисел), COORDS (координаты) или TIME (секунды)."
                ),
            }

        if variant == "DEFAULT":
            if any(k in base for k in ("latitude", "longitude", "altitude", "time")):
                return {
                    "error": (
                        "Вариант «По умолчанию»: не заполняйте координаты и время — будет отправлено CONFIG STANDALONE ENABLE."
                    ),
                }
            return self.device.config_command("STANDALONE", subcommand="ENABLE")

        if variant == "COORDS":
            need = ("latitude", "longitude", "altitude")
            missing = [k for k in need if k not in base]
            if missing:
                return {"error": f"Для координат приёмника укажите все поля: {', '.join(missing)}."}
            if "time" in base:
                return {"error": "В режиме координат не указывайте «Время перехода»."}
            return self.device.config_command(
                "STANDALONE",
                subcommand="ENABLE",
                latitude=base["latitude"],
                longitude=base["longitude"],
                altitude=base["altitude"],
            )

        if variant == "TIME":
            if "time" not in base:
                return {"error": "Укажите время перехода в режим STANDALONE (3–100 с)."}
            if any(k in base for k in ("latitude", "longitude", "altitude")):
                return {"error": "В режиме времени не указывайте координаты."}
            return self.device.config_command("STANDALONE", subcommand="ENABLE", time=base["time"])

        return {"error": f"Неизвестный тип настройки: {variant!r}. Допустимо: DEFAULT, COORDS, TIME."}

    def _execute_config_smooth(self, params: Dict[str, Any]) -> dict:
        """
        CONFIG SMOOTH (§4.12): в GUI раздельные поля smooth_epochs (RTKHEIGHT/HEADING) и
        smooth_psrvel (PSRVEL); в um982_commands уходит один ключ parameter.
        """
        engine = str(params.get("computing_engine", "")).strip().upper()
        if engine in ("RTKHEIGHT", "HEADING"):
            if "smooth_epochs" not in params:
                return {"error": "Укажите длину сглаживания в эпохах (целое число 0–100)."}
            raw = params["smooth_epochs"]
            if isinstance(raw, bool):
                return {"error": "Некорректное значение длины сглаживания."}
            if isinstance(raw, float) and raw.is_integer():
                ep = int(raw)
            elif isinstance(raw, int):
                ep = raw
            else:
                try:
                    ep = int(str(raw).strip())
                except (TypeError, ValueError):
                    return {"error": "Длина сглаживания должна быть целым числом эпох (0–100)."}
            return self.device.config_command("SMOOTH", computing_engine=engine, parameter=ep)
        if engine == "PSRVEL":
            v = params.get("smooth_psrvel")
            if v is None or (isinstance(v, str) and not v.strip()):
                return {"error": "Для PSRVEL выберите enable или disable."}
            s = str(v).strip().lower()
            if s not in ("enable", "disable"):
                return {"error": "Для PSRVEL допустимо только enable или disable."}
            return self.device.config_command("SMOOTH", computing_engine="PSRVEL", parameter=s)
        return {"error": f"Неизвестный движок: {engine!r}. Допустимо: RTKHEIGHT, HEADING, PSRVEL."}

    def _execute_config_mask(self, params: Dict[str, Any]) -> dict:
        """
        CONFIG MASK (§5.2): в GUI ключ mask_mode (ELEV_FREQ | PRN | RTCMCNO | CNO) не передаётся в um982_commands.
        """
        mode = str(params.get("mask_mode", "")).strip().upper()
        if not mode:
            return {"error": "Выберите тип команды MASK (раздел 5.2)."}

        if mode == "ELEV_FREQ":
            kw: Dict[str, Any] = {}
            if "elevation" in params:
                kw["elevation"] = params["elevation"]
            sys = params.get("system")
            if sys is not None and str(sys).strip():
                kw["system"] = str(sys).strip()
            fq = _frequency_token_from_dropdown(params.get("frequency"))
            if fq:
                kw["frequency"] = fq.upper()
            if not kw:
                return {
                    "error": (
                        "Задайте хотя бы одно из: угол возвышения (°), систему GNSS или частоту "
                        "(угол при этом необязателен)."
                    ),
                }
            return self.device.config_command("MASK", **kw)

        if mode == "PRN":
            if not params.get("system") or not str(params["system"]).strip():
                return {"error": "Укажите систему спутников для маски по PRN."}
            if "prn_id" not in params:
                return {"error": "Укажите идентификатор спутника (PRN), целое число ≥ 1."}
            return self.device.config_command(
                "MASK",
                mask_type="PRN",
                system=str(params["system"]).strip(),
                prn_id=params["prn_id"],
            )

        if mode == "RTCMCNO":
            if "cno" not in params:
                return {"error": "Укажите порог C/N0 для RTCMCNO (неотрицательное число)."}
            kw: Dict[str, Any] = {"mask_type": "RTCMCNO", "cno": params["cno"]}
            fq = _frequency_token_from_dropdown(params.get("frequency"))
            if fq:
                kw["frequency"] = fq.upper()
            return self.device.config_command("MASK", **kw)

        if mode == "CNO":
            if "cno" not in params:
                return {"error": "Укажите порог C/N0 для типа CNO (ограничение выдачи OBSV)."}
            kw: Dict[str, Any] = {"mask_type": "CNO", "cno": params["cno"]}
            fq = _frequency_token_from_dropdown(params.get("frequency"))
            if fq:
                kw["frequency"] = fq.upper()
            return self.device.config_command("MASK", **kw)

        return {"error": f"Неизвестный режим MASK: {mode!r}."}

    def _execute_config_unmask(self, params: Dict[str, Any]) -> dict:
        """CONFIG UNMASK (§5.3): в GUI ключ unmask_mode не передаётся в um982_commands."""
        mode = str(params.get("unmask_mode", "")).strip().upper()
        if mode not in ("SYS_FREQ", "SATELLITE"):
            return {"error": "Выберите вариант UNMASK: система/частота или спутник (§5.3)."}

        rest = {k: v for k, v in params.items() if k != "unmask_mode"}

        if mode == "SATELLITE":
            sys = str(rest.get("system") or "").strip()
            if not sys:
                return {"error": "Укажите систему GNSS для команды UNMASK … PRN …"}
            if "prn_id" not in rest:
                return {"error": "Укажите номер спутника (PRN), целое число ≥ 1."}
            prn = rest["prn_id"]
            if isinstance(prn, float) and prn.is_integer():
                prn = int(prn)
            if not isinstance(prn, int) or prn < 1:
                return {"error": "PRN должен быть целым числом ≥ 1."}
            return self.device.config_command("UNMASK", system=sys, prn_id=prn)

        sys = str(rest.get("system") or "").strip()
        fq = _frequency_token_from_dropdown(rest.get("frequency"))
        if sys and fq:
            return {
                "error": "Задайте либо систему GNSS, либо частоту — не оба сразу (одна команда UNMASK).",
            }
        if not sys and not fq:
            return {"error": "Выберите систему GNSS или частоту для размаскирования."}
        kw: Dict[str, Any] = {}
        if sys:
            kw["system"] = sys
        if fq:
            kw["frequency"] = fq.upper()
        return self.device.config_command("UNMASK", **kw)

    def _update_data_empty_hint(self) -> None:
        """Текст подсказки, когда форма «Текущее сообщение» пуста (зависит от категории и команды)."""
        if not hasattr(self, "_data_empty_default_text"):
            return
        hint = self._data_empty_default_text
        if self._active_command_category() == "Data Output" and not self._show_data_output_help_text:
            hint = ""
        self.data_empty_label.config(text=hint, wraplength=720)

    def show_config_command_params(self, cmd_type: str) -> None:
        """Отображение параметров CONFIG команды"""
        try:
            cmd_def = get_command_definition(cmd_type)
            if not cmd_def:
                ttk.Label(self.params_scrollable_frame, text=f"Команда {cmd_type} не найдена").pack()
                return

            sig = inspect.signature(cmd_def.command_builder)

            if cmd_type == "COM":
                self.create_param_widget("port", "Порт", "COM1", ["COM1", "COM2", "COM3"], required=True)
                self.create_param_widget(
                    "baudrate",
                    "Скорость",
                    "115200",
                    ["9600", "19200", "38400", "57600", "115200", "230400", "460800", "921600"],
                    required=True,
                )
                self.create_param_widget("data_bits", "Биты данных", "8", ["8"])
                self.create_param_widget("parity", "Четность", "N", ["N", "E", "O"])
                self.create_param_widget("stop_bits", "Стоп-биты", "1", ["1", "2"])

            elif cmd_type == "PPS":
                self.create_param_widget("enable", "Режим", "DISABLE", ["DISABLE", "ENABLE", "ENABLE2", "ENABLE3"],
                                         required=True)
                self.create_param_widget("timeref", "Временная ссылка", "GPS", ["GPS", "BDS", "GAL", "GLO"])
                self.create_param_widget("polarity", "Полярность", "POSITIVE", ["POSITIVE", "NEGATIVE"])
                self.create_param_widget("width", "Ширина (мкс)", "500000")
                self.create_param_widget("period", "Период (мс)", "1000")
                self.create_param_widget("rf_delay", "RF задержка (нс)", "0")
                self.create_param_widget("user_delay", "Пользовательская задержка (нс)", "0")

            elif cmd_type == "DGPS":
                self.create_param_widget("timeout", "Таймаут (сек)", "300", required=True)

            elif cmd_type == "RTK":
                self.create_param_widget("subcommand", "Подкоманда", "TIMEOUT",
                                         ["TIMEOUT", "RELIABILITY", "USER_DEFAULTS", "RESET", "DISABLE"], required=True)
                self.create_param_widget("timeout", "Таймаут (сек)", "600")
                self.create_param_widget("param1", "Параметр 1", "")
                self.create_param_widget("param2", "Параметр 2", "")
                ttk.Label(
                    self.params_scrollable_frame,
                    text="Звёздочка (*) у таймаута появляется только при подкоманде TIMEOUT.",
                    foreground="gray",
                    wraplength=520,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=4, pady=(0, 4))
                var_rtk, w_rtk = self._param_vw("subcommand")

                def _sync_rtk_ui(*_: object) -> None:
                    sub = (var_rtk.get() or "").strip().upper()
                    self._set_param_required_star("timeout", sub == "TIMEOUT")

                _sync_rtk_ui()
                try:
                    var_rtk.trace_add("write", _sync_rtk_ui)
                except AttributeError:
                    var_rtk.trace("w", _sync_rtk_ui)
                w_rtk.bind("<<ComboboxSelected>>", lambda _e: _sync_rtk_ui())

            elif cmd_type == "STANDALONE":
                ttk.Label(
                    self.params_scrollable_frame,
                    text=(
                        "CONFIG STANDALONE: при DISABLE другие параметры не используются."  # 4.7
                        # "При ENABLE сначала выберите тип настройки, затем заполните только соответствующие поля. Вариант «По умолчанию» — команда "
                        # "CONFIG STANDALONE ENABLE без чисел: приёмник берёт автоматически рассчитанную позицию и "
                        # "входит в STANDALONE через 100 с (см. мануал)."
                    ),
                    foreground="gray",
                    wraplength=540,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=2, pady=(0, 8))
                self.create_param_widget("subcommand", "Режим", "ENABLE", ["ENABLE", "DISABLE"], required=True)
                self.create_param_widget(
                    "standalone_variant",
                    "Тип настройки (только при ENABLE)",
                    "DEFAULT",
                    ["DEFAULT", "COORDS", "TIME"],
                    required=True,
                )
                ttk.Label(
                    self.params_scrollable_frame,
                    text=(
                        "DEFAULT — без координат и без времени. COORDS — широта, долгота и высота. TIME — только "
                        "Время перехода."
                    ),
                    foreground="gray",
                    wraplength=540,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=2, pady=(0, 6))
                self.create_param_widget("latitude", "Широта (°)", "")
                self.create_param_widget("longitude", "Долгота (°)", "")
                self.create_param_widget("altitude", "Высота (м)", "")
                self.create_param_widget("time", "Время перехода в режим (с)", "")

                var_sub, w_sub = self._param_vw("subcommand")
                var_sv, w_sv = self._param_vw("standalone_variant")
                _, w_lat = self._param_vw("latitude")
                _, w_lon = self._param_vw("longitude")
                _, w_alt = self._param_vw("altitude")
                _, w_time = self._param_vw("time")

                def _sync_standalone_ui(*_: object) -> None:
                    sub = (var_sub.get() or "").strip().upper()
                    if sub == "DISABLE":
                        w_sv.configure(state="disabled")
                        for w in (w_lat, w_lon, w_alt, w_time):
                            w.configure(state="disabled")
                        for n in ("latitude", "longitude", "altitude", "time"):
                            self._set_param_required_star(n, False)
                        return
                    w_sv.configure(state="readonly")
                    v = (var_sv.get() or "DEFAULT").strip().upper()
                    if v == "COORDS":
                        w_lat.configure(state="normal")
                        w_lon.configure(state="normal")
                        w_alt.configure(state="normal")
                        w_time.configure(state="disabled")
                        self._set_param_required_star("latitude", True)
                        self._set_param_required_star("longitude", True)
                        self._set_param_required_star("altitude", True)
                        self._set_param_required_star("time", False)
                    elif v == "TIME":
                        for w in (w_lat, w_lon, w_alt):
                            w.configure(state="disabled")
                        w_time.configure(state="normal")
                        for n in ("latitude", "longitude", "altitude"):
                            self._set_param_required_star(n, False)
                        self._set_param_required_star("time", True)
                    else:
                        for w in (w_lat, w_lon, w_alt, w_time):
                            w.configure(state="disabled")
                        for n in ("latitude", "longitude", "altitude", "time"):
                            self._set_param_required_star(n, False)

                _sync_standalone_ui()
                try:
                    var_sub.trace_add("write", _sync_standalone_ui)
                    var_sv.trace_add("write", _sync_standalone_ui)
                except AttributeError:
                    var_sub.trace("w", _sync_standalone_ui)
                    var_sv.trace("w", _sync_standalone_ui)
                w_sub.bind("<<ComboboxSelected>>", lambda _e: _sync_standalone_ui())
                w_sv.bind("<<ComboboxSelected>>", lambda _e: _sync_standalone_ui())

            elif cmd_type == "HEADING":
                self.create_param_widget("subcommand", "Подкоманда", "FIXLENGTH",
                                         ["FIXLENGTH", "VARIABLELENGTH", "STATIC", "LOWDYNAMIC", "TRACTOR",
                                          "LENGTH", "RELIABILITY", "OFFSET"], required=True)
                self.create_param_widget("param1", "Параметр 1", "")
                self.create_param_widget("param2", "Параметр 2", "")
                self.create_param_widget("heading_offset", "Смещение heading", "")
                self.create_param_widget("pitch_offset", "Смещение pitch", "")

            elif cmd_type == "SBAS":
                self.create_param_widget("subcommand", "Подкоманда", "ENABLE", ["ENABLE", "DISABLE", "TIMEOUT"],
                                         required=True)
                self.create_param_widget("mode", "Режим", "AUTO",
                                         ["AUTO", "WAAS", "GAGAN", "MSAS", "EGNOS", "SDCM", "BDS"])
                self.create_param_widget("timeout", "Таймаут (сек)", "600")
                var_sbas, w_sbas = self._param_vw("subcommand")
                _, w_mode = self._param_vw("mode")
                _, w_timeout = self._param_vw("timeout")

                def _sync_sbas_ui(*_: object) -> None:
                    sub = (var_sbas.get() or "").strip().upper()
                    if sub == "DISABLE":
                        w_mode.configure(state="disabled")
                        w_timeout.configure(state="disabled")
                    elif sub == "ENABLE":
                        w_mode.configure(state="readonly")
                        w_timeout.configure(state="disabled")
                    elif sub == "TIMEOUT":
                        w_mode.configure(state="disabled")
                        w_timeout.configure(state="normal")
                    else:
                        w_mode.configure(state="readonly")
                        w_timeout.configure(state="normal")
                    self._set_param_required_star("mode", sub == "ENABLE")
                    self._set_param_required_star("timeout", sub == "TIMEOUT")

                _sync_sbas_ui()
                try:
                    var_sbas.trace_add("write", _sync_sbas_ui)
                except AttributeError:
                    var_sbas.trace("w", _sync_sbas_ui)
                w_sbas.bind("<<ComboboxSelected>>", lambda _e: _sync_sbas_ui())
                w_sbas.configure(state="readonly")

            elif cmd_type == "PPP":
                # CONFIG PPP [parameter1] [parameter2]
                # CONFIG PPP CONVERGE [HorSTD] [VerSTD]
                # subcommand:
                #   - ENABLE  <service>      (B2B-PPP / SSR-RX)
                #   - DATUM   <datum>        (WGS84 / PPPORIGINAL)
                #   - CONVERGE <HorSTD> <VerSTD> (см)
                #   - DISABLE
                self.create_param_widget(
                    "subcommand",
                    "Подкоманда",
                    "ENABLE",
                    ["ENABLE", "DATUM", "CONVERGE", "DISABLE"],
                    required=True,
                )
                self.create_param_widget(
                    "service",
                    "Сервис PPP",
                    "B2B-PPP",
                    ["B2B-PPP", "SSR-RX"],
                )
                self.create_param_widget(
                    "datum",
                    "Система координат (DATUM)",
                    "PPPORIGINAL",
                    ["WGS84", "PPPORIGINAL"],
                )
                self.create_param_widget("hor_std", "HorSTD (см)", "")
                self.create_param_widget("ver_std", "VerSTD (см)", "")
                ttk.Label(
                    self.params_scrollable_frame,
                    text="Звёздочка (*) у HorSTD / VerSTD появляется только для подкоманды CONVERGE.",
                    foreground="gray",
                    wraplength=520,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=4, pady=(0, 4))
                var_ppp, w_ppp = self._param_vw("subcommand")

                def _sync_ppp_ui(*_: object) -> None:
                    sub = (var_ppp.get() or "").strip().upper()
                    need = sub == "CONVERGE"
                    self._set_param_required_star("hor_std", need)
                    self._set_param_required_star("ver_std", need)

                _sync_ppp_ui()
                try:
                    var_ppp.trace_add("write", _sync_ppp_ui)
                except AttributeError:
                    var_ppp.trace("w", _sync_ppp_ui)
                w_ppp.bind("<<ComboboxSelected>>", lambda _e: _sync_ppp_ui())

            elif cmd_type == "EVENT":
                self.create_param_widget("subcommand", "Подкоманда", "DISABLE", ["ENABLE", "DISABLE"], required=True)
                self.create_param_widget("polarity", "Полярность", "POSITIVE", ["POSITIVE", "NEGATIVE"])
                self.create_param_widget("tguard", "TGUARD (мс)", "4")
                ttk.Label(
                    self.params_scrollable_frame,
                    text=(
                        "Если интервал времени меньше TGUARD, второе событие будет проигнорировано. "
                        "По умолчанию: 4, минимум: 2, максимум: 3,599,999."
                    ),
                    foreground="gray",
                    wraplength=520,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=4, pady=(0, 4))
                var_event, w_event = self._param_vw("subcommand")
                _, w_pol = self._param_vw("polarity")
                _, w_tguard = self._param_vw("tguard")

                def _sync_event_ui(*_: object) -> None:
                    sub = (var_event.get() or "").strip().upper()
                    enable = sub == "ENABLE"
                    w_pol.configure(state="readonly" if enable else "disabled")
                    w_tguard.configure(state="normal" if enable else "disabled")
                    self._set_param_required_star("polarity", enable)
                    self._set_param_required_star("tguard", enable)

                _sync_event_ui()
                try:
                    var_event.trace_add("write", _sync_event_ui)
                except AttributeError:
                    var_event.trace("w", _sync_event_ui)
                w_event.bind("<<ComboboxSelected>>", lambda _e: _sync_event_ui())
                w_event.configure(state="readonly")

            elif cmd_type == "UNDULATION":
                # CONFIG UNDULATION [parameter]
                # AUTO (по умолчанию) или пользовательское значение разделения геоида (метры)
                self.create_param_widget("mode", "Режим", "AUTO", ["AUTO"])
                self.create_param_widget("separation", "Разделение геоида (м)", "")

            elif cmd_type == "SMOOTH":
                ttk.Label(
                    self.params_scrollable_frame,
                    text=(
                        # "§4.12 CONFIG SMOOTH — сглаживание при расчёте RTK (высота), heading и доплеровской скорости в SPPNAV."
                        "По умолчанию на приёмнике сглаживание выключено, пока не задана команда.\n"
                        "RTKHEIGHT — длина сглаживания высоты RTK: целое число эпох от 0 до 100.\n"
                        "HEADING — длина сглаживания курса: целое число эпох от 0 до 100.\n"
                        "PSRVEL — включение или выключение сглаживания доплеровской скорости в SPPNAV."
                    ),
                    foreground="gray",
                    wraplength=540,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=2, pady=(0, 8))
                self.create_param_widget(
                    "computing_engine",
                    "Движок вычислений",
                    "RTKHEIGHT",
                    ["RTKHEIGHT", "HEADING", "PSRVEL"],
                    required=True,
                )
                ttk.Label(
                    self.params_scrollable_frame,
                    text=(
                        "Красная * появляется только у поля, обязательного для выбранного движка: эпохи — для "
                        "RTKHEIGHT/HEADING, enable/disable — для PSRVEL."
                    ),
                    foreground="gray",
                    wraplength=520,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=2, pady=(0, 6))
                self.create_param_widget(
                    "smooth_epochs",
                    "Длина сглаживания (эпохи, 0–100) для RTKHEIGHT / HEADING",
                    "",
                )
                self.create_param_widget(
                    "smooth_psrvel",
                    "Сглаживание доплеровской скорости SPPNAV (PSRVEL)",
                    "disable",
                    ["enable", "disable"],
                )

                var_eng, w_eng = self._param_vw("computing_engine")
                _, w_ep = self._param_vw("smooth_epochs")
                _, w_ps = self._param_vw("smooth_psrvel")

                def _sync_smooth_ui(*_: object) -> None:
                    eng = (var_eng.get() or "").strip().upper()
                    if eng in ("RTKHEIGHT", "HEADING"):
                        w_ep.configure(state="normal")
                        w_ps.configure(state="disabled")
                        self._set_param_required_star("smooth_epochs", True)
                        self._set_param_required_star("smooth_psrvel", False)
                    elif eng == "PSRVEL":
                        w_ep.configure(state="disabled")
                        w_ps.configure(state="readonly")
                        self._set_param_required_star("smooth_epochs", False)
                        self._set_param_required_star("smooth_psrvel", True)
                    else:
                        w_ep.configure(state="normal")
                        w_ps.configure(state="readonly")
                        self._set_param_required_star("smooth_epochs", False)
                        self._set_param_required_star("smooth_psrvel", False)

                _sync_smooth_ui()
                try:
                    var_eng.trace_add("write", _sync_smooth_ui)
                except AttributeError:
                    var_eng.trace("w", _sync_smooth_ui)
                w_eng.bind("<<ComboboxSelected>>", lambda _e: _sync_smooth_ui())

            elif cmd_type == "MMP":
                # CONFIG MMP [parameter]
                # ENABLE / DISABLE (по умолчанию DISABLE)
                self.create_param_widget(
                    "state",
                    "Режим",
                    "DISABLE",
                    ["ENABLE", "DISABLE"],
                )

            elif cmd_type == "AGNSS":
                # CONFIG AGNSS [parameter]
                # ENABLE / DISABLE (по умолчанию DISABLE)
                self.create_param_widget(
                    "state",
                    "Режим",
                    "DISABLE",
                    ["ENABLE", "DISABLE"],
                )

            elif cmd_type == "MASK":
                ttk.Label(
                    self.params_scrollable_frame,
                    text=(
                        "CONFIG MASK: маскирование по углу возвышения и/или системе/частоте, по конкретному "  # §5.2 
                        "спутнику (система + PRN) или по порогу C/N0."
                    ),
                    foreground="gray",
                    wraplength=540,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=2, pady=(0, 8))
                self.create_param_widget(
                    "mask_mode",
                    "Тип маски (§5.2)",
                    "ELEV_FREQ",
                    ["ELEV_FREQ", "PRN", "RTCMCNO", "CNO"],
                    required=True,
                )
                ttk.Label(
                    self.params_scrollable_frame,
                    text=(
                        "ELEV_FREQ — угол возвышения (необязательно) и/или система и/или частота (нужно хотя бы одно). "
                        "PRN — система + номер спутника. RTCMCNO / CNO — порог C/N0; частота для них необязательна "
                        "(пусто — на все частоты)."
                    ),
                    foreground="gray",
                    wraplength=540,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=2, pady=(0, 6))
                self.create_param_widget(
                    "elevation",
                    "Угол возвышения (°), -90…90 (необязательно для ELEV_FREQ)",
                    "",
                )
                self.create_param_widget(
                    "system",
                    "Система GNSS",
                    "",
                    ["", "GPS", "BDS", "GLO", "GAL", "QZSS", "IRNSS"],
                )
                self.create_param_widget(
                    "frequency",
                    "Частота (необязательно)",
                    "",
                    _mask_unmask_frequency_dropdown_values(),
                )
                self.create_param_widget("prn_id", "Номер спутника (PRN)", "")
                self.create_param_widget(
                    "cno",
                    "Порог C/N0 (RTCMCNO или CNO)",
                    "",
                )

                var_mm, w_mm = self._param_vw("mask_mode")
                _, w_el = self._param_vw("elevation")
                _, w_sys = self._param_vw("system")
                _, w_freq = self._param_vw("frequency")
                _, w_prn = self._param_vw("prn_id")
                _, w_cno = self._param_vw("cno")

                def _sync_mask_ui(*_: object) -> None:
                    m = (var_mm.get() or "ELEV_FREQ").strip().upper()
                    self._set_param_required_star("elevation", False)
                    self._set_param_required_star("system", False)
                    self._set_param_required_star("frequency", False)
                    self._set_param_required_star("prn_id", False)
                    self._set_param_required_star("cno", False)
                    if m == "ELEV_FREQ":
                        for w in (w_el, w_sys, w_freq):
                            w.configure(state="readonly" if w in (w_sys, w_freq) else "normal")
                        w_prn.configure(state="disabled")
                        w_cno.configure(state="disabled")
                    elif m == "PRN":
                        w_el.configure(state="disabled")
                        w_sys.configure(state="readonly")
                        w_freq.configure(state="disabled")
                        w_prn.configure(state="normal")
                        w_cno.configure(state="disabled")
                        self._set_param_required_star("system", True)
                        self._set_param_required_star("prn_id", True)
                    elif m in ("RTCMCNO", "CNO"):
                        w_el.configure(state="disabled")
                        w_sys.configure(state="disabled")
                        w_freq.configure(state="readonly")
                        w_prn.configure(state="disabled")
                        w_cno.configure(state="normal")
                        self._set_param_required_star("cno", True)
                    else:
                        for w in (w_el, w_sys, w_freq, w_prn, w_cno):
                            w.configure(state="normal")

                _sync_mask_ui()
                try:
                    var_mm.trace_add("write", _sync_mask_ui)
                except AttributeError:
                    var_mm.trace("w", _sync_mask_ui)
                w_mm.bind("<<ComboboxSelected>>", lambda _e: _sync_mask_ui())
                w_mm.configure(state="readonly")

            elif cmd_type == "UNMASK":
                ttk.Label(
                    self.params_scrollable_frame,
                    text=(
                        "§5.3 CONFIG UNMASK: включение слежения за системой/частотой (UNMASK GPS, UNMASK E5a) "
                        "или за конкретным спутником (UNMASK GPS PRN 12). В одной команде — либо система, либо частота; "
                        "для спутника нужны система и PRN."
                    ),
                    foreground="gray",
                    wraplength=540,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=2, pady=(0, 8))
                self.create_param_widget(
                    "unmask_mode",
                    "Вариант UNMASK (§5.3)",
                    "SYS_FREQ",
                    ["SYS_FREQ", "SATELLITE"],
                    required=True,
                )
                ttk.Label(
                    self.params_scrollable_frame,
                    text=(
                        "SYS_FREQ — одна система GNSS или одна частота (подпись «код — система»), без звёздочек: "
                        "обязательно ровно одно из двух полей. SATELLITE — система и PRN; у них появится *."
                    ),
                    foreground="gray",
                    wraplength=540,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=2, pady=(0, 6))
                self.create_param_widget(
                    "system",
                    "Система GNSS",
                    "",
                    ["", "GPS", "BDS", "GLO", "GAL", "QZSS", "IRNSS"],
                )
                self.create_param_widget(
                    "frequency",
                    "Частота (код — система)",
                    "",
                    _mask_unmask_frequency_dropdown_values(),
                )
                self.create_param_widget("prn_id", "Номер спутника (PRN)", "")

                var_um, w_um = self._param_vw("unmask_mode")
                _, w_sys = self._param_vw("system")
                _, w_freq = self._param_vw("frequency")
                _, w_prn = self._param_vw("prn_id")

                def _sync_unmask_ui(*_: object) -> None:
                    m = (var_um.get() or "SYS_FREQ").strip().upper()
                    sat = m == "SATELLITE"
                    self._set_param_required_star("system", sat)
                    self._set_param_required_star("prn_id", sat)
                    self._set_param_required_star("frequency", False)
                    if m == "SYS_FREQ":
                        w_sys.configure(state="readonly")
                        w_freq.configure(state="readonly")
                        w_prn.configure(state="disabled")
                    else:
                        w_sys.configure(state="readonly")
                        w_freq.configure(state="disabled")
                        w_prn.configure(state="normal")

                _sync_unmask_ui()
                try:
                    var_um.trace_add("write", _sync_unmask_ui)
                except AttributeError:
                    var_um.trace("w", _sync_unmask_ui)
                w_um.bind("<<ComboboxSelected>>", lambda _e: _sync_unmask_ui())
                w_um.configure(state="readonly")

            else:
                ttk.Label(self.params_scrollable_frame, text=f"Параметры для {cmd_type} не определены").pack()

        except Exception as e:
            ttk.Label(self.params_scrollable_frame, text=f"Ошибка: {str(e)}").pack()

    def show_query_command_params(self, method_name: str) -> None:
        if method_name in ["query_obsvm", "query_obsvh", "query_obsvmcmp"]:
            self.create_param_widget(
                "port",
                "Порт (пусто = текущий, без COM в команде)",
                "",
                ["", "COM1", "COM2", "COM3"],
            )
            self.create_param_widget("rate", "Частота (вывод устройства, 1 = раз в эпоху)", "1")
            self.create_param_widget("binary", "Формат сообщения", "ASCII", ["ASCII", "Бинарный"])
            ttk.Label(
                self.params_scrollable_frame,
                text="Периодическая выдача OBSV на COM-порты — «System» → «LOG».",
                foreground="gray",
            ).pack(pady=4)

        elif method_name == "query_obsvbase":
            self.create_param_widget(
                "port",
                "Порт (пусто = текущий, без COM в команде)",
                "",
                ["", "COM1", "COM2", "COM3"],
            )
            self.create_param_widget("trigger", "Триггер", "ONCHANGED", ["ONCHANGED"])
            self.create_param_widget("binary", "Формат сообщения", "ASCII", ["ASCII", "Бинарный"])

        elif method_name in ["query_baseinfo", "query_gpsion", "query_bdsion", "query_bd3ion",
                             "query_galion", "query_gpsutc", "query_bd3utc",
                             "query_adrnav", "query_adrnavh", "query_pppnav",
                             "query_sppnav", "query_sppnavh",
                             "query_stadop", "query_adrdop", "query_adrdoph"]:
            self.create_param_widget("rate", "Частота", "1")
            self.create_param_widget("trigger", "Триггер", "", ["", "ONCHANGED"])
            self.create_param_widget("binary", "Формат сообщения", "ASCII", ["ASCII", "Бинарный"])

        elif method_name == "query_agric":
            self.create_param_widget("port", "Порт", "", ["", "COM1", "COM2", "COM3"])
            self.create_param_widget("rate", "Частота", "1")
            self.create_param_widget("binary", "Формат сообщения", "ASCII", ["ASCII", "Бинарный"])

        elif method_name in ["query_pvtsln", "query_bestnav", "query_bestnavxyz"]:
            self.create_param_widget("rate", "Частота", "1")
            self.create_param_widget("binary", "Формат сообщения", "ASCII", ["ASCII", "Бинарный"])

        elif method_name in ["query_version", "query_config", "query_uniloglist", "query_mode"]:
            if method_name == "query_version":
                self.create_param_widget("binary", "Формат сообщения", "ASCII", ["ASCII", "Бинарный"])

    def show_mode_command_params(self, method_name: str) -> None:
        if method_name == "set_mode_rover":
            tk.Label(
                self.params_scrollable_frame,
                text=(
                    "MODE ROVER: сначала выберите сценарий, при необходимости — подрежим. "
                    "Пустой подрежим или DEFAULT — вариант по умолчанию для сценария."
                ),
                wraplength=520,
                justify=tk.LEFT,
                fg="gray",
                anchor=tk.W,
            ).pack(anchor=tk.W, padx=2, pady=(0, 8))
            self.create_param_widget(
                "rover_param1",
                "Сценарий",
                "",
                ["", "UAV", "SURVEY", "AUTOMOTIVE"],
                required=True,
            )
            self.create_param_widget(
                "rover_param2",
                "Подрежим",
                "",
                [""],
            )
            var1, w1 = self._param_vw("rover_param1")
            var2, w2 = self._param_vw("rover_param2")

            def _sync_rover_param2(*_: object) -> None:
                p1 = (var1.get() or "").strip().upper()
                cur2 = (var2.get() or "").strip().upper()
                if p1 == "":
                    w2.configure(values=("",), state="disabled")
                    var2.set("")
                elif p1 == "UAV":
                    w2.configure(values=("", "DEFAULT", "FORMATION"), state="readonly")
                    if cur2 not in ("", "DEFAULT", "FORMATION"):
                        var2.set("")
                elif p1 == "SURVEY":
                    w2.configure(values=("", "DEFAULT", "MOW"), state="readonly")
                    if cur2 not in ("", "DEFAULT", "MOW"):
                        var2.set("")
                elif p1 == "AUTOMOTIVE":
                    w2.configure(values=("", "DEFAULT"), state="readonly")
                    if cur2 not in ("", "DEFAULT"):
                        var2.set("")

            _sync_rover_param2()
            try:
                var1.trace_add("write", _sync_rover_param2)
            except AttributeError:
                var1.trace("w", _sync_rover_param2)
            w1.bind("<<ComboboxSelected>>", lambda _e: _sync_rover_param2())

        elif method_name == "set_mode_heading2":
            # ttk.Label(
            #     self.params_scrollable_frame,
            #     text=(
            #         "§3.7 MODE HEADING2: heading между двумя приёмниками / антеннами. "
            #         "Вариант задаёт динамику baseline (фикс. длина, статика, низкая динамика, техника и т.д.). "
            #         "Пустое значение — команда без второго слова (как в мануале)."
            #     ),
            #     wraplength=520,
            #     justify=tk.LEFT,
            #     foreground="gray",
            # ).pack(anchor=tk.W, padx=2, pady=(0, 8))
            self.create_param_widget(
                "heading2_variant",
                "Вариант heading",
                "",
                ["", "FIXLENGTH", "VARIABLELENGTH", "STATIC", "LOWDYNAMIC", "TRACTOR"],
            )

        elif method_name == "set_mode_base":
            # ttk.Label(
            #     self.params_scrollable_frame,
            #     text=(
            #         "Сначала выберите способ задания координат (§3.2–3.5): без фикс. точки, только ID, геодезия или ECEF. "
            #         "По мануалу команда MODE BASE без аргументов допустима; при GEODETIC нужны все три lat/lon/hgt, "
            #         "при ECEF — все три X/Y/Z (опционально с Station ID)."
            #     ),
            #     foreground="gray",
            #     wraplength=520,
            #     justify=tk.LEFT,
            # ).pack(pady=5, anchor=tk.W)
            self.create_param_widget(
                "base_coordinate_system",
                "СК / режим",
                "DEFAULT",
                ["DEFAULT", "GEODETIC", "ECEF"],
            )

            self.create_param_widget("station_id", "Station ID (0-4095)", "")

            ttk.Label(self.params_scrollable_frame,
                      text="Геодезические координаты (WGS84):",
                      font=("Arial", 11, "bold")).pack(pady=(10, 2), anchor=tk.W)
            self.create_param_widget("lat", "Широта (-90 до 90)", "")
            self.create_param_widget("lon", "Долгота (-180 до 180)", "")
            self.create_param_widget("hgt", "Высота (м)", "")

            ttk.Label(self.params_scrollable_frame,
                      text="ECEF (м):",
                      font=("Arial", 11, "bold")).pack(pady=(10, 2), anchor=tk.W)
            self.create_param_widget("x", "X (ECEF, м)", "")
            self.create_param_widget("y", "Y (ECEF, м)", "")
            self.create_param_widget("z", "Z (ECEF, м)", "")

            var_cs, w_cs = self._param_vw("base_coordinate_system")
            _, w_lat = self._param_vw("lat")
            _, w_lon = self._param_vw("lon")
            _, w_hgt = self._param_vw("hgt")
            _, w_x = self._param_vw("x")
            _, w_y = self._param_vw("y")
            _, w_z = self._param_vw("z")

            def _sync_mode_base_ui(*_: object) -> None:
                cs = (var_cs.get() or "DEFAULT").strip().upper()
                geodetic = cs == "GEODETIC"
                ecef = cs == "ECEF"
                self._set_param_required_star("lat", geodetic)
                self._set_param_required_star("lon", geodetic)
                self._set_param_required_star("hgt", geodetic)
                self._set_param_required_star("x", ecef)
                self._set_param_required_star("y", ecef)
                self._set_param_required_star("z", ecef)
                w_lat.configure(state="normal" if geodetic else "disabled")
                w_lon.configure(state="normal" if geodetic else "disabled")
                w_hgt.configure(state="normal" if geodetic else "disabled")
                w_x.configure(state="normal" if ecef else "disabled")
                w_y.configure(state="normal" if ecef else "disabled")
                w_z.configure(state="normal" if ecef else "disabled")

            _sync_mode_base_ui()
            try:
                var_cs.trace_add("write", _sync_mode_base_ui)
            except AttributeError:
                var_cs.trace("w", _sync_mode_base_ui)
            w_cs.bind("<<ComboboxSelected>>", lambda _e: _sync_mode_base_ui())
            w_cs.configure(state="readonly")

        elif method_name == "set_mode_base_time":
            ttk.Label(self.params_scrollable_frame,
                      #  text="Все параметры опциональны",
                      foreground="gray").pack(pady=5)
            self.create_param_widget("station_id", "Station ID (0-4095)", "")
            self.create_param_widget("time", "Время (сек)", "60", required=True)
            self.create_param_widget("distance", "Расстояние (м, 0-10)", "")

    def show_system_command_params(self, command: str) -> None:
        if command == "restore_output":
            ttk.Label(self.params_scrollable_frame,
                      text="Включить вывод BESTNAV (если устройство перестало отвечать после UNLOG)",
                      foreground="gray").pack(pady=2)
            self.create_param_widget("port", "Порт (пусто = текущий)", "", ["", "COM1", "COM2", "COM3"])
        elif command == "log":
            self._build_log_configure_ui()
        elif command == "unlog":
            ttk.Label(self.params_scrollable_frame, text="Оставьте пусто — остановить все на текущем порту",
                      foreground="gray").pack(pady=2)
            self.create_param_widget("port", "Порт", "", ["", "COM1", "COM2", "COM3"])
            self.create_param_widget("message", "Сообщение (напр. GPGGA)", "")
        elif command == "reset":
            ttk.Label(self.params_scrollable_frame,
                      text="Пусто = только перезапуск. Или через пробел: EPHEM ALMANAC IONUTC POSITION XOPARAM ALL",
                      foreground="gray").pack(pady=2)
            self.create_param_widget("parameters", "Параметры", "")
        elif command == "freset":
            ttk.Label(self.params_scrollable_frame, text="Параметры не требуются", foreground="gray").pack(pady=10)
        elif command == "saveconfig":
            ttk.Label(
                self.params_scrollable_frame,
                text=(
                    "Сохраняет текущую конфигурацию в NVM (в том числе настройки LOG по портам и частотам, "
                    "если вы их уже задали)."
                ),
                wraplength=540,
                justify=tk.LEFT,
                foreground="gray",
            ).pack(anchor=tk.W, padx=2, pady=10)

    def _log_norm_message(self, s: str) -> str:
        return (s or "").strip().upper()

    def _build_log_configure_ui(self) -> None:
        """Панель LOG: тип сообщения + матрица COM1–COM3 (как MSG в u-center), опрос UNILOGLIST."""
        outer = ttk.Frame(self.params_scrollable_frame)
        outer.pack(fill=tk.X, padx=2, pady=4)

        ttk.Label(
            outer,
            text=(
                "Настройка выдачи по портам приёмника COM1, COM2, COM3."
            ),
            wraplength=560,
            justify=tk.LEFT,
            foreground="gray",
        ).pack(anchor=tk.W, pady=(0, 8))

        # Единственный рабочий режим LOG: «одно сообщение».
        self._log_ui_mode = "single"
        self._log_active_memory_key = self._log_message_key()

        row_msg = ttk.Frame(outer)
        row_msg.pack(fill=tk.X, pady=2)
        ttk.Label(row_msg, text="Сообщение:", width=14).pack(side=tk.LEFT)
        if self._log_message_var is None:
            self._log_message_var = tk.StringVar(value="BESTNAVA")
        if not self._log_message_trace_installed:
            try:
                self._log_message_var.trace_add("write", lambda *_a: self._log_schedule_message_changed())
            except AttributeError:
                self._log_message_var.trace("w", lambda *_a: self._log_schedule_message_changed())
            self._log_message_trace_installed = True
        msg_combo = ttk.Combobox(
            row_msg,
            textvariable=self._log_message_var,
            values=_LOG_MESSAGE_TYPE_CHOICES,
            width=22,
        )
        msg_combo.pack(side=tk.LEFT, padx=4)
        self._log_msg_combo_widget = msg_combo
        msg_combo.bind("<<ComboboxSelected>>", lambda _e: self._log_on_message_changed())
        msg_combo.bind("<FocusOut>", lambda _e: self._log_on_message_changed())

        ttk.Button(row_msg, text="Обновить статус (UNILOGLIST)", command=self._log_refresh_unilog_async).pack(
            side=tk.LEFT, padx=(12, 0)
        )

        hdr = ttk.Frame(outer)
        hdr.pack(fill=tk.X, pady=(10, 2))
        ttk.Label(hdr, text="Порт", width=8).grid(row=0, column=0, sticky=tk.W)
        ttk.Label(hdr, text="Текущая выдача (по UNILOGLIST)", width=36).grid(row=0, column=1, sticky=tk.W)
        ttk.Label(hdr, text="Период (эпохи)", width=16).grid(row=0, column=2, sticky=tk.W)
        ttk.Label(hdr, text="Действие", width=18).grid(row=0, column=3, sticky=tk.W)

        self._log_rate_vars.clear()
        self._log_status_labels.clear()
        for port in ("COM1", "COM2", "COM3"):
            fr = ttk.Frame(outer)
            fr.pack(fill=tk.X, pady=2)
            ttk.Label(fr, text=port, width=8).grid(row=0, column=0, sticky=tk.W)
            st = ttk.Label(fr, text="— (запросите UNILOGLIST)", width=44, relief=tk.GROOVE)
            st.grid(row=0, column=1, sticky=tk.EW, padx=(0, 6))
            self._log_status_labels[port] = st
            rv = tk.StringVar(value="1")
            self._log_rate_vars[port] = rv
            sp = ttk.Entry(fr, textvariable=rv, width=10, justify=tk.RIGHT)
            sp.grid(row=0, column=2, sticky=tk.W, padx=(0, 8))
            self._log_bind_period_entry_trace(sp)
            ttk.Button(fr, text=f"LOG только {port}", command=lambda p=port: self._log_apply_one_port_async(p)).grid(
                row=0, column=3, sticky=tk.W
            )
            fr.columnconfigure(1, weight=1)

        self._log_load_rates_from_memory()
        self._log_update_message_row_state()
        self._log_update_port_status_labels()
        self._refresh_run_button_label()

    def _log_schedule_message_changed(self) -> None:
        if getattr(self, "_log_ui_mode", "single") != "single":
            return
        if self._log_msg_trace_after:
            try:
                self.root.after_cancel(self._log_msg_trace_after)
            except Exception:
                pass
            self._log_msg_trace_after = None
        self._log_msg_trace_after = self.root.after(450, self._log_on_message_changed)

    def _log_sync_mode_var_from_state(self) -> None:
        if self._log_mode_display_var is not None:
            self._log_mode_display_var.set(_LOG_MODE_DISPLAY_BY_ID.get(self._log_ui_mode, "Одно сообщение"))

    def _log_on_mode_ui_changed(self, initial: bool = False) -> None:
        disp = self._log_mode_display_var.get() if self._log_mode_display_var else "Одно сообщение"
        new_id = _LOG_MODE_ID_BY_DISPLAY.get(disp, "single")
        old_id = self._log_ui_mode
        if not initial:
            if new_id == old_id:
                self._log_update_message_row_state()
                self._refresh_run_button_label()
                return
            if old_id == "single":
                ak = self._log_active_memory_key
                if ak:
                    self._log_save_rates_to_memory_for_key(ak)
            else:
                self._log_save_bulk_rates_from_spinboxes()
        self._log_ui_mode = new_id
        if new_id == "single":
            self._log_load_rates_from_memory()
            self._log_active_memory_key = self._log_message_key()
        else:
            self._log_load_bulk_rates_into_spinboxes()
            self._log_active_memory_key = None
        self._log_update_message_row_state()
        self._log_update_port_status_labels()
        self._refresh_run_button_label()

    def _log_update_message_row_state(self) -> None:
        mc = self._log_msg_combo_widget
        if mc is None:
            return
        try:
            mc.configure(state=("normal" if self._log_ui_mode == "single" else "disabled"))
        except tk.TclError:
            pass

    def _log_bind_period_entry_trace(self, sp: tk.Widget) -> None:
        def _persist(_e: Optional[tk.Event] = None) -> None:
            if getattr(self, "_log_ui_mode", "single") == "single":
                k = self._log_active_memory_key or self._log_message_key()
                self._log_save_rates_to_memory_for_key(k)
            else:
                self._log_save_bulk_rates_from_spinboxes()

        sp.bind("<KeyRelease>", _persist, add="+")
        sp.bind("<FocusOut>", _persist, add="+")

    def _log_save_bulk_rates_from_spinboxes(self) -> None:
        self._log_save_rates_to_memory_for_key(_LOG_BULK_MEMORY_KEY)

    def _log_load_bulk_rates_into_spinboxes(self) -> None:
        mem = self._log_rate_memory.get(_LOG_BULK_MEMORY_KEY, {})
        for port in ("COM1", "COM2", "COM3"):
            rv = self._log_rate_vars.get(port)
            if rv is None:
                continue
            if port in mem:
                rv.set(format_log_period_display(float(mem[port])))
            else:
                rv.set("1")

    def _log_on_message_changed(self) -> None:
        self._log_msg_trace_after = None
        if getattr(self, "_log_ui_mode", "single") != "single":
            return
        old = self._log_active_memory_key
        cur = self._log_message_key()
        if old and old != cur:
            self._log_save_rates_to_memory_for_key(old)
        self._log_load_rates_from_memory()
        self._log_active_memory_key = cur
        self._log_update_port_status_labels()

    def _log_message_key(self) -> str:
        return self._log_norm_message(self._log_message_var.get() if self._log_message_var else "BESTNAVA")

    def _log_load_rates_from_memory(self) -> None:
        key = self._log_message_key()
        mem = self._log_rate_memory.get(key, {})
        for port in ("COM1", "COM2", "COM3"):
            rv = self._log_rate_vars.get(port)
            if rv is None:
                continue
            if port in mem:
                rv.set(format_log_period_display(float(mem[port])))
            else:
                rv.set("1")

    def _log_save_rates_to_memory_for_key(self, key: str) -> None:
        d: Dict[str, float] = {}
        for port in ("COM1", "COM2", "COM3"):
            rv = self._log_rate_vars.get(port)
            if rv is None:
                continue
            try:
                v = parse_log_period_str(str(rv.get()))
            except ValueError:
                v = 1.0
            d[port] = max(0.0, min(1e6, v))
        self._log_rate_memory[key] = d

    def _log_save_rates_to_memory(self) -> None:
        self._log_save_rates_to_memory_for_key(self._log_message_key())

    def _log_build_run_all_params(self) -> Dict[str, Any]:
        if self._log_message_var is None:
            return {"message": "BESTNAVA", "log_all_ports": True, "rates": {"COM1": 1.0, "COM2": 1.0, "COM3": 1.0}}
        self._log_save_rates_to_memory()
        key = self._log_message_key()
        rates = dict(self._log_rate_memory.get(key, {}))
        return {"message": key, "log_all_ports": True, "rates": rates}

    def _log_refresh_unilog_async(self) -> None:
        if not self.device:
            messagebox.showwarning("UNILOGLIST", "Сначала подключитесь к приёмнику.")
            return
        try:
            if not self.device.serial_conn or not self.device.serial_conn.is_open:
                messagebox.showwarning("UNILOGLIST", "Порт не открыт.")
                return
        except Exception:
            messagebox.showwarning("UNILOGLIST", "Порт не открыт.")
            return

        def worker() -> None:
            try:
                with self._serial_io_lock:
                    r = self.device.query_uniloglist()
                logs: Optional[List[Dict[str, Any]]] = None
                if isinstance(r, dict) and "error" not in r:
                    ul = r.get("uniloglist")
                    if isinstance(ul, dict):
                        raw_logs = ul.get("logs")
                        if isinstance(raw_logs, list):
                            logs = raw_logs
                self.root.after(0, lambda lg=logs: self._log_set_unilog_cache(lg))
            except Exception as e:
                self.root.after(0, lambda err=str(e): self.log_result(f"UNILOGLIST: {err}\n"))

        threading.Thread(target=worker, daemon=True).start()

    def _log_set_unilog_cache(self, logs: Optional[List[Dict[str, Any]]]) -> None:
        self._log_cached_unilog_logs = logs
        self._log_update_port_status_labels()
        if logs is not None:
            self.log_result(f"UNILOGLIST: получено записей логов: {len(logs)}\n")

    def _log_update_port_status_labels(self) -> None:
        logs = self._log_cached_unilog_logs
        for port in ("COM1", "COM2", "COM3"):
            lb = self._log_status_labels.get(port)
            if lb is None:
                continue
            if getattr(self, "_log_ui_mode", "single") == "all_types":
                if not isinstance(logs, list):
                    lb.config(text="— (нажмите «Обновить статус»)")
                else:
                    n = sum(
                        1
                        for entry in logs
                        if isinstance(entry, dict)
                        and (entry.get("port") or "").upper() == port
                        and self._log_norm_message(str(entry.get("message", ""))) in _LOG_MESSAGE_TYPE_CHOICES
                    )
                    lb.config(text=f"записей из списка типов на порту: {n}" if n else "нет типов из списка")
                continue
            sel = self._log_message_key()
            if not isinstance(logs, list):
                lb.config(text="— (нажмите «Обновить статус»)")
                continue
            found: Optional[Dict[str, Any]] = None
            for entry in logs:
                if not isinstance(entry, dict):
                    continue
                if (entry.get("port") or "").upper() != port:
                    continue
                if self._log_norm_message(str(entry.get("message", ""))) == sel:
                    found = entry
                    break
            if found is None:
                lb.config(text="не залогировано на этом порту")
                continue
            tr = found.get("trigger", "?")
            per = found.get("period")
            if per is not None:
                lb.config(text=f"{tr}, период {per} эпох")
            else:
                lb.config(text=str(tr))

    def _log_apply_one_port_async(self, port: str) -> None:
        if not self.device:
            messagebox.showwarning("LOG", "Сначала подключитесь к приёмнику.")
            return
        try:
            if not self.device.serial_conn or not self.device.serial_conn.is_open:
                messagebox.showwarning("LOG", "Порт не открыт.")
                return
        except Exception:
            messagebox.showwarning("LOG", "Порт не открыт.")
            return
        if getattr(self, "_log_ui_mode", "single") == "all_types":
            self._log_save_bulk_rates_from_spinboxes()
            rv = self._log_rate_vars.get(port)
            try:
                rate = parse_log_period_str(str(rv.get()).strip() if rv else "1")
            except ValueError:
                messagebox.showinfo("LOG", "Введите число ≥ 0 (период в эпохах, допускается дробь, например 0.5).")
                return
            if rate <= 0:
                messagebox.showinfo("LOG", "Задайте период > 0 для отправки LOG на этот порт.")
                return
            params = {
                "log_apply_all_on_port": True,
                "port": port,
                "rate": rate,
                "messages": list(_LOG_MESSAGE_TYPE_CHOICES),
            }
            self.log_result(
                f"\n{'─' * 50}\nLOG (все типы из списка) → {port}, период {rate}, "
                f"сообщений: {len(_LOG_MESSAGE_TYPE_CHOICES)}\n{'─' * 50}\n"
            )
        else:
            self._log_save_rates_to_memory()
            key = self._log_message_key()
            rv = self._log_rate_vars.get(port)
            try:
                rate = parse_log_period_str(str(rv.get()).strip() if rv else "1")
            except ValueError:
                messagebox.showinfo("LOG", "Введите число ≥ 0 (период в эпохах, допускается дробь, например 0.5).")
                return
            if rate <= 0:
                messagebox.showinfo("LOG", "Задайте период > 0 для отправки LOG на этот порт.")
                return
            params = {
                "log_apply_one": True,
                "message": key,
                "port": port,
                "rate": rate,
            }
            self.log_result(f"\n{'─' * 50}\nLOG → {port}, сообщение {key}, период {rate}\n{'─' * 50}\n")
        self._begin_log_stream_hint_transaction(params)
        self.run_btn.config(state=tk.DISABLED, text="Выполняется...")
        threading.Thread(
            target=self._oneshot_worker,
            args=("log", params),
            kwargs={"from_selection": False, "ui_context": None},
            daemon=True,
        ).start()
        self._schedule_poll()

    def _param_vw(self, name: str) -> Tuple[Any, tk.Widget]:
        t = self.param_widgets[name]
        return t[0], t[1]

    def _set_param_required_star(self, param_name: str, show: bool) -> None:
        """Показать/скрыть красную звёздочку «обязательное поле» у параметра с именем param_name."""
        tup = self.param_widgets.get(param_name)
        if not tup or len(tup) < 3:
            return
        meta = tup[2]
        if not isinstance(meta, dict):
            return
        star = meta.get("star")
        if star is None:
            return
        # Текст в фиксированной по ширине метке — колонка полей ввода не смещается (без pack_forget).
        if show:
            star.config(text="*", fg="red")
        else:
            star.config(text="", fg="red")

    def create_param_widget(
            self,
            param_name: str,
            label: str,
            default: str = "",
            choices: Optional[List[str]] = None,
            *,
            required: bool = False,
    ) -> None:
        frame = ttk.Frame(self.params_scrollable_frame)
        frame.pack(fill=tk.X, pady=2)

        lab_col = ttk.Frame(frame)
        lab_col.pack(side=tk.LEFT, padx=5)
        lw = 24
        ttk.Label(lab_col, text=f"{label}:", width=lw, anchor=tk.W).pack(side=tk.LEFT)
        # Резервируем место под «*», чтобы строки параметров выравнивались по колонке ввода.
        star_lbl = tk.Label(
            lab_col,
            text="*" if required else "",
            width=2,
            anchor=tk.W,
            fg="red",
            font=("Arial", 11, "bold"),
        )
        star_lbl.pack(side=tk.LEFT, padx=(2, 0))

        if choices:
            var = tk.StringVar(value=default)
            widget = ttk.Combobox(frame, textvariable=var, values=choices, width=30)
        else:
            var = tk.StringVar(value=default)
            widget = ttk.Entry(frame, textvariable=var, width=30)

        widget.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        self.param_widgets[param_name] = (var, widget, {"star": star_lbl})

    def get_params(self) -> Dict[str, Any]:
        params = {}
        for param_name, item in self.param_widgets.items():
            var, widget = item[0], item[1]
            value = var.get().strip()
            if value:
                if param_name == "binary":
                    params["binary"] = value == "Бинарный" or value.upper() == "TRUE"
                    continue
                try:
                    float_val = float(value)
                    if float_val.is_integer():
                        params[param_name] = int(float_val)
                    else:
                        params[param_name] = float_val
                except ValueError:
                    value_upper = value.upper()
                    if value_upper in ("TRUE", "1", "YES", "ON"):
                        params[param_name] = True
                    elif value_upper in ("FALSE", "0", "NO", "OFF", ""):
                        params[param_name] = False
                    else:
                        params[param_name] = value
        return params

    def run_command(self) -> None:
        if not self.device:
            messagebox.showerror("Ошибка", "Устройство не подключено")
            return

        try:
            if not self.device.serial_conn or not self.device.serial_conn.is_open:
                messagebox.showerror("Ошибка", "Устройство не подключено")
                return
        except AttributeError:
            messagebox.showerror("Ошибка", "Устройство не подключено")
            return

        if not self.current_command:
            messagebox.showerror("Ошибка", "Команда не выбрана")
            return

        try:
            if self._is_data_output_query_id(self.current_command):
                params = self._build_data_output_params()
            elif self.current_command == "log":
                params = self._log_build_run_all_params()
            else:
                params = self.get_params()
        except Exception as e:
            self.log_result(f"Ошибка параметров: {e}\n")
            return

        if self.current_command.startswith("query_") and not self._is_data_output_query_id(self.current_command):
            self._apply_log_stream_hint_to_query_params(self.current_command, params)
        if (
                self._is_data_output_query_id(self.current_command)
                and self._has_streaming_log_for_query(self.current_command, params)
        ):
            self.log_result(
                f"\n{'─' * 50}\n"
                "STREAM FROM LOG DETECTED -> PARSE ONLY (single query skipped)\n"
                f"{'─' * 50}\n"
            )
            self._sync_receiver_parse_mode()
            return

        self.log_result(f"\n{'=' * 70}\n")
        if self._is_data_output_query_id(self.current_command):
            self.log_result(f"Одиночный запрос данных: {self.current_command}\n")
        else:
            self.log_result(f"Выполнение: {self.current_command}\n")
        self.log_result(f"{'=' * 70}\n\n")
        if params:
            self.log_result(f"Параметры: {json.dumps(params, ensure_ascii=False, indent=2)}\n\n")

        if self.current_command == "log":
            self._begin_log_stream_hint_transaction(params)

        self.run_btn.config(state=tk.DISABLED, text="Выполняется...")
        try:
            thread = threading.Thread(
                target=self._oneshot_worker,
                args=(self.current_command, params),
                daemon=True,
            )
            thread.start()
            self._schedule_poll()
        except Exception as e:
            if self.current_command == "log":
                self._rollback_log_stream_hint_transaction()
            self.log_result(f"Ошибка запуска: {e}\n")
            self.run_btn.config(state=tk.NORMAL)
            self._refresh_run_button_label()

    def _oneshot_worker(
            self,
            command: str,
            params: dict,
            *,
            from_selection: bool = False,
            ui_context: Optional[str] = None,
    ) -> None:
        """Воркер одного запроса: выполняет команду и кладёт результат в очередь."""
        try:
            with self._serial_io_lock:
                if command.startswith("config_command:"):
                    cmd_type = command.split(":")[1]
                    if cmd_type == "STANDALONE":
                        result = self._execute_config_standalone(params)
                    elif cmd_type == "SMOOTH":
                        result = self._execute_config_smooth(params)
                    elif cmd_type == "MASK":
                        result = self._execute_config_mask(params)
                    elif cmd_type == "UNMASK":
                        result = self._execute_config_unmask(params)
                    else:
                        result = self.device.config_command(cmd_type, **params)
                elif command == "query_config":
                    suspended_bin = self._suspend_binary_log_streams_for_config()
                    try:
                        # Быстрый путь: бинарный шум уже снят, читаем сырой ответ без долгого readline-цикла.
                        result = self.device.query_config(use_lines=False, **params)
                    finally:
                        self._resume_binary_log_streams_after_config(suspended_bin)
                elif command.startswith("query_"):
                    method = getattr(self.device, command)
                    qp = dict(params) if params else {}
                    raw_once = bool(qp.pop("_data_output_raw_once", False))
                    skip_hint = bool(qp.pop("_skip_log_stream_hint", False))
                    # PVTSLN / OBSVBASE (ASCII/binary): только для пакета из _build_data_output_params (_skip_log_stream_hint).
                    # Для single-shot отправляем запрос и сразу ставим точечный UNLOG после первого кадра.
                    # Raw без периода: для всех raw_once (в т.ч. binary) — не требовать Tk/категорию в воркере.
                    if skip_hint and command == "query_pvtsln":
                        is_bin_pv = bool(qp.get("binary"))
                        ac = qp.get("add_crlf")
                        qkw: Dict[str, Any] = {"rate": 1, "binary": is_bin_pv}
                        if ac is not None:
                            qkw["add_crlf"] = ac
                        result = self.device.query_pvtsln(**qkw)
                        msg_ul = "PVTSLNB" if is_bin_pv else "PVTSLNA"
                        hint_pv = self._log_stream_output_hint.get(("query_pvtsln", is_bin_pv))
                        port_ul: Optional[str] = None
                        if isinstance(hint_pv, dict):
                            p0 = str(hint_pv.get("port") or "").strip().upper()
                            if p0 in ("COM1", "COM2", "COM3"):
                                port_ul = p0

                        def _deferred_unlog_pvtsln() -> None:
                            time.sleep(0.08)
                            ukw: Dict[str, Any] = {"message": msg_ul}
                            if port_ul:
                                ukw["port"] = port_ul
                            if ac is not None:
                                ukw["add_crlf"] = ac
                            with self._serial_io_lock:
                                self.device.unlog(**ukw)

                        threading.Thread(target=_deferred_unlog_pvtsln, daemon=True).start()
                    elif skip_hint and command == "query_obsvbase":
                        is_bin_ob = bool(qp.get("binary"))
                        ac = qp.get("add_crlf")
                        port_ob: Optional[str] = None
                        p0 = str(qp.get("port") or "").strip().upper()
                        if p0 in ("COM1", "COM2", "COM3"):
                            port_ob = p0
                        else:
                            hint_ob = self._log_stream_output_hint.get(("query_obsvbase", is_bin_ob))
                            if isinstance(hint_ob, dict):
                                p1 = str(hint_ob.get("port") or "").strip().upper()
                                if p1 in ("COM1", "COM2", "COM3"):
                                    port_ob = p1
                        cmd_ob = f"OBSVBASEB {port_ob} ONCHANGED" if is_bin_ob and port_ob else (
                            f"OBSVBASEB ONCHANGED" if is_bin_ob else (
                                f"OBSVBASEA {port_ob} ONCHANGED" if port_ob else "OBSVBASEA ONCHANGED"
                            )
                        )
                        ok_ob = bool(self.device.send_ascii_command(cmd_ob, add_crlf=ac))
                        if ok_ob:
                            result = {
                                "command": cmd_ob,
                                "success": True,
                                "message_sent_style": True,
                                "oneshot_query_sent": True,
                            }
                        else:
                            result = {"error": f"Не удалось отправить команду: {cmd_ob}"}
                        msg_ob = "OBSVBASEB" if is_bin_ob else "OBSVBASEA"

                        def _deferred_unlog_obsvbase() -> None:
                            # Даём потоку время принять хотя бы 1 полный многострочный кадр OBSVBASE.
                            time.sleep(0.45)
                            ukw: Dict[str, Any] = {"message": msg_ob}
                            if port_ob:
                                ukw["port"] = port_ob
                            if ac is not None:
                                ukw["add_crlf"] = ac
                            with self._serial_io_lock:
                                self.device.unlog(**ukw)

                        if ok_ob:
                            threading.Thread(target=_deferred_unlog_obsvbase, daemon=True).start()
                    elif raw_once:
                        raw_cmd = self._build_data_output_oneshot_command(command, qp)
                        if not raw_cmd:
                            result = {"error": f"Не удалось собрать one-shot команду для {command}"}
                        else:
                            ok = bool(self.device.send_ascii_command(raw_cmd))
                            if ok:
                                result = {
                                    "command": raw_cmd,
                                    "success": True,
                                    "message_sent_style": True,
                                    "oneshot_query_sent": True,
                                }
                            else:
                                result = {"error": f"Не удалось отправить команду: {raw_cmd}"}
                    elif not skip_hint:
                        self._apply_log_stream_hint_to_query_params(command, qp)
                        result = method(**qp)
                    else:
                        result = method(**qp)
                elif command.startswith("set_mode_"):
                    method = getattr(self.device, command)
                    if command == "set_mode_rover":
                        p = {k: v for k, v in params.items() if k in ("rover_param1", "rover_param2")}
                        result = method(**p)
                    elif command == "set_mode_heading2":
                        p = {k: v for k, v in params.items() if k in ("heading2_variant",)}
                        result = method(**p)
                    elif command == "set_mode_base":
                        cs = str(params.get("base_coordinate_system") or "DEFAULT").strip().upper()
                        if cs not in ("DEFAULT", "GEODETIC", "ECEF"):
                            cs = "DEFAULT"
                        has_geo = any(params.get(k) is not None for k in ("lat", "lon", "hgt"))
                        has_ecef = any(params.get(k) is not None for k in ("x", "y", "z"))
                        if cs == "DEFAULT":
                            if has_geo or has_ecef:
                                result = {
                                    "error": (
                                        "Режим DEFAULT: не заполняйте lat/lon/hgt и X/Y/Z "
                                        "(или выберите GEODETIC / ECEF)."
                                    ),
                                }
                            else:
                                sid = params.get("station_id")
                                result = method(station_id=sid) if sid is not None else method()
                        elif cs == "GEODETIC":
                            missing = [k for k in ("lat", "lon", "hgt") if params.get(k) is None]
                            if missing:
                                result = {
                                    "error": (
                                        "Для GEODETIC нужны все три: lat, lon, hgt. "
                                        f"Не задано: {', '.join(missing)}."
                                    ),
                                }
                            elif has_ecef:
                                result = {
                                    "error": "Для GEODETIC не указывайте X/Y/Z (или смените режим на ECEF).",
                                }
                            else:
                                p = {k: params[k] for k in ("lat", "lon", "hgt")}
                                sid = params.get("station_id")
                                if sid is not None:
                                    p["station_id"] = sid
                                result = method(**p)
                        else:
                            missing = [k for k in ("x", "y", "z") if params.get(k) is None]
                            if missing:
                                result = {
                                    "error": (
                                        "Для ECEF нужны все три: X, Y, Z. "
                                        f"Не задано: {', '.join(missing)}."
                                    ),
                                }
                            elif has_geo:
                                result = {
                                    "error": "Для ECEF не указывайте lat/lon/hgt (или смените режим на GEODETIC).",
                                }
                            else:
                                p = {k: params[k] for k in ("x", "y", "z")}
                                sid = params.get("station_id")
                                if sid is not None:
                                    p["station_id"] = sid
                                result = method(**p)
                    elif command == "set_mode_base_time":
                        p = {k: v for k, v in params.items() if v}
                        result = method(**p) if p else method()
                    else:
                        result = method(**params)
                elif command in ("restore_output", "log", "unlog", "freset", "reset", "saveconfig"):
                    if command == "restore_output":
                        p = params.get("port", "").strip() or None
                        result = self.device.restore_output(port=p)
                    elif command == "log":
                        if params.get("log_apply_one"):
                            r1 = _coerce_log_rate_param(params.get("rate"), default=1.0)
                            result = self.device.log(
                                message=str(params.get("message") or "BESTNAVA").strip(),
                                port=str(params["port"]),
                                rate=r1,
                            )
                            if not result.get("error"):
                                self._stream_log_register_hint(
                                    str(params.get("message") or ""),
                                    str(params.get("port") or ""),
                                    r1,
                                )
                        elif params.get("log_apply_all_on_port"):
                            port_u = str(params["port"]).strip().upper()
                            rate_ap = _coerce_log_rate_param(params.get("rate"), default=1.0)
                            msgs = list(params.get("messages") or _LOG_MESSAGE_TYPE_CHOICES)
                            lines_ap: List[str] = []
                            last_ap: Dict[str, Any] = {}
                            err_ap = False
                            for idx, m in enumerate(msgs):
                                m_u = str(m).strip().upper()
                                last_ap = self.device.log(message=m_u, port=port_u, rate=rate_ap)
                                if idx % 4 == 3:
                                    time.sleep(0.04)
                                if last_ap.get("error"):
                                    result = {
                                        "error": f"{port_u} {m_u}: {last_ap['error']}",
                                        "log_multi_lines": lines_ap,
                                    }
                                    err_ap = True
                                    break
                                if not last_ap.get("error"):
                                    self._stream_log_register_hint(m_u, port_u, rate_ap)
                            if not err_ap:
                                result = {
                                    "command": f"{len(msgs)} LOG на {port_u}, период {rate_ap}",
                                    "success": True,
                                    "log_multi_lines": [
                                        f"Успешно: {len(msgs)} команд на {port_u} (период {rate_ap} эпох).",
                                    ],
                                    "last": last_ap,
                                }
                        elif params.get("log_all_listed"):
                            msgs_l = [str(x).strip().upper() for x in
                                      (params.get("messages") or _LOG_MESSAGE_TYPE_CHOICES)]
                            rates_l = params.get("rates") or {}
                            lines_l: List[str] = []
                            last_l: Dict[str, Any] = {}
                            err_l = False
                            sent = 0
                            for port_l in ("COM1", "COM2", "COM3"):
                                r_l = _coerce_log_rate_param(rates_l.get(port_l, 1), default=1.0)
                                if r_l <= 0:
                                    lines_l.append(f"{port_l}: пропуск (период ≤ 0)")
                                    continue
                                for idx, m in enumerate(msgs_l):
                                    last_l = self.device.log(message=m, port=port_l, rate=r_l)
                                    sent += 1
                                    if sent % 4 == 0:
                                        time.sleep(0.04)
                                    if last_l.get("error"):
                                        result = {
                                            "error": f"{port_l} {m}: {last_l['error']}",
                                            "log_multi_lines": lines_l,
                                        }
                                        err_l = True
                                        break
                                    if not last_l.get("error"):
                                        self._stream_log_register_hint(m, port_l, r_l)
                                if err_l:
                                    break
                            if not err_l:
                                if sent == 0:
                                    result = {
                                        "error": "Ни одной команды: для всех COM период ≤ 0.",
                                    }
                                else:
                                    result = {
                                        "command": f"LOG всех типов ({len(msgs_l)}) на COM с периодами из rates",
                                        "success": True,
                                        "log_multi_lines": lines_l
                                                           + [f"Готово: отправлено {sent} команд LOG."],
                                        "last": last_l,
                                    }
                        elif params.get("log_all_ports"):
                            msg = str(params.get("message") or "BESTNAVA").strip()
                            rates = params.get("rates") or {}
                            lines: List[str] = []
                            last: Dict[str, Any] = {}
                            err_hit = False
                            for port in ("COM1", "COM2", "COM3"):
                                r = _coerce_log_rate_param(rates.get(port, 1), default=1.0)
                                if r <= 0:
                                    lines.append(f"{port}: не отправляем (период ≤ 0)")
                                    continue
                                last = self.device.log(message=msg, port=port, rate=r)
                                if last.get("error"):
                                    result = {
                                        "error": f"{port}: {last['error']}",
                                        "log_multi_lines": lines,
                                    }
                                    err_hit = True
                                    break
                                if not last.get("error"):
                                    self._stream_log_register_hint(msg, port, r)
                                ok = bool(last.get("confirmation") or last.get("success"))
                                cmd_s = last.get("command", "?")
                                lines.append(f"{port}: «{cmd_s}» ({'подтверждено' if ok else 'ответ без OK'})")
                            if not err_hit:
                                if not lines:
                                    result = {
                                        "error": "Ни на один порт не отправлено: для всех COM период ≤ 0.",
                                    }
                                else:
                                    result = {
                                        "command": f"{msg} на COM1–COM3 (период по портам)",
                                        "success": True,
                                        "log_multi_lines": lines,
                                        "last": last,
                                    }
                        else:
                            msg = (params.get("message") or "BESTNAVA").strip()
                            pr = params.get("port")
                            p = str(pr).strip().upper() if pr is not None and str(pr).strip() else None
                            rate = _coerce_log_rate_param(params.get("rate"), default=1.0)
                            result = self.device.log(message=msg, port=p, rate=rate)
                            if not result.get("error"):
                                self._stream_log_register_hint(msg, p, rate)
                    elif command == "unlog":
                        p = {k: v for k, v in params.items() if k in ("port", "message") and v}
                        result = self.device.unlog(**p)
                        if not result.get("error"):
                            self._stream_log_unregister_hint(
                                message=str(p.get("message") or "").strip() or None,
                                port=str(p.get("port") or "").strip().upper() or None,
                            )
                    elif command == "freset":
                        result = self.device.freset()
                    elif command == "reset":
                        par = params.get("parameters", "").strip()
                        if par:
                            result = self.device.reset(parameters=[s.strip() for s in par.split() if s.strip()])
                        else:
                            result = self.device.reset()
                    elif command == "saveconfig":
                        result = self.device.saveconfig()
                else:
                    result = {"error": "Неизвестная команда"}
                self._result_queue.put(
                    ("result", command, result, False, from_selection, ui_context)
                )
        except Exception as e:
            self._result_queue.put(("error", str(e), command))
        self._result_queue.put(("done",))

    def _schedule_poll(self) -> None:
        """Запуск периодической проверки очереди результатов."""
        if self._poll_after_id:
            self.root.after_cancel(self._poll_after_id)
        self._poll_queue()

    def _poll_queue(self) -> None:
        """Обработка очереди результатов (вызывается по таймеру из главного потока)."""
        batch: List[Any] = []
        try:
            while True:
                batch.append(self._result_queue.get_nowait())
        except queue.Empty:
            pass

        pending_rx: Dict[str, Dict[str, Any]] = {}
        for item in batch:
            if item[0] == "rx_update" and isinstance(item[2], dict):
                pending_rx[item[1]] = item[2]

        for item in batch:
            if item[0] == "rx_update":
                continue
            if item[0] == "result":
                _, command, result = item[0], item[1], item[2]
                from_selection = item[4] if len(item) > 4 else False
                if "error" in result:
                    self.log_result(f"Ошибка: {result['error']}\n")
                    if command == "log":
                        self._rollback_log_stream_hint_transaction()
                else:
                    if command.startswith("config_command:"):
                        formatted = self.format_config_result(result)
                    elif command.startswith("set_mode_"):
                        formatted = self.format_set_mode_result(result, command)
                    elif command.startswith("query_") and result.get("message_sent_style"):
                        formatted = self._format_system_result(result)
                    elif command in ("restore_output", "log", "unlog", "freset", "reset", "saveconfig"):
                        formatted = self._format_system_result(result)
                    else:
                        formatted = self.format_query_result(command, result)
                    self.log_result(formatted)
                    if command.startswith("query_") and "error" not in result:
                        self._apply_query_data_to_panel(command, result)
                    elif command.startswith("config_command:") and "response" in result and "parsed" in result.get(
                            "response", {}):
                        parsed_data = result["response"]["parsed"]
                        if parsed_data:
                            self.populate_data_table(parsed_data, source_command=command)
                    show_notification = (
                            not from_selection
                            and (result.get("sent_no_response") or result.get("message_sent_style"))
                    )
                    if (
                            not show_notification
                            and not from_selection
                            and "error" not in result
                    ):
                        if command in ("set_mode_rover", "set_mode_heading2"):
                            show_notification = True
                    if show_notification:
                        msg = self._notification_message_for_command(command, result)
                        messagebox.showinfo("Уведомление", msg)
                    if command == "log":
                        self._commit_log_stream_hint_transaction()
                self.run_btn.config(state=tk.NORMAL)
                self._refresh_run_button_label()
            elif item[0] == "error":
                exc_cmd = item[2] if len(item) > 2 else None
                self.log_result(f"Исключение: {item[1]}\n")
                if exc_cmd == "log":
                    self._rollback_log_stream_hint_transaction()
                self.run_btn.config(state=tk.NORMAL)
                self._refresh_run_button_label()
            elif item[0] == "done":
                self.run_btn.config(state=tk.NORMAL)
                self._refresh_run_button_label()
            elif item[0] == "profile_export_done":
                _, err, path = item
                self._profile_export_done(err, path)
            elif item[0] == "profile_compare_done":
                _, err, path, diff = item
                self._profile_compare_done(err, path, diff)
            elif item[0] == "profile_apply_done":
                _, err, apply_res = item
                self._profile_apply_done(err, apply_res)

        for command, payload in pending_rx.items():
            self._apply_rx_stream_to_data_panel(command, payload)

        self._poll_after_id = self.root.after(80, self._poll_queue)

    def format_query_result(self, command: str, result: dict) -> str:
        """Форматирование результата query команды"""
        output = []

        data_key = QUERY_COMMAND_DATA_KEY.get(command)
        if not data_key or data_key not in result:
            return "Данные не получены\n"

        data = result[data_key]
        if not data:
            return (
                "Снимок по запросу не распарсился (ответ пришёл, но разбор вернул пусто).\n"
                "Попробуйте другой режим чекбокса «Бинарный», скорость порта и выдачу LOG на этот COM. "
                "Поля «Текущее сообщение» при этом могут обновляться из фонового потока, если кадры "
                "уже идут с приёмника.\n"
            )

        # Специальное форматирование для разных команд
        if command == "query_mode":
            return self._format_mode_data(data)
        elif command == "query_version":
            return self._format_version_data(data)
        elif command == "query_mask":
            return self._format_mask_data(data)
        elif command == "query_baseinfo":
            return self._format_baseinfo_data(data)
        elif command == "query_gpsion":
            return self._format_gpsion_data(data)
        elif command == "query_bdsion":
            return self._format_bdsion_data(data)
        elif command == "query_galion":
            return self._format_galion_data(data)
        elif command == "query_bd3ion":
            return self._format_bd3ion_data(data)
        elif command == "query_uniloglist":
            return self._format_uniloglist_data(data)
        elif command == "query_bestnav":
            return self._format_bestnav_data(data)
        elif command == "query_adrnav":
            return self._format_generic_data("ADRNAV", data)
        elif command == "query_adrnavh":
            return self._format_generic_data("ADRNAVH", data)
        elif command == "query_pppnav":
            return self._format_generic_data("PPPNAV", data)
        elif command == "query_sppnav":
            return self._format_generic_data("SPPNAV", data)
        elif command == "query_sppnavh":
            return self._format_generic_data("SPPNAVH", data)
        elif command == "query_stadop":
            return self._format_generic_data("STADOP", data)
        elif command == "query_adrdop":
            return self._format_generic_data("ADRDOP", data)
        elif command == "query_adrdoph":
            return self._format_generic_data("ADRDOPH", data)
        elif command == "query_bestnavxyz":
            return self._format_bestnavxyz_data(data)
        elif command == "query_agric":
            return self._format_agric_data(data)
        elif command == "query_pvtsln":
            return self._format_pvtsln_data(data)
        elif command == "query_obsvm":
            return self._format_obsvm_data(data, "OBSVM", "главной антенны")
        elif command == "query_obsvh":
            return self._format_obsvm_data(data, "OBSVH", "ведомой антенны")
        elif command == "query_obsvbase":
            return self._format_obsvm_data(data, "OBSVBASE", "базовой станции")
        elif command == "query_obsvmcmp":
            return self._format_obsvmcmp_data(data)
        elif command == "query_config":
            return self._format_config_data(data)
        elif command == "query_hwstatus":
            return self._format_hwstatus_data(data)
        elif command == "query_agc":
            return self._format_agc_data(data)
        else:
            # Общее форматирование для остальных команд
            return self._format_generic_data(data_key.upper(), data)

    def _format_mode_data(self, data: dict) -> str:
        """Форматирование данных MODE"""
        output = ["Информация MODE:"]
        output.append(f"  Формат: {data.get('format', 'unknown')}")
        output.append(f"  Режим: {data.get('mode', 'N/A')}")

        mode_subtype = data.get('mode_subtype')
        if mode_subtype:
            mode_subtype_clean = mode_subtype.rstrip(', ').strip()
            if mode_subtype_clean:
                output.append(f"  Подтип: {mode_subtype_clean}")

        heading_mode = data.get('heading_mode')
        if heading_mode:
            output.append(f"  Heading Mode: {heading_mode}")

        mode_string = data.get('mode_string', '')
        if mode_string:
            mode_string_clean = mode_string.rstrip(', ').strip()
            if mode_string_clean:
                output.append(f"  Строка режима: {mode_string_clean}")

        return "\n".join(output) + "\n"

    def _format_version_data(self, data: dict) -> str:
        """Форматирование данных VERSION"""
        output = ["Информация о версии:"]
        if 'format' in data:
            output.append(f"  Формат: {data['format']}")
        if 'product_name' in data:
            output.append(f"  Продукт: {data['product_name']}")
        if 'product_type' in data:
            output.append(f"  Тип продукта: {data['product_type']}")
        if 'sw_version' in data:
            output.append(f"  Версия ПО: {data['sw_version']}")
        if 'psn' in data:
            output.append(f"  PN/SN: {data['psn']}")
        if 'auth' in data and data['auth'] != '-':
            output.append(f"  Авторизация: {data['auth']}")
        if 'efuse_id' in data:
            output.append(f"  Board ID: {data['efuse_id']}")
        if 'comp_time' in data:
            output.append(f"  Дата компиляции: {data['comp_time']}")
        return "\n".join(output) + "\n"

    def _format_mask_data(self, data: dict) -> str:
        """Форматирование данных MASK: отдельный блок на каждую маску."""
        blocks = _mask_blocks(data)
        entries = blocks["entries"]

        if not entries:
            output = [
                "MASK (конфигурация масок):",
                "  Наложенных масок нет (приёмник не маскирует по углу/системе/PRN).",
                "",
                "Если нужны маски — задайте их командами CONFIG MASK / UNMASK.",
            ]
            if data.get("note"):
                output.append(f"  {data['note']}")
            if data.get("raw_preview"):
                output.append("  Начало ответа:")
                output.append(f"    {data['raw_preview']}")
            return "\n".join(output) + "\n"

        output = ["MASK (конфигурация масок):", f"  Масок: {len(entries)}", ""]
        for i, ent in enumerate(entries, 1):
            output.append(f"  Маска {i}:")
            for k, v in _mask_entry_params(ent):
                if v:
                    output.append(f"    {k}: {v}")
            output.append("")
        return "\n".join(output) + "\n"

    def _format_baseinfo_data(self, data: dict) -> str:
        """Форматирование данных BASEINFO"""
        output = ["Информация о базовой станции:"]
        if 'status' in data:
            output.append(f"  Статус: {data['status']}")
        if 'x' in data and 'y' in data and 'z' in data:
            output.append(f"  Координаты ECEF: X={data['x']:.3f}, Y={data['y']:.3f}, Z={data['z']:.3f}")
        if 'station_id' in data:
            output.append(f"  Station ID: {data['station_id']}")
        if 'format' in data:
            output.append(f"  Формат: {data['format']}")
        return "\n".join(output) + "\n"

    def _format_gpsion_data(self, data: dict) -> str:
        """Форматирование данных GPSION"""
        output = ["Параметры ионосферы GPS:"]
        if 'format' in data:
            output.append(f"  Формат: {data['format']}")

        if 'alpha' in data:
            alpha = data['alpha']
            output.append("  Alpha параметры:")
            if 'a0' in alpha:
                output.append(f"    a0: {alpha['a0']:.15e}")
            if 'a1' in alpha:
                output.append(f"    a1: {alpha['a1']:.15e}")
            if 'a2' in alpha:
                output.append(f"    a2: {alpha['a2']:.15e}")
            if 'a3' in alpha:
                output.append(f"    a3: {alpha['a3']:.15e}")

        if 'beta' in data:
            beta = data['beta']
            output.append("  Beta параметры:")
            if 'b0' in beta:
                output.append(f"    b0: {beta['b0']:.15e}")
            if 'b1' in beta:
                output.append(f"    b1: {beta['b1']:.15e}")
            if 'b2' in beta:
                output.append(f"    b2: {beta['b2']:.15e}")
            if 'b3' in beta:
                output.append(f"    b3: {beta['b3']:.15e}")

        if 'us_svid' in data:
            output.append(f"  SVID: {data['us_svid']}")
        if 'us_week' in data:
            output.append(f"  Week: {data['us_week']}")
        if 'ul_sec' in data:
            output.append(f"  Second: {data['ul_sec']} мс")
        if 'reserved' in data:
            output.append(f"  Reserved: {data['reserved']}")

        return "\n".join(output) + "\n"

    def _format_bdsion_data(self, data: dict) -> str:
        """Форматирование данных BDSION (структура как в спецификации)."""
        output = ["Параметры ионосферы BDS:"]
        if "format" in data:
            output.append(f"  Формат: {data['format']}")

        alpha = data.get("alpha") or {}
        beta = data.get("beta") or {}

        if alpha:
            output.append("  Alpha коэффициенты (a0..a3):")
            for name in ("a0", "a1", "a2", "a3"):
                if name in alpha:
                    output.append(f"    {name}: {alpha[name]:.15e}")

        if beta:
            output.append("  Beta коэффициенты (b0..b3):")
            for name in ("b0", "b1", "b2", "b3"):
                if name in beta:
                    output.append(f"    {name}: {beta[name]:.15e}")

        if "us_svid" in data:
            output.append(f"  usSVID (спутник для расчёта): {data['us_svid']}")
        if "us_week" in data:
            output.append(f"  usWeek (GPS неделя): {data['us_week']}")
        if "ul_sec" in data:
            output.append(f"  ulSec (GPS время, мс): {data['ul_sec']}")
        if "reserved" in data:
            output.append(f"  reserved: {data['reserved']}")
        if "crc" in data and data["crc"] is not None:
            output.append(f"  CRC: {data['crc']}")

        return "\n".join(output) + "\n"

    def _format_galion_data(self, data: dict) -> str:
        """Форматирование данных GALION."""
        output = ["Параметры ионосферы Galileo:"]
        if "format" in data:
            output.append(f"  Формат: {data['format']}")
        alpha = data.get("alpha") or {}
        if alpha:
            output.append("  Alpha:")
            for name in ("a0", "a1", "a2"):
                if name in alpha:
                    output.append(f"    {name}: {alpha[name]:.15e}")
        sf = data.get("sf") or {}
        if sf:
            output.append("  SF:")
            for i in range(1, 6):
                k = f"sf{i}"
                if k in sf:
                    output.append(f"    {k}: {sf[k]}")
        if "reserved" in data:
            output.append(f"  Reserved: {data['reserved']}")
        return "\n".join(output) + "\n"

    def _format_bd3ion_data(self, data: dict) -> str:
        """Форматирование данных BD3ION."""
        output = ["Параметры ионосферы BDS-3:"]
        if "format" in data:
            output.append(f"  Формат: {data['format']}")
        a = data.get("a") or {}
        if a:
            output.append("  Коэффициенты ai:")
            for i in range(1, 10):
                ak = f"a{i}"
                if ak in a:
                    output.append(f"    {ak}: {a[ak]:.15e}")
        if "reserved" in data:
            output.append(f"  Reserved: {data['reserved']}")
        return "\n".join(output) + "\n"

    def _format_uniloglist_data(self, data: dict) -> str:
        """Форматирование данных UNILOGLIST"""
        output = ["Список активных логов:"]
        if 'count' in data:
            output.append(f"  Количество: {data['count']}")
        if 'logs' in data and isinstance(data['logs'], list):
            for i, log in enumerate(data['logs'], 1):
                port = log.get('port', 'N/A')
                message = log.get('message', 'N/A')
                trigger = log.get('trigger', 'N/A')
                period = log.get('period')
                period_str = f" {period}" if period else ""
                output.append(f"  {i}. {message} ({port}) - {trigger}{period_str}")
        return "\n".join(output) + "\n"

    def _format_bestnav_data(self, data: dict) -> str:
        """Форматирование данных BESTNAV"""
        output = ["Лучшая позиция и скорость (BESTNAV):"]
        if 'format' in data:
            output.append(f"  Формат: {data['format']}")
        if 'position' in data:
            pos = data['position']
            if 'sol_status' in pos:
                output.append(f"  Статус решения (p-sol): {pos['sol_status']}")
            if 'pos_type' in pos:
                output.append(f"  Тип позиции: {pos['pos_type']}")
            if 'lat' in pos and 'lon' in pos and 'hgt' in pos:
                output.append(f"  Координаты: Lat={pos['lat']:.9f}, Lon={pos['lon']:.9f}, Hgt={pos['hgt']:.3f}")
            if 'undulation' in pos:
                output.append(f"  Undulation: {pos['undulation']:.4f} м")
            if 'datum_id' in pos:
                output.append(f"  Datum: {pos['datum_id']}")
            if 'lat_std' in pos and 'lon_std' in pos and 'hgt_std' in pos:
                output.append(
                    f"  σ (lat, lon, hgt): {pos['lat_std']:.4f}, {pos['lon_std']:.4f}, {pos['hgt_std']:.4f} м"
                )
            if 'stn_id' in pos:
                output.append(f"  Station ID: {pos['stn_id']}")
            if 'diff_age' in pos and 'sol_age' in pos:
                output.append(f"  diff_age / sol_age: {pos['diff_age']:.3f} / {pos['sol_age']:.3f} с")
            if 'num_svs' in pos and 'num_soln_svs' in pos:
                output.append(f"  Спутников / в решении: {pos['num_svs']} / {pos['num_soln_svs']}")
        if 'extended' in data:
            ext = data['extended']
            if 'ext_sol_stat_hex' in ext:
                output.append(f"  Ext sol stat: {ext['ext_sol_stat_hex']}")
            if ext.get('ext_sol_rtk_verification'):
                output.append(f"    RTK verification: {ext['ext_sol_rtk_verification']}")
            if 'ionospheric_correction_type' in ext:
                output.append(f"    Ionospheric correction type (bits 1–3): {ext['ionospheric_correction_type']}")
            if 'gal_bds3_signals_text' in ext:
                output.append(
                    f"  Galileo & BDS-3 mask: {ext.get('gal_bds3_mask_hex', '')} → {ext['gal_bds3_signals_text']}")
            if 'gps_glo_bds2_signals_text' in ext:
                output.append(
                    f"  GPS / GLONASS / BDS-2 mask: {ext.get('gps_glo_bds2_mask_hex', '')} → {ext['gps_glo_bds2_signals_text']}")
        if 'velocity' in data:
            vel = data['velocity']
            if 'sol_status' in vel:
                output.append(f"  Статус скорости (v-sol): {vel['sol_status']}")
            if 'vel_type' in vel:
                output.append(f"  Тип скорости: {vel['vel_type']}")
            if 'latency' in vel and 'age' in vel:
                output.append(f"  Latency / age: {vel['latency']:.6f} / {vel['age']:.6f} с")
            if 'hor_spd' in vel and 'trk_gnd' in vel and 'vert_spd' in vel:
                output.append(
                    f"  Скорость: hor={vel['hor_spd']:.6f} м/с, track={vel['trk_gnd']:.3f}°, vert={vel['vert_spd']:.6f} м/с"
                )
            if 'versp_std' in vel and 'horspd_std' in vel:
                output.append(f"  σ скорости (vert / hor): {vel['versp_std']:.6f} / {vel['horspd_std']:.6f}")
        return "\n".join(output) + "\n"

    def _format_bestnavxyz_data(self, data: dict) -> str:
        """Форматирование BESTNAVXYZ (ECEF), §7.3.25."""
        output = ["Лучшая позиция и скорость в ECEF (BESTNAVXYZ):"]
        if "format" in data:
            output.append(f"  Формат: {data['format']}")
        if "position" in data:
            pos = data["position"]
            if "P_sol_status" in pos:
                output.append(f"  Статус решения (p-sol): {pos['P_sol_status']}")
            if "pos_type" in pos:
                output.append(f"  Тип позиции: {pos['pos_type']}")
            if "P_X" in pos and "P_Y" in pos and "P_Z" in pos:
                output.append(
                    f"  Позиция ECEF: X={pos['P_X']:.4f}, Y={pos['P_Y']:.4f}, Z={pos['P_Z']:.4f} м"
                )
            if "P_X_sigma" in pos:
                output.append(
                    f"  σ (X,Y,Z): {pos.get('P_X_sigma', 0):.4f}, {pos.get('P_Y_sigma', 0):.4f}, {pos.get('P_Z_sigma', 0):.4f} м"
                )
        if "velocity" in data:
            vel = data["velocity"]
            if "V_sol_status" in vel:
                output.append(f"  Статус скорости (v-sol): {vel['V_sol_status']}")
            if "vel_type" in vel:
                output.append(f"  Тип скорости: {vel['vel_type']}")
            if "V_X" in vel and "V_Y" in vel and "V_Z" in vel:
                output.append(
                    f"  Скорость ECEF: Vx={vel['V_X']:.6f}, Vy={vel['V_Y']:.6f}, Vz={vel['V_Z']:.6f} м/с"
                )
            if "V_X_sigma" in vel:
                output.append(
                    f"  σ скорости (X,Y,Z): {vel.get('V_X_sigma', 0):.4f}, {vel.get('V_Y_sigma', 0):.4f}, {vel.get('V_Z_sigma', 0):.4f}"
                )
        if "metadata" in data:
            meta = data["metadata"]
            if "station_id" in meta:
                sid = meta["station_id"]
                output.append(f"  Station ID: {sid if sid else '(пусто)'}")
            if "V_latency" in meta and "diff_age" in meta and "sol_age" in meta:
                output.append(
                    f"  v-latency / diff_age / sol_age: {meta['V_latency']:.6f} / {meta['diff_age']:.3f} / {meta['sol_age']:.3f} с"
                )
            nt = meta.get("num_sats_tracked")
            nu = meta.get("num_sats_used")
            if nt is not None and nu is not None:
                output.append(f"  Спутников отслеживаемых / в решении: {nt} / {nu}")
            if meta.get("num_gg_l1") is not None or meta.get("num_soln_multi_svs") is not None:
                output.append(
                    f"  #ggL1 / #solnMultiSVs: {meta.get('num_gg_l1', '—')} / {meta.get('num_soln_multi_svs', '—')}"
                )
            if "reserved" in meta and meta["reserved"] is not None:
                output.append(f"  Reserved (byte): {meta['reserved']}")
        if "extended" in data:
            ext = data["extended"]
            if "ext_sol_stat_hex" in ext:
                output.append(f"  Ext sol stat: {ext['ext_sol_stat_hex']}")
            if ext.get("ext_sol_rtk_verification"):
                output.append(f"    RTK verification: {ext['ext_sol_rtk_verification']}")
            if "ionospheric_correction_type" in ext:
                output.append(f"    Ionospheric correction type: {ext['ionospheric_correction_type']}")
            if "gal_bds3_signals_text" in ext:
                output.append(
                    f"  Galileo & BDS-3 mask: {ext.get('gal_bds3_mask_hex', '')} → {ext['gal_bds3_signals_text']}")
            if "gps_glo_bds2_signals_text" in ext:
                output.append(
                    f"  GPS / GLONASS / BDS-2 mask: {ext.get('gps_glo_bds2_mask_hex', '')} → {ext['gps_glo_bds2_signals_text']}")
        if data.get("inner_crc"):
            output.append(f"  Внутренний CRC (binary): {data['inner_crc']}")
        return "\n".join(output) + "\n"

    def _format_hwstatus_data(self, data: dict) -> str:
        """Форматирование данных HWSTATUS"""
        output = ["Статус оборудования (HWSTATUS):"]

        if 'format' in data:
            output.append(f"  Формат: {data['format']}")

        # Температура
        if 'temp1' in data:
            temp1 = data['temp1']
            temp_celsius = data.get('temp1_celsius', temp1 / 1000.0)
            output.append(f"  Температура чипа: {temp_celsius:.3f}°C ({temp1} × 1000)")

        # Напряжения
        if 'dc09' in data:
            dc09 = data['dc09']
            output.append(f"  DC09: {dc09:.3f} V (норма: 0.85-1.0 V)")
        if 'dc10' in data:
            dc10 = data['dc10']
            output.append(f"  DC10: {dc10:.3f} V (норма: 0.95-1.1 V)")
        if 'dc18' in data:
            dc18 = data['dc18']
            output.append(f"  DC18: {dc18:.3f} V (норма: 1.7-1.9 V)")

        # Clock
        if 'clockflag' in data:
            clockflag = data['clockflag']
            clockflag_valid = data.get('clockflag_valid', clockflag == 1)
            output.append(f"  Clockflag: {clockflag} ({'Валидно' if clockflag_valid else 'Невалидно'})")
        if 'clock_drift' in data:
            clock_drift = data['clock_drift']
            output.append(f"  Clock Drift: {clock_drift:.6f} m/s")

        # Hardware Flag
        if 'hw_flag' in data:
            hw_flag = data['hw_flag']
            hw_flag_hex = data.get('hw_flag_hex', f"0x{hw_flag:02X}")
            output.append(f"  Hardware Flag: {hw_flag_hex}")

            if 'hw_flag_bits' in data:
                bits = data['hw_flag_bits']
                output.append("  Флаги оборудования:")
                output.append(f"    Тип осциллятора: {'Crystal' if bits.get('oscillator_type') else 'Oscillator'}")
                output.append(f"    VCXO/TCXO: {'TCXO' if bits.get('vcxo_tcxo') else 'VCXO'}")
                output.append(f"    Частота: {'20 MHz' if bits.get('osc_freq') else '26 MHz'}")
                output.append(
                    f"    Поддержка: {'Oscillator + Crystal' if bits.get('osc_crystal_support') else 'Только Oscillator'}")
                output.append(f"    Статус проверки: {'Валидно' if bits.get('check_status') else 'Неизвестно'}")

        # PLL Lock
        if 'pll_lock' in data:
            pll_lock = data['pll_lock']
            pll_lock_hex = data.get('pll_lock_hex', f"0x{pll_lock:04X}")
            output.append(f"  PLL Lock: {pll_lock_hex} ({pll_lock})")

        return "\n".join(output) + "\n"

    def _format_agc_data(self, data: dict) -> str:
        """Форматирование данных AGC"""
        output = ["Автоматическая регулировка усиления (AGC):"]

        if 'format' in data:
            output.append(f"  Формат: {data['format']}")

        # Главная антенна
        if 'master_antenna' in data:
            master = data['master_antenna']
            output.append("\n  Главная антенна:")
            if 'l1' in master:
                l1_val = master['l1']
                if l1_val is not None:
                    output.append(f"    L1: {l1_val} (0-255, -1 = невалидно)")
                else:
                    output.append(f"    L1: невалидно (-1)")
            if 'l2' in master:
                l2_val = master['l2']
                if l2_val is not None:
                    output.append(f"    L2: {l2_val} (0-255, -1 = невалидно)")
                else:
                    output.append(f"    L2: невалидно (-1)")
            if 'l5' in master:
                l5_val = master['l5']
                if l5_val is not None:
                    output.append(f"    L5: {l5_val} (0-255, -1 = невалидно)")
                else:
                    output.append(f"    L5: невалидно (-1)")

        # Ведомая антенна
        if 'slave_antenna' in data:
            slave = data['slave_antenna']
            output.append("\n  Ведомая антенна:")
            if 'l1' in slave:
                l1_val = slave['l1']
                if l1_val is not None:
                    output.append(f"    L1: {l1_val} (0-255, -1 = невалидно)")
                else:
                    output.append(f"    L1: невалидно (-1)")
            if 'l2' in slave:
                l2_val = slave['l2']
                if l2_val is not None:
                    output.append(f"    L2: {l2_val} (0-255, -1 = невалидно)")
                else:
                    output.append(f"    L2: невалидно (-1)")
            if 'l5' in slave:
                l5_val = slave['l5']
                if l5_val is not None:
                    output.append(f"    L5: {l5_val} (0-255, -1 = невалидно)")
                else:
                    output.append(f"    L5: невалидно (-1)")

        return "\n".join(output) + "\n"

    def _format_agric_data(self, data: dict) -> str:
        """Форматирование данных AGRIC (полный снимок полей парсера)."""
        lines = ["AGRIC данные:"]
        if "format" in data:
            lines.append(f"  Формат: {data['format']}")
        pos_txt = data.get("postype_text") or data.get("position_status")
        if pos_txt is not None:
            lines.append(f"  Статус позиции: {pos_txt}")
        elif "postype" in data:
            lines.append(f"  Тип решения (код postype): {data['postype']}")
        h_txt = data.get("heading_status_text")
        if h_txt is not None:
            lines.append(f"  Статус heading: {h_txt}")
        elif "heading_status" in data:
            lines.append(f"  Статус heading (код): {data['heading_status']}")
        dt = data.get("datetime")
        if isinstance(dt, dict):
            lines.append(
                "  Дата/время (GNSS): "
                f"{dt.get('year', '?')}-{dt.get('month', '?')}-{dt.get('day', '?')} "
                f"{dt.get('hour', '?')}:{dt.get('minute', '?')}:{dt.get('second', '?')}"
            )
        if "gnss" in data:
            lines.append(f"  Система: {data['gnss']}, длина поля: {data.get('length', '—')}")
        sats = data.get("satellites")
        if isinstance(sats, dict):
            lines.append(
                "  Спутники GPS/BDS/GLO/GAL: "
                f"{sats.get('gps', '—')}/{sats.get('bds', '—')}/"
                f"{sats.get('glo', '—')}/{sats.get('gal', '—')}"
            )
        if "rover_position" in data:
            rp = data["rover_position"]
            if all(k in rp for k in ("lat", "lon", "hgt")):
                lines.append(
                    f"  Позиция Rover: Lat={rp['lat']:.9f}, Lon={rp['lon']:.9f}, Hgt={rp['hgt']:.3f} м"
                )
        pos = data.get("position")
        if isinstance(pos, dict) and all(k in pos for k in ("lat_std", "lon_std", "hgt_std")):
            lines.append(
                "  σ позиции (lat, lon, hgt): "
                f"{pos['lat_std']:.4f}, {pos['lon_std']:.4f}, {pos['hgt_std']:.4f} м"
            )
        if "baseline" in data:
            bl = data["baseline"]
            if all(k in bl for k in ("north", "east", "up")):
                lines.append(
                    f"  Baseline N/E/U: {bl['north']:.4f}, {bl['east']:.4f}, {bl['up']:.4f} м"
                )
            if all(k in bl for k in ("n_std", "e_std", "u_std")):
                lines.append(
                    f"  σ baseline N/E/U: {bl['n_std']:.4f}, {bl['e_std']:.4f}, {bl['u_std']:.4f} м"
                )
        att = data.get("attitude")
        if isinstance(att, dict) and all(k in att for k in ("heading", "pitch", "roll")):
            lines.append(
                "  Attitude H/P/R: "
                f"{float(att['heading']):.4f}°, {float(att['pitch']):.4f}°, {float(att['roll']):.4f}°"
            )
        if "heading" in data and isinstance(data["heading"], dict) and "degree" in data["heading"]:
            try:
                lines.append(f"  Heading (поле сообщения): {float(data['heading']['degree']):.4f}°")
            except (TypeError, ValueError):
                lines.append(f"  Heading (поле сообщения): {data['heading'].get('degree')}")
        vel = data.get("velocity")
        if isinstance(vel, dict):
            if "speed" in vel:
                lines.append(f"  Скорость (модуль): {float(vel['speed']):.4f} м/с")
            if all(k in vel for k in ("north", "east", "up")):
                lines.append(
                    "  Скорость NEU: "
                    f"{float(vel['north']):.4f}, {float(vel['east']):.4f}, {float(vel['up']):.4f} м/с"
                )
            if all(k in vel for k in ("n_std", "e_std", "u_std")):
                lines.append(
                    "  σ скорости NEU: "
                    f"{float(vel['n_std']):.4f}, {float(vel['e_std']):.4f}, {float(vel['u_std']):.4f} м/с"
                )
        ecef = data.get("ecef")
        if isinstance(ecef, dict) and all(k in ecef for k in ("x", "y", "z")):
            lines.append(
                f"  ECEF X/Y/Z: {ecef['x']:.4f}, {ecef['y']:.4f}, {ecef['z']:.4f} м"
            )
        if isinstance(ecef, dict) and all(k in ecef for k in ("x_std", "y_std", "z_std")):
            lines.append(
                f"  σ ECEF: {ecef['x_std']:.4f}, {ecef['y_std']:.4f}, {ecef['z_std']:.4f} м"
            )
        bp = data.get("base_position")
        if isinstance(bp, dict) and any(abs(float(bp.get(k, 0) or 0)) > 1e-9 for k in ("lat", "lon", "alt")):
            lines.append(
                f"  База lat/lon/alt: {float(bp['lat']):.9f}, {float(bp['lon']):.9f}, {float(bp['alt']):.3f}"
            )
        sp = data.get("secondary_position")
        if isinstance(sp, dict) and any(abs(float(sp.get(k, 0) or 0)) > 1e-9 for k in ("lat", "lon", "alt")):
            lines.append(
                f"  Вторичная lat/lon/alt: {float(sp['lat']):.9f}, {float(sp['lon']):.9f}, {float(sp['alt']):.3f}"
            )
        if "gps_week_second" in data:
            lines.append(f"  GPS week second: {data['gps_week_second']}")
        if "diffage" in data:
            try:
                lines.append(f"  Diffage: {float(data['diffage']):.3f} с")
            except (TypeError, ValueError):
                lines.append(f"  Diffage: {data['diffage']}")
        if "speed_heading" in data:
            try:
                lines.append(f"  Speed heading: {float(data['speed_heading']):.4f}°")
            except (TypeError, ValueError):
                lines.append(f"  Speed heading: {data['speed_heading']}")
        if "undulation" in data:
            try:
                lines.append(f"  Undulation: {float(data['undulation']):.4f} м")
            except (TypeError, ValueError):
                lines.append(f"  Undulation: {data['undulation']}")
        if "speed_type" in data:
            lines.append(f"  Speed type: {data['speed_type']}")
        if "ascii_crc_hex" in data:
            lines.append(f"  CRC (ASCII): {data['ascii_crc_hex']}")
        if "crc" in data and str(data.get("format", "")).lower() == "binary":
            lines.append(f"  CRC (binary): {data['crc']}")
        return "\n".join(lines) + "\n"

    def _format_pvtsln_data(self, data: dict) -> str:
        """Форматирование данных PVTSLN (§7.3.22)."""
        output = ["PVTSLN данные:"]
        if "format" in data:
            output.append(f"  Формат: {data['format']}")
        if "bestpos" in data:
            bp = data["bestpos"]
            if "type" in bp:
                output.append(f"  Bestpos тип: {bp['type']}")
            if "lat" in bp and "lon" in bp and "hgt" in bp:
                output.append(f"  Лучшая позиция: Lat={bp['lat']:.9f}, Lon={bp['lon']:.9f}, Hgt={bp['hgt']:.3f}")
        if "psr_position" in data:
            pp = data["psr_position"]
            if "lat" in pp and "lon" in pp:
                output.append(
                    f"  PSR позиция: Lat={pp['lat']:.9f}, Lon={pp['lon']:.9f}, H={pp.get('height', 0):.3f}"
                )
        if "undulation" in data:
            output.append(f"  Undulation: {float(data['undulation']):.4f} м")
        if "velocity" in data:
            v = data["velocity"]
            if "north" in v and "east" in v:
                g = v.get("ground")
                if g is not None:
                    output.append(
                        f"  PSR скорость: N={v['north']:.4f}, E={v['east']:.4f}, ground={float(g):.4f} м/с"
                    )
                else:
                    output.append(f"  PSR скорость: N={v['north']:.4f}, E={v['east']:.4f} м/с")
        if "heading" in data:
            h = data["heading"]
            if "degree" in h:
                output.append(f"  Heading: {float(h['degree']):.4f}° (pitch={float(h.get('pitch', 0)):.4f}°)")
        if "dop" in data:
            d = data["dop"]
            if all(k in d for k in ("gdop", "pdop", "hdop", "htdop", "tdop")):
                output.append(
                    f"  DOP: GDOP={d['gdop']:.3f} PDOP={d['pdop']:.3f} HDOP={d['hdop']:.3f} "
                    f"HTDOP={d['htdop']:.3f} TDOP={d['tdop']:.3f}"
                )
        if "cutoff" in data:
            output.append(f"  Cutoff: {float(data['cutoff']):.2f}°")
        if "prn_no" in data and "prn_list" in data:
            prns = data["prn_list"]
            show = prns[:20]
            tail = " …" if len(prns) > 20 else ""
            output.append(f"  PRN (n={data['prn_no']}): {', '.join(str(p) for p in show)}{tail}")
        return "\n".join(output) + "\n"

    def _format_obsvm_data(self, data: dict, name: str, description: str) -> str:
        """Форматирование данных OBSVM/OBSVH/OBSVBASE"""
        output = [f"Наблюдения {description} ({name}):"]
        if 'format' in data:
            output.append(f"  Формат: {data['format']}")
        if 'obs_number' in data:
            output.append(f"  Количество наблюдений: {data['obs_number']}")

        observations = data.get('observations', [])
        if observations:
            output.append(f"\n  Примеры наблюдений (показано {min(5, len(observations))} из {len(observations)}):")
            for i, obs in enumerate(observations[:5]):
                output.append(f"\n  Наблюдение {i + 1}:")
                if obs.get("nav_system"):
                    output.append(f"    ГНСС: {obs['nav_system']}")
                if obs.get("signal_name"):
                    output.append(f"    Сигнал: {obs['signal_name']}")
                if obs.get("system_freq_note"):
                    output.append(f"    Поле System Freq (UShort): {obs['system_freq_note']}")
                if "prn" in obs:
                    output.append(f"    PRN: {obs['prn']}")
                if 'psr' in obs:
                    output.append(f"    Pseudorange: {obs['psr']:.3f} м")
                if 'adr' in obs:
                    output.append(f"    Carrier phase (ADR): {obs['adr']:.3f} циклов")
                if 'cn0' in obs:
                    output.append(f"    C/N0: {obs['cn0']:.2f} dB-Hz")
                if 'locktime' in obs:
                    output.append(f"    Lock time: {obs['locktime']:.3f} сек")
        else:
            output.append(f"\n  Наблюдения отсутствуют (возможно, нет спутников)")

        return "\n".join(output) + "\n"

    def _format_obsvmcmp_data(self, data: dict) -> str:
        """Форматирование данных OBSVMCMP"""
        output = ["Сжатые наблюдения (OBSVMCMP):"]
        if 'format' in data:
            output.append(f"  Формат: {data['format']}")
        if 'obs_number' in data:
            output.append(f"  Количество наблюдений: {data['obs_number']}")
        if 'note' in data:
            output.append(f"  Примечание: {data['note']}")

        compressed_records = data.get('compressed_records', [])
        if compressed_records:
            output.append(
                f"\n  Сжатые записи (показано {min(3, len(compressed_records))} из {len(compressed_records)}):")
            for i, record in enumerate(compressed_records[:3]):
                output.append(f"\n  Запись {i + 1}:")
                if 'raw_hex' in record:
                    hex_str = record['raw_hex']
                    if len(hex_str) > 32:
                        output.append(f"    Hex: {hex_str[:32]}... ({len(hex_str) // 2} байт)")
                    else:
                        output.append(f"    Hex: {hex_str} ({len(hex_str) // 2} байт)")
                dec = record.get('decoded')
                if dec and 'decode_error' not in dec:
                    parts = []
                    if dec.get("nav_system"):
                        parts.append(f"gnss={dec['nav_system']}")
                    if dec.get("signal_name"):
                        parts.append(f"sig={dec['signal_name']}")
                    if 'prn' in dec:
                        parts.append(f"prn={dec['prn']}")
                    if 'cn0_dbhz' in dec:
                        parts.append(f"cn0={dec['cn0_dbhz']:.1f}")
                    if 'pseudorange_m' in dec:
                        parts.append(f"psr_m={dec['pseudorange_m']:.3f}")
                    if 'adr_cycles' in dec:
                        parts.append(f"adr_cyc={dec['adr_cycles']:.3f}")
                    if 'doppler_hz' in dec:
                        parts.append(f"dopp={dec['doppler_hz']:.2f}")
                    if 'lock_time_s' in dec:
                        parts.append(f"lock_s={dec['lock_time_s']:.2f}")
                    if parts:
                        output.append(f"    Расшифровано: {', '.join(parts)}")
                elif dec and 'decode_error' in dec:
                    output.append(f"    (ошибка декода: {dec['decode_error']})")
        else:
            output.append(f"\n  Сжатые записи отсутствуют")

        return "\n".join(output) + "\n"

    def _format_config_data(self, data: dict) -> str:
        """Форматирование данных CONFIG"""
        output = ["Конфигурация устройства:"]
        if isinstance(data, dict):
            # Если это словарь с конфигурацией
            if 'format' in data:
                output.append(f"  Формат: {data['format']}")

            # Проверяем наличие бинарных данных
            if 'binary' in data:
                binary_info = data['binary']
                output.append(f"  Тип данных: бинарный")
                if isinstance(binary_info, dict):
                    if 'header' in binary_info:
                        header = binary_info['header']
                        if header and header != '0000':
                            output.append(f"  Заголовок: {header}")
                    if 'payload' in binary_info:
                        payload = binary_info['payload']
                        if payload:
                            payload_size = len(payload) // 2  # hex строка, каждый байт = 2 символа
                            output.append(f"  Размер payload: {payload_size} байт")
                            # Показываем первые байты в hex и пытаемся найти ASCII
                            if payload_size > 0:
                                hex_preview = payload[:200]  # Первые 100 байт в hex
                                output.append(f"  Payload (hex, первые 100 байт): {hex_preview}...")
                                # Пытаемся декодировать как ASCII для предпросмотра
                                try:
                                    ascii_bytes = bytes.fromhex(payload[:500])  # Увеличиваем для поиска
                                    ascii_preview = ascii_bytes.decode('ascii', errors='replace')
                                    # В payload ищем ASCII-строки протокола Unicore (часто с префиксом «$»).
                                    # NMEA — отдельный протокол (см. мануал Unicore, п. 7.1–7.2); ответы CONFIG — это ASCII, не NMEA.
                                    if '$' in ascii_preview:
                                        ascii_dollar_lines = []
                                        for line in ascii_preview.split('\n'):
                                            line = line.strip()
                                            if '$' in line:
                                                dollar_idx = line.find('$')
                                                ascii_part = line[dollar_idx:]
                                                ascii_part = ''.join(
                                                    c if 32 <= ord(c) <= 126 or c in '\r\n' else '' for c in ascii_part
                                                )
                                                if ascii_part.startswith('$') and len(ascii_part) > 5:
                                                    ascii_dollar_lines.append(ascii_part)

                                        if ascii_dollar_lines:
                                            output.append(f"  Найдены ASCII-строки в payload (первые 3):")
                                            for line in ascii_dollar_lines[:3]:
                                                # Очищаем строку от мусора
                                                clean_line = line.strip()
                                                if clean_line:
                                                    output.append(
                                                        f"    {clean_line[:70]}{'...' if len(clean_line) > 70 else ''}")
                                except:
                                    pass

            # Проверяем наличие Unicore binary данных
            if 'unicore_binary' in data:
                unicore = data['unicore_binary']
                output.append(f"  Тип данных: Unicore binary")
                if isinstance(unicore, dict):
                    if 'message_id' in unicore:
                        output.append(f"  Message ID: {unicore['message_id']}")
                    if 'message_length' in unicore:
                        output.append(f"  Message Length: {unicore['message_length']}")
                    if 'data' in unicore and isinstance(unicore['data'], dict):
                        data_info = unicore['data']
                        if 'length' in data_info:
                            output.append(f"  Размер данных: {data_info['length']} байт")

            # Разобранные ASCII-предложения ответа ($…); CONFIG — в этом же ASCII-формате Unicore, не путать с протоколом NMEA (гл. 7).
            if 'messages' in data:
                messages = data['messages']
                if isinstance(messages, list):
                    output.append(f"  ASCII-предложений в ответе: {len(messages)}")

                    # Ищем строки CONFIG (ожидаемы при запросе конфигурации)
                    config_messages = []
                    other_messages = []
                    for msg in messages:
                        if isinstance(msg, dict):
                            msg_type = msg.get('type', '')
                            raw = msg.get('raw', '')
                            if 'CONFIG' in msg_type or 'CONFIG' in raw:
                                config_messages.append(msg)
                            else:
                                other_messages.append(msg)
                        elif isinstance(msg, str):
                            if 'CONFIG' in msg.upper():
                                config_messages.append(msg)
                            else:
                                other_messages.append(msg)

                    # Показываем CONFIG сообщения (главное!)
                    if config_messages:
                        output.append(f"\n  ✓ CONFIG сообщений найдено: {len(config_messages)}")
                        output.append(f"  Важные CONFIG сообщения:")
                        # Ищем важные: COM1, COM2, COM3, PPS
                        important_keywords = ['COM1', 'COM2', 'COM3', 'PPS']
                        found_important = []
                        for msg in config_messages:
                            if isinstance(msg, dict):
                                raw = msg.get('raw', '')
                            else:
                                raw = str(msg)
                            for keyword in important_keywords:
                                if keyword in raw and keyword not in found_important:
                                    found_important.append(keyword)
                                    if isinstance(msg, dict):
                                        output.append(f"    ✓ {keyword}: {raw[:80]}{'...' if len(raw) > 80 else ''}")
                                    else:
                                        output.append(f"    ✓ {keyword}: {raw[:80]}{'...' if len(raw) > 80 else ''}")

                        # Показываем остальные CONFIG сообщения
                        remaining_config = [m for m in config_messages if not any(
                            kw in (m.get('raw', '') if isinstance(m, dict) else str(m)) for kw in found_important)]
                        if remaining_config:
                            output.append(f"\n  Другие CONFIG сообщения ({len(remaining_config)}):")
                            for i, msg in enumerate(remaining_config):  # Показываем все сообщения
                                if isinstance(msg, dict):
                                    raw = msg.get('raw', '')
                                    output.append(f"    {i + 1}. {raw[:75]}{'...' if len(raw) > 75 else ''}")
                                else:
                                    output.append(f"    {i + 1}. {str(msg)[:75]}{'...' if len(str(msg)) > 75 else ''}")
                    else:
                        output.append(f"\n  ⚠ CONFIG сообщения НЕ найдены!")
                        output.append(
                            f"     Ожидаются сообщения типа: $CONFIG,COM1, $CONFIG,COM2, $CONFIG,COM3, $CONFIG,PPS")
                        output.append(f"     Возможно, они еще не пришли или находятся в бинарных данных")

                    # Группируем остальные сообщения по типам
                    if other_messages:
                        msg_types = {}
                        for msg in other_messages:
                            if isinstance(msg, dict):
                                msg_type = msg.get('type', 'UNKNOWN')
                                if msg_type not in msg_types:
                                    msg_types[msg_type] = 0
                                msg_types[msg_type] += 1
                            elif isinstance(msg, str):
                                if msg.startswith('$'):
                                    parts = msg.split(',')
                                    if parts:
                                        msg_type = parts[0].replace('$', '')
                                        if msg_type not in msg_types:
                                            msg_types[msg_type] = 0
                                        msg_types[msg_type] += 1

                        if msg_types:
                            output.append(f"\n  Другие типы ASCII-строк (не CONFIG):")
                            for msg_type, count in sorted(msg_types.items(), key=lambda x: x[1], reverse=True)[:10]:
                                output.append(f"    {msg_type}: {count}")
                            if len(msg_types) > 10:
                                output.append(f"    ... и еще {len(msg_types) - 10} типов")

                            output.append(f"\n  Примеры других ASCII-строк (первые 3):")
                            for i, msg in enumerate(other_messages[:3]):
                                if isinstance(msg, dict):
                                    msg_type = msg.get('type', 'UNKNOWN')
                                    raw = msg.get('raw', '')
                                    if raw:
                                        output.append(
                                            f"    {i + 1}. {msg_type}: {raw[:60]}{'...' if len(raw) > 60 else ''}")
                                    else:
                                        output.append(
                                            f"    {i + 1}. {msg_type}: {str(msg)[:60]}{'...' if len(str(msg)) > 60 else ''}")
                                elif isinstance(msg, str):
                                    output.append(f"    {i + 1}. {msg[:60]}{'...' if len(msg) > 60 else ''}")

            # Показываем основные параметры конфигурации (если есть)
            config_keys = ['port', 'baudrate', 'data_bits', 'parity', 'stop_bits',
                           'pps_enable', 'pps_timeref', 'pps_polarity', 'pps_width', 'pps_period']
            found_config = False
            for key in config_keys:
                if key in data:
                    output.append(f"  {key.replace('_', ' ').title()}: {data[key]}")
                    found_config = True

            # Если есть другие ключи, показываем их
            shown_keys = ['format', 'binary', 'unicore_binary', 'messages', 'type', 'nmea_count'] + config_keys
            other_keys = [k for k in data.keys() if k not in shown_keys]
            if other_keys:
                output.append(f"\n  Другие параметры: {', '.join(other_keys[:10])}")
                if len(other_keys) > 10:
                    output.append(f"    ... и еще {len(other_keys) - 10} параметров")

            if not found_config and 'binary' not in data and 'unicore_binary' not in data and 'messages' not in data:
                output.append(f"  ⚠ Конфигурация в бинарном формате, требуется специальный парсер")
        elif isinstance(data, list):
            # Если это список строк конфигурации
            output.append(f"  Количество строк: {len(data)}")
            output.append(f"\n  Примеры строк (показано {min(10, len(data))} из {len(data)}):")
            for i, line in enumerate(data[:10]):
                output.append(f"    {i + 1}. {line[:80]}{'...' if len(line) > 80 else ''}")
        else:
            output.append(f"  Тип данных: {type(data).__name__}")
            output.append(f"  Данные: {str(data)[:200]}")

        return "\n".join(output) + "\n"

    def _format_generic_data(self, name: str, data: dict) -> str:
        """Общее форматирование для остальных команд"""
        output = [f"Данные {name}:"]
        if 'format' in data:
            output.append(f"  Формат: {data['format']}")
        if 'status' in data:
            output.append(f"  Статус: {data['status']}")
        if 'obs_number' in data:
            output.append(f"  Количество наблюдений: {data['obs_number']}")
        # Показываем первые несколько ключевых полей
        important_keys = ['type', 'mode', 'port', 'rate', 'trigger']
        for key in important_keys:
            if key in data:
                output.append(f"  {key.capitalize()}: {data[key]}")
        return "\n".join(output) + "\n"

    def format_config_result(self, result: dict) -> str:
        """Форматирование результата config команды"""
        output = []
        if "error" in result:
            output.append(f"Ошибка: {result['error']}")
        elif result.get("success"):
            output.append("Команда выполнена успешно")
            if "confirmation" in result:
                output.append(f"Подтверждение: {result['confirmation']}")
        elif "command" in result:
            output.append(f"Отправлена команда: {result['command']}")
            if "confirmed" in result:
                output.append("Подтверждение получено")
            elif "warning" in result:
                output.append(f"Предупреждение: {result['warning']}")
        else:
            output.append("Команда выполнена, но статус неизвестен")
        return "\n".join(output) + "\n"

    def _format_system_result(self, result: dict) -> str:
        """Форматирование результата System команд (restore_output, log, unlog, freset, reset, saveconfig)"""
        output = []
        if result.get("oneshot_query_sent") and result.get("success"):
            output.append("Отправлен одиночный запрос.")
            return "\n".join(output) + "\n"
        if "error" in result:
            output.append(f"Ошибка: {result['error']}")
            if result.get("log_multi_lines"):
                output.append("Уже выполнено:")
                for ln in result["log_multi_lines"]:
                    output.append(f"  {ln}")
        elif result.get("success"):
            output.append("Команда выполнена успешно")
            if "command" in result:
                output.append(f"Команда: {result['command']}")
            if result.get("response_received"):
                output.append("Ответ от устройства получен.")
            elif result.get("confirmation"):
                output.append(f"Подтверждение: {result['confirmation']}")
            if result.get("note"):
                output.append(f"Примечание: {result['note']}")
            if result.get("log_multi_lines"):
                for ln in result["log_multi_lines"]:
                    output.append(f"  {ln}")
        elif "command" in result:
            output.append(f"Отправлена команда: {result['command']}")
            if result.get("note"):
                output.append(result["note"])
            if result.get("log_multi_lines"):
                for ln in result["log_multi_lines"]:
                    output.append(f"  {ln}")
        else:
            output.append("Статус неизвестен")
        return "\n".join(output) + "\n"

    def _notification_message_for_command(self, command: str, result: dict) -> str:
        """Текст уведомления для команд без ответа (messagebox)."""
        if command == "set_mode_rover":
            cmd = result.get("command") if isinstance(result, dict) else None
            if cmd:
                return f"Отправлено: {cmd}\n(ответ приёмника может отсутствовать.)"
            return "Режим ROVER: команда отправлена.\n(ответ приёмника может отсутствовать.)"
        if command == "set_mode_heading2":
            cmd = result.get("command") if isinstance(result, dict) else None
            if cmd:
                return f"Отправлено: {cmd}\n(ответ приёмника может отсутствовать.)"
            return "Режим HEADING2: команда отправлена.\n(ответ приёмника может отсутствовать.)"
        return "Команда отправлена"

    def format_set_mode_result(self, result: dict, command: str = "") -> str:
        """Форматирование результата set_mode команды (лог)."""
        output = []
        if "error" in result:
            output.append(f"Ошибка: {result['error']}")
        elif command in ("set_mode_rover", "set_mode_heading2"):
            mode_name = "ROVER" if command == "set_mode_rover" else "HEADING2"
            if result.get("command"):
                output.append(f"Отправлена: {result['command']}")
            else:
                output.append(f"Команда MODE {mode_name} отправлена.")
            if result.get("confirmation"):
                output.append("Подтверждение получено")
            else:
                output.append("Режим установлен.")
        elif "command" in result:
            output.append(f"Отправлена команда: {result['command']}")
            if result.get("confirmation"):
                output.append("Подтверждение получено")
        else:
            output.append("Команда отправлена")
        return "\n".join(output) + "\n"

    def log_result(self, text: str) -> None:
        """Добавление текста в область результатов"""
        self.results_text.insert(tk.END, text)
        self.results_text.see(tk.END)
        self.root.update()

    def toggle_params(self) -> None:
        """Сворачивание/разворачивание панели параметров"""
        if self.params_expanded:
            self.params_content_frame.pack_forget()
            self.params_toggle_btn.config(text="▶ Параметры команды")
            self.params_expanded = False
        else:
            self.params_content_frame.pack(fill=tk.BOTH, expand=True)
            self.params_toggle_btn.config(text="▼ Параметры команды")
            self.params_expanded = True
        self.root.after_idle(self._sync_params_pane_height)

    def toggle_data(self) -> None:
        """Сворачивание/разворачивание панели «Текущее сообщение» (поля + при необходимости таблица наблюдений)."""
        if getattr(self, "_data_pane_hidden", False):
            return
        if self.data_expanded:
            self.data_content_frame.pack_forget()
            self.data_toggle_btn.config(text="▶ Текущее сообщение")
            self.data_expanded = False
        else:
            self.data_content_frame.pack(fill=tk.BOTH, expand=True)
            self.data_toggle_btn.config(text="▼ Текущее сообщение")
            self.data_expanded = True
        self.root.after_idle(self._redistribute_right_vertical_panes)

    def toggle_table(self) -> None:
        """Сворачивание/разворачивание панели таблицы наблюдений."""
        if self.table_expanded:
            self.table_content_frame.pack_forget()
            self.table_toggle_btn.config(text="▶ Таблица")
            self.table_expanded = False
        else:
            self.table_content_frame.pack(fill=tk.BOTH, expand=True)
            self.table_toggle_btn.config(text="▼ Таблица")
            self.table_expanded = True
        self.root.after_idle(self._redistribute_right_vertical_panes)

    def toggle_log(self) -> None:
        """Сворачивание/разворачивание лога"""
        if self.log_expanded:
            self.log_content_frame.pack_forget()
            self.log_toggle_btn.config(text="▶ Лог")
            self.log_expanded = False
        else:
            self.log_content_frame.pack(fill=tk.BOTH, expand=True)
            self.log_toggle_btn.config(text="▼ Лог")
            self.log_expanded = True
        self.root.after_idle(self._redistribute_right_vertical_panes)

    def clear_data_table(self) -> None:
        """Очистить поля «Текущее сообщение» и таблицу наблюдений (если была)."""
        self._clear_tree_data()
        for w in self.message_form_inner.winfo_children():
            w.destroy()
        self._message_title_var.set("")
        try:
            self.message_form_outer.pack_forget()
            self.data_tree_frame.pack_forget()
            self.data_empty_label.pack(pady=20)
            self.table_empty_label.pack(pady=20)
        except Exception:
            pass
        self._update_data_empty_hint()
        self._refresh_data_table_export_state()

    def _table_export_rows(self) -> List[Dict[str, Any]]:
        return [row for row in self.data_rows if isinstance(row, dict) and not row.get("_separator")]

    def _refresh_data_table_export_state(self) -> None:
        rows = self._table_export_rows()
        self.export_table_btn.config(state=tk.NORMAL if rows else tk.DISABLED)
        try:
            if rows:
                if self.table_empty_label.winfo_ismapped():
                    self.table_empty_label.pack_forget()
            else:
                if not self.table_empty_label.winfo_ismapped():
                    self.table_empty_label.pack(pady=20)
        except Exception:
            pass

    def export_data_table(self) -> None:
        rows = self._table_export_rows()
        if not rows:
            messagebox.showwarning("Таблица", "Нет строк таблицы для экспорта.")
            return
        short = self._get_command_display_short(self.current_command or "table").replace(" ", "_")
        path = filedialog.asksaveasfilename(
            title="Экспорт таблицы",
            defaultextension=".csv",
            initialfile=f"{short.lower()}_table.csv",
            filetypes=[("CSV", "*.csv"), ("JSON", "*.json"), ("Все файлы", "*.*")],
        )
        if not path:
            return
        out = Path(path)
        suffix = out.suffix.lower()
        if suffix not in (".csv", ".json"):
            out = out.with_suffix(".csv")
            suffix = ".csv"
        if suffix == ".json":
            out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            cols = [c for c in self.data_columns if c] or list(rows[0].keys())
            with out.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
                writer.writeheader()
                for row in rows:
                    writer.writerow({col: row.get(col, "") for col in cols})
        self.log_result(f"Таблица сохранена: {out}\n")
        messagebox.showinfo("Таблица", f"Таблица сохранена:\n{out}")

    def _renumber_data_tree_rows(self) -> None:
        """Колонка «#»: только для строк данных; строки-разделители без номера."""
        n = 0
        for item_id in self.data_tree.get_children():
            tags = self.data_tree.item(item_id, "tags") or ()
            if "obs_sep" in tags:
                self.data_tree.item(item_id, text=" ")
            else:
                n += 1
                self.data_tree.item(item_id, text=str(n))

    def _trim_data_table_to_limit(self) -> None:
        """Оставляем в таблице не более MAX_DATA_ROWS записей (удаляем самые старые — с конца списка)."""
        while len(self.data_rows) > MAX_DATA_ROWS:
            self.data_rows.pop()
            children = self.data_tree.get_children()
            if children:
                self.data_tree.delete(children[-1])
        self._renumber_data_tree_rows()
        self._autofit_data_tree_columns()

    def _autofit_data_tree_columns(self) -> None:
        """Ширина столбцов Treeview по заголовку и содержимому ячеек (с потолком и minwidth)."""
        tree = self.data_tree
        cols = list(tree["columns"])
        if not cols:
            return
        try:
            cell_font = tkfont.nametofont("TkTreeview.Font")
        except tk.TclError:
            cell_font = tkfont.nametofont("TkDefaultFont")
        try:
            head_font = tkfont.nametofont("TkHeadingFont")
        except tk.TclError:
            head_font = cell_font
        pad = 20
        max_w = 480
        min_w = 40
        show = str(tree.cget("show"))
        tree_shown = "tree" in show.split()

        def measure(font: tkfont.Font, text: Any) -> int:
            s = "" if text is None else str(text)
            return font.measure(s) if s else 0

        children = tree.get_children()

        if tree_shown:
            sizes = [measure(head_font, tree.heading("#0", "text"))]
            for iid in children:
                sizes.append(measure(cell_font, tree.item(iid, "text")))
            w = max(min_w, min(max_w, max(sizes) + pad)) if sizes else min_w
            tree.column("#0", width=w, stretch=tk.NO)

        for idx, col in enumerate(cols):
            sizes = [measure(head_font, tree.heading(col, "text"))]
            for iid in children:
                vals = tree.item(iid, "values") or ()
                if idx < len(vals):
                    sizes.append(measure(cell_font, vals[idx]))
            w = max(min_w, min(max_w, max(sizes) + pad)) if sizes else min_w
            try:
                mw = int(tree.column(col, "minwidth"))
                if mw > 0:
                    w = max(w, mw)
            except (tk.TclError, TypeError, ValueError):
                pass
            tree.column(col, width=w, stretch=tk.NO)

    def format_data_for_table(self, command: str, data: dict) -> dict:
        """Преобразование данных в формат для таблицы, соответствующий логу"""
        result = {}

        # Используем те же форматтеры, что и для лога
        if command == "query_version":
            if 'format' in data:
                result['Формат'] = data['format']
            if 'product_name' in data:
                result['Продукт'] = data['product_name']
            if 'product_type' in data:
                result['Тип продукта'] = data['product_type']
            if 'sw_version' in data:
                result['Версия ПО'] = data['sw_version']
            if 'psn' in data:
                result['PN/SN'] = data['psn']
            if 'auth' in data and data['auth'] != '-':
                result['Авторизация'] = data['auth']
            if 'efuse_id' in data:
                result['Board ID'] = data['efuse_id']
            if 'comp_time' in data:
                result['Дата компиляции'] = data['comp_time']

        elif command == "query_mode":
            # Основной режим
            if 'mode' in data:
                mode = data['mode']
                if mode:
                    result['Режим'] = mode

            # Подтип режима (может быть None)
            if 'mode_subtype' in data and data['mode_subtype']:
                mode_subtype = data['mode_subtype']
                if isinstance(mode_subtype, str):
                    mode_subtype_clean = mode_subtype.rstrip(', ').strip()
                    if mode_subtype_clean:
                        result['Подтип'] = mode_subtype_clean

            # Heading mode
            if 'heading_mode' in data and data['heading_mode']:
                result['Heading Mode'] = data['heading_mode']

            # Строка режима
            if 'mode_string' in data and data['mode_string']:
                mode_string_clean = data['mode_string'].rstrip(', ').strip()
                if mode_string_clean:
                    result['Строка режима'] = mode_string_clean

        elif command == "query_baseinfo":
            if 'status' in data:
                result['Статус'] = data['status']
            if 'x' in data and 'y' in data and 'z' in data:
                result['Координаты ECEF'] = f"X={data['x']:.3f}, Y={data['y']:.3f}, Z={data['z']:.3f}"
            if 'station_id' in data:
                result['Station ID'] = data['station_id']
            if 'format' in data:
                result['Формат'] = data['format']

        elif command in ("query_gpsion", "query_bdsion"):
            if 'format' in data:
                result['Формат'] = data['format']
            if 'alpha' in data:
                alpha = data['alpha']
                if 'a0' in alpha:
                    result['Alpha a0'] = f"{alpha['a0']:.15e}"
                if 'a1' in alpha:
                    result['Alpha a1'] = f"{alpha['a1']:.15e}"
                if 'a2' in alpha:
                    result['Alpha a2'] = f"{alpha['a2']:.15e}"
                if 'a3' in alpha:
                    result['Alpha a3'] = f"{alpha['a3']:.15e}"
            if 'beta' in data:
                beta = data['beta']
                if 'b0' in beta:
                    result['Beta b0'] = f"{beta['b0']:.15e}"
                if 'b1' in beta:
                    result['Beta b1'] = f"{beta['b1']:.15e}"
                if 'b2' in beta:
                    result['Beta b2'] = f"{beta['b2']:.15e}"
                if 'b3' in beta:
                    result['Beta b3'] = f"{beta['b3']:.15e}"
            if 'us_svid' in data:
                result['SVID'] = data['us_svid']
            if 'us_week' in data:
                result['Week'] = data['us_week']
            if 'ul_sec' in data:
                result['Second'] = f"{data['ul_sec']} мс"
            if 'reserved' in data:
                result['Reserved'] = data['reserved']

        elif command == "query_galion":
            if 'format' in data:
                result['Формат'] = data['format']
            if 'alpha' in data:
                alpha = data['alpha']
                if 'a0' in alpha:
                    result['Alpha a0'] = f"{alpha['a0']:.15e}"
                if 'a1' in alpha:
                    result['Alpha a1'] = f"{alpha['a1']:.15e}"
                if 'a2' in alpha:
                    result['Alpha a2'] = f"{alpha['a2']:.15e}"
            if 'sf' in data:
                sf = data['sf']
                if 'sf1' in sf:
                    result['SF1'] = sf['sf1']
                if 'sf2' in sf:
                    result['SF2'] = sf['sf2']
                if 'sf3' in sf:
                    result['SF3'] = sf['sf3']
                if 'sf4' in sf:
                    result['SF4'] = sf['sf4']
                if 'sf5' in sf:
                    result['SF5'] = sf['sf5']
            if 'reserved' in data:
                result['Reserved'] = data['reserved']

        elif command == "query_bd3ion":
            if 'format' in data:
                result['Формат'] = data['format']
            if 'a' in data:
                a = data['a']
                for i in range(1, 10):
                    ak = f'a{i}'
                    if ak in a:
                        result[f'Коэфф. a{i}'] = f"{a[ak]:.15e}"
            if 'reserved' in data:
                result['Reserved'] = data['reserved']

        elif command == "query_gpsutc":
            if 'format' in data:
                result['Формат'] = data['format']
            if 'utc_wn' in data:
                result['UTC нед.'] = data['utc_wn']
            if 'tot' in data:
                result['TOT'] = data['tot']
            if 'A0' in data:
                result['A0 (смещ.)'] = f"{data['A0']:.15e}"
            if 'A1' in data:
                result['A1 (скорость)'] = f"{data['A1']:.15e}"
            if 'wn_lsf' in data:
                result['wn_lsf'] = data['wn_lsf']
            if 'dn' in data:
                result['dn'] = data['dn']
            if 'delta_ls' in data:
                result['delta_ls'] = data['delta_ls']
            if 'delta_lsf' in data:
                result['delta_lsf'] = data['delta_lsf']
            if 'delta_utc' in data:
                result['delta_utc'] = data['delta_utc']
            if 'reserved' in data:
                result['Reserved'] = data['reserved']

        elif command == "query_bd3utc":
            if 'format' in data:
                result['Формат'] = data['format']
            if 'utc_wn' in data:
                result['UTC нед.'] = data['utc_wn']
            if 'tot' in data:
                result['TOT'] = data['tot']
            if 'A0' in data:
                result['A0 (смещ.)'] = f"{data['A0']:.15e}"
            if 'A1' in data:
                result['A1 (дрейф)'] = f"{data['A1']:.15e}"
            if 'A2' in data:
                result['A2 (дрейф)'] = f"{data['A2']:.15e}"
            if 'wn_lsf' in data:
                result['wn_lsf'] = data['wn_lsf']
            if 'dn' in data:
                result['dn'] = data['dn']
            if 'delta_ls' in data:
                result['delta_ls'] = data['delta_ls']
            if 'delta_lsf' in data:
                result['delta_lsf'] = data['delta_lsf']
            if 'reserved' in data:
                result['Reserved'] = data['reserved']
            if 'reserved2' in data:
                result['Reserved2'] = data['reserved2']

        elif command == "query_uniloglist":
            if 'count' in data:
                result['Количество'] = data['count']
            if 'logs' in data and isinstance(data['logs'], list):
                for i, log in enumerate(data['logs'], 1):
                    port = log.get('port', 'N/A')
                    message = log.get('message', 'N/A')
                    trigger = log.get('trigger', 'N/A')
                    period = log.get('period')
                    period_str = f" {period}" if period else ""
                    result[f"Лог {i}"] = f"{message} ({port}) - {trigger}{period_str}"

        elif command == "query_bestnav":
            if 'format' in data:
                result['Формат'] = data['format']
            if 'position' in data:
                pos = data['position']
                if 'sol_status' in pos:
                    result['Статус решения'] = pos['sol_status']
                if 'pos_type' in pos:
                    result['Тип позиции'] = pos['pos_type']
                if 'lat' in pos and 'lon' in pos and 'hgt' in pos:
                    result['Позиция'] = f"Lat={pos['lat']:.9f}, Lon={pos['lon']:.9f}, Hgt={pos['hgt']:.3f}"
                if 'num_svs' in pos and 'num_soln_svs' in pos:
                    result['Спутники'] = f"{pos['num_svs']} / {pos['num_soln_svs']}"
                if 'datum_id' in pos:
                    result['Datum'] = str(pos['datum_id'])
            if 'extended' in data:
                ext = data['extended']
                if 'gal_bds3_signals_text' in ext:
                    result['Маска GAL/BDS3'] = ext['gal_bds3_signals_text']
                if 'gps_glo_bds2_signals_text' in ext:
                    result['Маска GPS/GLO/BDS2'] = ext['gps_glo_bds2_signals_text']
                if ext.get('ext_sol_rtk_verification'):
                    result['RTK verify'] = ext['ext_sol_rtk_verification']
                if 'ionospheric_correction_type' in ext:
                    result['Iono type'] = str(ext['ionospheric_correction_type'])
            if 'velocity' in data:
                vel = data['velocity']
                if 'sol_status' in vel:
                    result['Статус скорости'] = vel['sol_status']
                if 'vel_type' in vel:
                    result['Тип скорости'] = vel['vel_type']
                if 'hor_spd' in vel and 'trk_gnd' in vel and 'vert_spd' in vel:
                    result['Скорость (hor/trk°/vert)'] = (
                        f"{vel['hor_spd']:.6f} / {vel['trk_gnd']:.2f} / {vel['vert_spd']:.6f}"
                    )

        elif command == "query_adrnav" or command == "query_adrnavh":
            if 'position' in data:
                pos = data['position']
                if 'sol_status' in pos:
                    result['Статус позиции'] = pos['sol_status']
                if 'pos_type' in pos:
                    result['Тип позиции'] = pos['pos_type']
                if 'lat' in pos and 'lon' in pos and 'hgt' in pos:
                    result['Позиция'] = f"Lat={pos['lat']:.9f}, Lon={pos['lon']:.9f}, Hgt={pos['hgt']:.3f}"
                if 'lat_sigma' in pos and 'lon_sigma' in pos and 'hgt_sigma' in pos:
                    result[
                        'Sigma (lat,lon,hgt)'] = f"{pos['lat_sigma']:.3f}, {pos['lon_sigma']:.3f}, {pos['hgt_sigma']:.3f}"
            if 'velocity' in data:
                vel = data['velocity']
                if 'hor_speed' in vel and 'track_ground' in vel and 'vert_speed' in vel:
                    result[
                        'Скорость'] = f"Vh={vel['hor_speed']:.3f}, Trk={vel['track_ground']:.2f}, Vv={vel['vert_speed']:.3f}"
            if 'metadata' in data:
                meta = data['metadata']
                if 'station_id' in meta:
                    result['Station ID'] = meta['station_id']
                if 'num_sats_used' in meta:
                    result['Спутников в решении'] = meta['num_sats_used']

        elif command == "query_pppnav":
            if 'position' in data:
                pos = data['position']
                if 'sol_status' in pos:
                    result['Статус PPP'] = pos['sol_status']
                if 'pos_type' in pos:
                    result['Тип позиции'] = pos['pos_type']
                if 'lat' in pos and 'lon' in pos and 'hgt' in pos:
                    result['Позиция'] = f"Lat={pos['lat']:.9f}, Lon={pos['lon']:.9f}, Hgt={pos['hgt']:.3f}"
                if 'lat_sigma' in pos and 'lon_sigma' in pos and 'hgt_sigma' in pos:
                    result[
                        'Sigma (lat,lon,hgt)'] = f"{pos['lat_sigma']:.3f}, {pos['lon_sigma']:.3f}, {pos['hgt_sigma']:.3f}"
            if 'metadata' in data:
                meta = data['metadata']
                if 'num_sats_used' in meta:
                    result['Спутников в решении'] = meta['num_sats_used']

        elif command == "query_sppnav" or command == "query_sppnavh":
            if 'position' in data:
                pos = data['position']
                if 'sol_status' in pos:
                    result['Статус SPP'] = pos['sol_status']
                if 'pos_type' in pos:
                    result['Тип позиции'] = pos['pos_type']
                if 'lat' in pos and 'lon' in pos and 'hgt' in pos:
                    result['Позиция'] = f"Lat={pos['lat']:.9f}, Lon={pos['lon']:.9f}, Hgt={pos['hgt']:.3f}"
                if 'lat_sigma' in pos and 'lon_sigma' in pos and 'hgt_sigma' in pos:
                    result[
                        'Sigma (lat,lon,hgt)'] = f"{pos['lat_sigma']:.3f}, {pos['lon_sigma']:.3f}, {pos['hgt_sigma']:.3f}"
            if 'velocity' in data:
                vel = data['velocity']
                if 'hor_speed' in vel and 'track_ground' in vel and 'vert_speed' in vel:
                    result[
                        'Скорость'] = f"Vh={vel['hor_speed']:.3f}, Trk={vel['track_ground']:.2f}, Vv={vel['vert_speed']:.3f}"

        elif command in ("query_stadop", "query_adrdop", "query_adrdoph"):
            if 'gdop' in data:
                result['GDOP'] = f"{data['gdop']:.3f}"
            if 'pdop' in data:
                result['PDOP'] = f"{data['pdop']:.3f}"
            if 'hdop' in data and 'vdop' in data:
                result['HDOP / VDOP'] = f"{data['hdop']:.3f} / {data['vdop']:.3f}"
            if 'num_satellites' in data:
                result['Число спутников'] = data['num_satellites']

        elif command == "query_bestnavxyz":
            if 'format' in data:
                result['Формат'] = data['format']
            if 'position' in data:
                pos = data['position']
                if 'P_sol_status' in pos:
                    result['P sol status'] = str(pos['P_sol_status'])
                if 'pos_type' in pos:
                    result['Тип позиции'] = str(pos['pos_type'])
                if 'P_X' in pos and 'P_Y' in pos and 'P_Z' in pos:
                    result['Позиция ECEF'] = f"X={pos['P_X']:.3f}, Y={pos['P_Y']:.3f}, Z={pos['P_Z']:.3f}"
                if 'P_X_sigma' in pos:
                    result[
                        'Sigma X,Y,Z'] = f"{pos.get('P_X_sigma', 0):.4f}, {pos.get('P_Y_sigma', 0):.4f}, {pos.get('P_Z_sigma', 0):.4f}"
            if 'velocity' in data:
                vel = data['velocity']
                if 'V_sol_status' in vel:
                    result['V sol status'] = str(vel['V_sol_status'])
                if 'vel_type' in vel:
                    result['Тип скорости'] = str(vel['vel_type'])
                if 'V_X' in vel and 'V_Y' in vel and 'V_Z' in vel:
                    result['Скорость ECEF'] = f"Vx={vel['V_X']:.4f}, Vy={vel['V_Y']:.4f}, Vz={vel['V_Z']:.4f}"
            if 'metadata' in data:
                meta = data['metadata']
                if 'station_id' in meta:
                    result['Station ID'] = str(meta['station_id'])
                nt = meta.get('num_sats_tracked')
                nu = meta.get('num_sats_used')
                if nt is not None and nu is not None:
                    result['Спутники (отслеж/реш)'] = f"{nt} / {nu}"
                elif nu is not None:
                    result['Спутники (отслеж/реш)'] = str(nu)
                gl = meta.get('num_gg_l1')
                mu = meta.get('num_soln_multi_svs')
                if gl is not None or mu is not None:
                    result['L1 / multi'] = f"{gl if gl is not None else '—'} / {mu if mu is not None else '—'}"
            if 'extended' in data:
                ext = data['extended']
                if 'gal_bds3_signals_text' in ext:
                    result['Маска GAL/BDS3'] = ext['gal_bds3_signals_text']
                if 'gps_glo_bds2_signals_text' in ext:
                    result['Маска GPS/GLO/BDS2'] = ext['gps_glo_bds2_signals_text']
                if ext.get('ext_sol_rtk_verification'):
                    result['RTK verify'] = ext['ext_sol_rtk_verification']
                if 'ionospheric_correction_type' in ext:
                    result['Iono type'] = str(ext['ionospheric_correction_type'])

        elif command == "query_agric":
            if "format" in data:
                result["Формат"] = str(data["format"])
            pos_txt = data.get("postype_text") or data.get("position_status")
            if pos_txt is not None:
                result["Статус позиции"] = str(pos_txt)
            elif "postype" in data:
                result["Статус позиции"] = f"{data['postype']} (код)"
            h_txt = data.get("heading_status_text")
            if h_txt is not None:
                result["Статус heading"] = str(h_txt)
            elif "heading_status" in data:
                result["Статус heading"] = str(data["heading_status"])
            dt = data.get("datetime")
            if isinstance(dt, dict) and all(k in dt for k in ("year", "month", "day", "hour", "minute", "second")):
                result["Дата и время (GNSS)"] = (
                    f"{dt['year']:02d}-{dt['month']:02d}-{dt['day']:02d} "
                    f"{dt['hour']:02d}:{dt['minute']:02d}:{dt['second']:02d}"
                )
            if "gnss" in data:
                result["Система"] = str(data["gnss"])
            if "length" in data:
                result["Длина полезной нагрузки"] = str(data["length"])
            sats = data.get("satellites")
            if isinstance(sats, dict):
                result["Спутники (GPS/BDS/GLO/GAL)"] = (
                    f"{sats.get('gps', '—')}/{sats.get('bds', '—')}/"
                    f"{sats.get('glo', '—')}/{sats.get('gal', '—')}"
                )
            if "rover_position" in data:
                rp = data["rover_position"]
                if "lat" in rp and "lon" in rp and "hgt" in rp:
                    result["Позиция Rover"] = f"Lat={rp['lat']:.9f}, Lon={rp['lon']:.9f}, Hgt={rp['hgt']:.3f}"
            pos = data.get("position")
            if isinstance(pos, dict) and all(k in pos for k in ("lat_std", "lon_std", "hgt_std")):
                result["σ позиции (lat, lon, hgt)"] = (
                    f"{pos['lat_std']:.4f}, {pos['lon_std']:.4f}, {pos['hgt_std']:.4f} м"
                )
            if "baseline" in data:
                bl = data["baseline"]
                if "north" in bl and "east" in bl and "up" in bl:
                    result["Baseline"] = f"N={bl['north']:.4f}, E={bl['east']:.4f}, U={bl['up']:.4f} м"
                if all(k in bl for k in ("n_std", "e_std", "u_std")):
                    result["σ baseline (N,E,U)"] = f"{bl['n_std']:.4f}, {bl['e_std']:.4f}, {bl['u_std']:.4f} м"
            att = data.get("attitude")
            if isinstance(att, dict) and all(k in att for k in ("heading", "pitch", "roll")):
                result["Attitude (H/P/R)"] = (
                    f"{float(att['heading']):.4f}°, {float(att['pitch']):.4f}°, {float(att['roll']):.4f}°"
                )
            if "heading" in data:
                h = data["heading"]
                if isinstance(h, dict) and "degree" in h:
                    try:
                        result["Heading (поле сообщения)"] = f"{float(h['degree']):.4f}°"
                    except (TypeError, ValueError):
                        result["Heading (поле сообщения)"] = str(h.get("degree", ""))
            vel = data.get("velocity")
            if isinstance(vel, dict):
                parts = []
                if "speed" in vel:
                    parts.append(f"скорость={float(vel['speed']):.4f} м/с")
                if all(k in vel for k in ("north", "east", "up")):
                    parts.append(
                        f"NEU={float(vel['north']):.4f},{float(vel['east']):.4f},{float(vel['up']):.4f} м/с"
                    )
                if all(k in vel for k in ("n_std", "e_std", "u_std")):
                    parts.append(
                        f"σVel={float(vel['n_std']):.4f},{float(vel['e_std']):.4f},{float(vel['u_std']):.4f}"
                    )
                if parts:
                    result["Скорость"] = "; ".join(parts)
            ecef = data.get("ecef")
            if isinstance(ecef, dict) and all(k in ecef for k in ("x", "y", "z")):
                result["ECEF"] = f"X={ecef['x']:.4f}, Y={ecef['y']:.4f}, Z={ecef['z']:.4f} м"
            if isinstance(ecef, dict) and all(k in ecef for k in ("x_std", "y_std", "z_std")):
                result["σ ECEF"] = f"{ecef['x_std']:.4f}, {ecef['y_std']:.4f}, {ecef['z_std']:.4f} м"
            bp = data.get("base_position")
            if isinstance(bp, dict) and any(abs(float(bp.get(k, 0) or 0)) > 1e-9 for k in ("lat", "lon", "alt")):
                result["База (lat, lon, alt)"] = (
                    f"{float(bp['lat']):.9f}, {float(bp['lon']):.9f}, {float(bp['alt']):.3f}"
                )
            sp = data.get("secondary_position")
            if isinstance(sp, dict) and any(abs(float(sp.get(k, 0) or 0)) > 1e-9 for k in ("lat", "lon", "alt")):
                result["Вторичная антенна (lat, lon, alt)"] = (
                    f"{float(sp['lat']):.9f}, {float(sp['lon']):.9f}, {float(sp['alt']):.3f}"
                )
            if "gps_week_second" in data:
                result["GPS week second"] = str(data["gps_week_second"])
            if "diffage" in data:
                try:
                    result["Diffage"] = f"{float(data['diffage']):.3f} с"
                except (TypeError, ValueError):
                    result["Diffage"] = str(data["diffage"])
            if "speed_heading" in data:
                try:
                    result["Speed heading"] = f"{float(data['speed_heading']):.4f}°"
                except (TypeError, ValueError):
                    result["Speed heading"] = str(data["speed_heading"])
            if "undulation" in data:
                try:
                    result["Undulation"] = f"{float(data['undulation']):.4f} м"
                except (TypeError, ValueError):
                    result["Undulation"] = str(data["undulation"])
            if "speed_type" in data:
                result["Speed type"] = str(data["speed_type"])
            if "ascii_crc_hex" in data:
                result["CRC (ASCII)"] = str(data["ascii_crc_hex"])
            if "crc" in data and str(data.get("format", "")).lower() == "binary":
                result["CRC (binary)"] = str(data["crc"])

        elif command == "query_pvtsln":
            if "format" in data:
                result["Формат"] = str(data["format"])
            if "bestpos" in data:
                bp = data["bestpos"]
                if "type" in bp:
                    result["Тип позиции (best)"] = str(bp["type"])
                if "lat" in bp and "lon" in bp and "hgt" in bp:
                    result["Лучшая позиция"] = f"Lat={bp['lat']:.9f}, Lon={bp['lon']:.9f}, Hgt={bp['hgt']:.3f}"
            if "psr_position" in data:
                pp = data["psr_position"]
                if "lat" in pp and "lon" in pp:
                    result["PSR позиция"] = (
                        f"Lat={pp['lat']:.9f}, Lon={pp['lon']:.9f}, H={float(pp.get('height', 0)):.3f}"
                    )
            if "undulation" in data:
                result["Undulation"] = f"{float(data['undulation']):.4f} м"
            if "heading" in data:
                h = data["heading"]
                if "degree" in h:
                    result["Heading"] = f"{float(h['degree']):.4f}°"
            if "velocity" in data:
                v = data["velocity"]
                if "north" in v and "east" in v:
                    g = v.get("ground")
                    if g is not None:
                        result[
                            "PSR скорость"] = f"N={float(v['north']):.4f}, E={float(v['east']):.4f}, G={float(g):.4f}"
                    else:
                        result["PSR скорость"] = f"N={float(v['north']):.4f}, E={float(v['east']):.4f}"
            if "dop" in data:
                d = data["dop"]
                if all(k in d for k in ("gdop", "pdop", "hdop", "htdop", "tdop")):
                    result["DOP"] = (
                        f"G={d['gdop']:.3f} P={d['pdop']:.3f} H={d['hdop']:.3f} "
                        f"HT={d['htdop']:.3f} T={d['tdop']:.3f}"
                    )
            if "cutoff" in data:
                result["Cutoff"] = f"{float(data['cutoff']):.2f}°"
            if "prn_no" in data and "prn_list" in data:
                prns = data["prn_list"]
                head = ",".join(str(p) for p in prns[:16])
                tail = "…" if len(prns) > 16 else ""
                result["PRN список"] = f"n={data['prn_no']}: {head}{tail}"

        elif command in ["query_obsvm", "query_obsvh", "query_obsvbase"]:
            # Таблица: одна строка на каждое наблюдение (спутник) со всеми полями.
            # Режим таблицы включаем всегда (в т.ч. binary при 0 наблюдений), иначе «Текущее сообщение»
            # не получает ту же сводку, что и у ASCII через populate_data_table.
            observations = data.get('observations', [])
            result['_table_mode'] = 'observations'
            result['observations'] = []
            for obs in observations:
                row = {}
                if "nav_system" in obs:
                    row["ГНСС"] = obs["nav_system"]
                if obs.get("signal_name"):
                    row["Сигнал"] = obs["signal_name"]
                if obs.get("system_freq_note"):
                    row["ГЛО k / UShort"] = obs["system_freq_note"]
                elif 'system_freq' in obs:
                    row["ГЛО k / UShort"] = str(obs["system_freq"])
                if 'prn' in obs:
                    row['PRN'] = obs['prn']
                if 'psr' in obs:
                    row['Псевдодальность, м'] = f"{obs['psr']:.3f}"
                if 'adr' in obs:
                    row['Фаза несущей (ADR), циклы'] = f"{obs['adr']:.3f}"
                if 'psr_std' in obs:
                    row['psr_std'] = f"{obs['psr_std']:.3f}"
                if 'adr_std' in obs:
                    row['adr_std'] = f"{obs['adr_std']:.4f}"
                if 'dopp' in obs:
                    row['Doppler, Гц'] = f"{obs['dopp']:.3f}"
                if 'cn0' in obs:
                    row['C/N0, dB-Hz'] = f"{obs['cn0']:.2f}"
                if 'locktime' in obs:
                    row['Lock time, сек'] = f"{obs['locktime']:.3f}"
                if 'ch_tr_status_hex' in obs:
                    row['ch_tr_status'] = obs['ch_tr_status_hex']
                result['observations'].append(row)
            if 'format' in data:
                result['Формат'] = data['format']
            if 'obs_number' in data:
                result['Число наблюдений'] = data['obs_number']

        elif command == "query_obsvmcmp":
            # Таблица: одна строка на каждую сжатую запись (как для OBSVM/OBSVH).
            compressed_records = data.get('compressed_records', [])
            result['_table_mode'] = 'observations'
            result['observations'] = []
            for rec in compressed_records:
                decoded = rec.get('decoded') if isinstance(rec.get('decoded'), dict) else {}
                row = {}
                if "nav_system" in decoded:
                    row["ГНСС"] = decoded["nav_system"]
                if decoded.get("signal_name"):
                    row["Сигнал"] = decoded["signal_name"]
                if 'prn' in decoded:
                    row['PRN'] = decoded['prn']
                if 'cn0_dbhz' in decoded:
                    row['C/N0, dB-Hz'] = f"{decoded['cn0_dbhz']:.2f}"
                if 'pseudorange_m' in decoded:
                    row['Псевдодальность, м'] = f"{decoded['pseudorange_m']:.3f}"
                if 'adr_cycles' in decoded:
                    row['Фаза несущей (ADR), циклы'] = f"{decoded['adr_cycles']:.3f}"
                if 'doppler_hz' in decoded:
                    row['Doppler, Гц'] = f"{decoded['doppler_hz']:.3f}"
                if 'lock_time_s' in decoded:
                    row['Lock time, сек'] = f"{decoded['lock_time_s']:.3f}"
                if 'psr_std_m' in decoded:
                    row['psr_std'] = f"{decoded['psr_std_m']:.3f}"
                if 'adr_std_cycles' in decoded:
                    row['adr_std'] = f"{decoded['adr_std_cycles']:.4f}"
                if 'raw_hex' in decoded:
                    row['Hex'] = decoded['raw_hex'][:48] + ("..." if len(decoded.get('raw_hex', '')) > 48 else "")
                result['observations'].append(row)
            if 'format' in data:
                result['Формат'] = data['format']
            if 'obs_number' in data:
                result['Число наблюдений'] = data['obs_number']
            if 'note' in data:
                result['Примечание'] = data['note']

        elif command == "query_config":
            if "format" in data:
                result["Формат"] = data["format"]
            for k, v in self.flatten_dict(data).items():
                if k not in result:
                    result[k] = v

        elif command == "query_mask":
            blocks = _mask_blocks(data)
            entries = blocks["entries"]
            n_rules = len(entries)
            result["Масок"] = n_rules
            for i, ent in enumerate(entries, 1):
                for k, v in _mask_entry_params(ent):
                    if v:
                        # Индекс сохраняем только в скрытом суффиксе ключа; в форме он не показывается.
                        result[f"{k}__{i}"] = v

        elif command == "query_hwstatus":
            if 'format' in data:
                result['Формат'] = data['format']
            if 'temp1_celsius' in data:
                result['Температура'] = f"{data['temp1_celsius']:.3f}°C"
            if 'dc09' in data:
                result['DC09'] = f"{data['dc09']:.3f} V"
            if 'dc10' in data:
                result['DC10'] = f"{data['dc10']:.3f} V"
            if 'dc18' in data:
                result['DC18'] = f"{data['dc18']:.3f} V"
            if 'clockflag_valid' in data:
                result['Clock Valid'] = 'Да' if data['clockflag_valid'] else 'Нет'
            if 'clock_drift' in data:
                result['Clock Drift'] = f"{data['clock_drift']:.6f} m/s"
            if 'hw_flag_hex' in data:
                result['Hardware Flag'] = data['hw_flag_hex']
            if 'pll_lock_hex' in data:
                result['PLL Lock'] = data['pll_lock_hex']
            if 'hw_flag_bits' in data:
                bits = data['hw_flag_bits']
                osc_type = 'Crystal' if bits.get('oscillator_type') else 'Oscillator'
                vcxo_tcxo = 'TCXO' if bits.get('vcxo_tcxo') else 'VCXO'
                freq = '20 MHz' if bits.get('osc_freq') else '26 MHz'
                result['Осциллятор'] = f"{osc_type}, {vcxo_tcxo}, {freq}"

        elif command == "query_agc":
            if 'format' in data:
                result['Формат'] = data['format']
            if 'master_antenna' in data:
                master = data['master_antenna']
                master_values = []
                if 'l1' in master and master['l1'] is not None:
                    master_values.append(f"L1={master['l1']}")
                if 'l2' in master and master['l2'] is not None:
                    master_values.append(f"L2={master['l2']}")
                if 'l5' in master and master['l5'] is not None:
                    master_values.append(f"L5={master['l5']}")
                if master_values:
                    result['Главная антенна'] = ", ".join(master_values)
            if 'slave_antenna' in data:
                slave = data['slave_antenna']
                slave_values = []
                if 'l1' in slave and slave['l1'] is not None:
                    slave_values.append(f"L1={slave['l1']}")
                if 'l2' in slave and slave['l2'] is not None:
                    slave_values.append(f"L2={slave['l2']}")
                if 'l5' in slave and slave['l5'] is not None:
                    slave_values.append(f"L5={slave['l5']}")
                if slave_values:
                    result['Ведомая антенна'] = ", ".join(slave_values)

        else:
            # Для остальных команд используем общий формат
            result = self.flatten_dict(data)

        return result

    def flatten_dict(self, data: dict, prefix: str = "", skip_fields: set = None) -> dict:
        """Рекурсивное разворачивание словаря в плоский формат"""
        if skip_fields is None:
            skip_fields = {'raw', 'raw_bytes', 'hex', 'raw_response', 'response'}

        result = {}
        for key, value in data.items():
            # Пропускаем raw данные
            if key.lower() in skip_fields:
                continue

            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                # Рекурсивно обрабатываем вложенные словари
                result.update(self.flatten_dict(value, full_key, skip_fields))
            elif isinstance(value, list):
                # Для списков показываем количество элементов
                if len(value) > 0:
                    # Если первый элемент - словарь, показываем структуру
                    if isinstance(value[0], dict):
                        result[f"{full_key}.count"] = len(value)
                        # Показываем ключи первого элемента
                        for sub_key in value[0].keys():
                            if sub_key.lower() not in skip_fields:
                                result[f"{full_key}.{sub_key}"] = f"[{len(value)} items]"
                    else:
                        result[full_key] = f"[{len(value)} items]"
                else:
                    result[full_key] = "[]"
            else:
                # Обычное значение
                value_str = str(value)
                # Пропускаем очень длинные hex строки
                if len(value_str) > 200 and all(c in '0123456789abcdefABCDEF' for c in value_str.replace(' ', '')):
                    continue
                if len(value_str) > 100:
                    value_str = value_str[:100] + "..."
                result[full_key] = value_str

        return result

    def _get_command_display_short(self, command: Optional[str]) -> str:
        if not command:
            return "—"
        if command.startswith("config_command:"):
            return command.split(":", 1)[1]
        for internal, short, _, _ in self.all_commands:
            if internal == command:
                return short
        return command.replace("query_", "").upper()

    def _clear_tree_data(self) -> None:
        for item in self.data_tree.get_children():
            self.data_tree.delete(item)
        self.data_rows = []
        self.data_columns = []
        self.data_tree["columns"] = ()

    def _fill_message_form(self, fields: Dict[str, Any], source_command: Optional[str] = None) -> None:
        for w in self.message_form_inner.winfo_children():
            w.destroy()
        if not fields:
            return
        row = 0
        for kind, value in build_form_rows(source_command, fields):
            if kind == "section":
                ttk.Label(
                    self.message_form_inner,
                    text=value,
                    font=("Arial", 11, "bold"),
                ).grid(
                    row=row,
                    column=0,
                    columnspan=2,
                    sticky=tk.W,
                    padx=(4, 0),
                    pady=(4, 4) if row == 0 else (10, 4),
                )
                row += 1
                continue
            key = value
            label_text = str(key)
            if "__" in label_text:
                label_text = label_text.split("__", 1)[0]
            ttk.Label(self.message_form_inner, text=label_text).grid(row=row, column=0, sticky=tk.NW, padx=(4, 8),
                                                                     pady=3)
            text = str(fields[key])
            if len(text) > 200 or "\n" in text:
                lines = min(14, max(3, text.count("\n") + 1))
                tb = tk.Text(
                    self.message_form_inner,
                    height=lines,
                    width=72,
                    wrap=tk.WORD,
                    font=("Arial", 11),
                    relief=tk.SOLID,
                    borderwidth=1,
                )
                tb.insert("1.0", text)
                tb.config(state=tk.DISABLED)
                tb.grid(row=row, column=1, sticky=tk.EW, pady=3)
            else:
                ent = ttk.Entry(self.message_form_inner)
                ent.insert(0, text)
                ent.config(state="readonly")
                ent.grid(row=row, column=1, sticky=tk.EW, pady=3)
            row += 1
        self.message_form_inner.columnconfigure(1, weight=1)
        self.message_form_canvas.update_idletasks()
        self.message_form_canvas.configure(scrollregion=self.message_form_canvas.bbox("all"))

    def populate_data_table(self, data: dict, source_command: Optional[str] = None) -> None:
        """
        Обновить «Текущее сообщение»: почти всегда — только форма полей (данные в реальном времени).
        Дополнительная таблица внизу — только для OBSVM/OBSVH/OBSVMCMP (много строк по спутникам).
        """
        if getattr(self, "_data_pane_hidden", False):
            return
        if not isinstance(data, dict):
            return
        try:
            if self.data_empty_label.winfo_viewable():
                self.data_empty_label.pack_forget()
        except Exception:
            pass

        short = self._get_command_display_short(source_command)
        ts = datetime.now().strftime("%H:%M:%S")
        self._message_title_var.set(f"{short}  ·  {ts}")

        if data.get("_table_mode") == "observations":
            observations = data.get("observations", [])
            meta = {k: v for k, v in data.items() if k not in ("_table_mode", "observations")}
            # OBSV: блочный вывод в «Текущее сообщение» (Наблюдение 1/2/…),
            # чтобы новые записи были видны сразу без таблицы.
            form_fields: Dict[str, Any] = {}
            if "Число наблюдений" in meta:
                form_fields["Число наблюдений"] = meta["Число наблюдений"]
            elif "obs_number" in meta:
                form_fields["Число наблюдений"] = meta["obs_number"]
            if "Формат" in meta:
                form_fields["Формат"] = meta["Формат"]
            elif "format" in meta:
                form_fields["Формат"] = meta["format"]
            if "Примечание" in meta:
                form_fields["Примечание"] = meta["Примечание"]
            if not observations:
                n = meta.get("Число наблюдений", meta.get("obs_number"))
                if n == 0 or n == "0":
                    form_fields.setdefault(
                        "Примечание",
                        "Наблюдения отсутствуют (возможно, нет спутников)",
                    )
                if form_fields:
                    self._fill_message_form(form_fields, source_command=source_command)
                    self.message_form_outer.pack(fill=tk.BOTH, expand=True, pady=(0, 4))
                self._clear_tree_data()
                try:
                    self.data_tree_frame.pack_forget()
                except Exception:
                    pass
                self._refresh_data_table_export_state()
                return
            for idx, obs in enumerate(observations, start=1):
                if not isinstance(obs, dict):
                    continue
                for k, v in obs.items():
                    form_fields[f"{k}__OBS{idx}"] = v
            if form_fields:
                self._fill_message_form(form_fields, source_command=source_command)
                self.message_form_outer.pack(fill=tk.BOTH, expand=True, pady=(0, 4))
            else:
                for w in self.message_form_inner.winfo_children():
                    w.destroy()
                self.message_form_outer.pack_forget()

            recv_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            tc = OBS_TABLE_TIME_COLUMN
            children_before = self.data_tree.get_children()
            n_before = len(children_before)

            all_keys: List[str] = []
            for obs in observations:
                for k in obs.keys():
                    if k not in all_keys:
                        all_keys.append(k)

            if tc not in self.data_columns:
                prev_cols = list(self.data_columns)
                self.data_columns = [tc] + prev_cols
                self.data_tree["columns"] = tuple(self.data_columns)
                self.data_tree.heading(tc, text=tc)
                self.data_tree.column(tc, width=168, anchor=tk.CENTER, stretch=tk.NO, minwidth=120)
                if children_before:
                    for child in children_before:
                        old_vals = list(self.data_tree.item(child, "values"))
                        tags = self.data_tree.item(child, "tags") or ()
                        mark = "" if "obs_sep" in tags else "—"
                        new_vals = [mark] + old_vals
                        while len(new_vals) < len(self.data_columns):
                            new_vals.append("")
                        self.data_tree.item(child, values=tuple(new_vals[: len(self.data_columns)]))
                    for row in self.data_rows:
                        if row.get("_separator"):
                            row[tc] = ""
                        else:
                            row[tc] = "—"

            new_columns = [c for c in all_keys if c not in self.data_columns]
            if new_columns:
                self.data_columns = list(self.data_columns) + new_columns
                self.data_tree["columns"] = tuple(self.data_columns)
                base_width = max(100, min(200, 800 // max(1, len(self.data_columns))))
                for col in new_columns:
                    self.data_tree.heading(col, text=col)
                    self.data_tree.column(col, width=base_width, anchor=tk.W, stretch=tk.YES, minwidth=80)
                for child in self.data_tree.get_children():
                    vals = list(self.data_tree.item(child, "values"))
                    if len(vals) < len(self.data_columns):
                        vals.extend([""] * (len(self.data_columns) - len(vals)))
                        self.data_tree.item(child, values=tuple(vals))
                for row in self.data_rows:
                    if row.get("_separator"):
                        continue
                    for col in new_columns:
                        if col not in row:
                            row[col] = ""

            batch_len = len(observations)
            for obs in reversed(observations):
                vals: List[str] = [recv_ts]
                for col in self.data_columns[1:]:
                    v = obs.get(col, "")
                    vals.append("" if v is None else str(v))
                while len(vals) < len(self.data_columns):
                    vals.append("")
                self.data_tree.insert("", 0, text="1", values=tuple(vals[: len(self.data_columns)]))
                row_copy = {k: v for k, v in obs.items()}
                row_copy[tc] = recv_ts
                self.data_rows.insert(0, row_copy)

            if n_before > 0 and self.data_columns:
                sep_vals = [""] * len(self.data_columns)
                sep_vals[0] = " ▼ более ранний приём (ниже по таблице) ▼ "
                self.data_tree.insert(
                    "",
                    batch_len,
                    text=" ",
                    values=tuple(sep_vals),
                    tags=("obs_sep",),
                )
                self.data_rows.insert(batch_len, {"_separator": True, tc: recv_ts})

            self._trim_data_table_to_limit()
            self._show_data_table()
            self._refresh_data_table_export_state()
            return

        self._clear_tree_data()
        flat_data = {k: v for k, v in data.items() if not k.startswith("_")}
        if not flat_data:
            self._refresh_data_table_export_state()
            return
        self.data_tree_frame.pack_forget()
        self.message_form_outer.pack(fill=tk.BOTH, expand=True)
        self._fill_message_form(flat_data, source_command=source_command)
        self._refresh_data_table_export_state()
        try:
            self.data_content_frame.update_idletasks()
        except Exception:
            pass

    def _show_data_table(self) -> None:
        """Показать таблицу наблюдений в отдельной панели."""
        try:
            if self.table_empty_label.winfo_viewable():
                self.table_empty_label.pack_forget()
            if not self.data_tree_frame.winfo_ismapped():
                self.data_tree_frame.pack(fill=tk.BOTH, expand=True)
            self.data_tree_frame.update_idletasks()
            self.table_content_frame.update_idletasks()
        except Exception:
            try:
                self.data_tree_frame.pack(fill=tk.BOTH, expand=True)
                self.data_tree_frame.update_idletasks()
                self.table_content_frame.update_idletasks()
            except Exception:
                pass

    def clear_results(self) -> None:
        """Очистка лога"""
        self.results_text.delete(1.0, tk.END)

    def on_closing(self) -> None:
        """Обработка закрытия окна"""
        if self._selection_fetch_after_id:
            self.root.after_cancel(self._selection_fetch_after_id)
            self._selection_fetch_after_id = None
        if self._data_output_resend_after_id:
            try:
                self.root.after_cancel(self._data_output_resend_after_id)
            except Exception:
                pass
            self._data_output_resend_after_id = None
        if self.device:
            self.disconnect_device()
        self.root.destroy()

    def export_results(self) -> None:
        """Экспорт результатов в файл"""
        content = self.results_text.get(1.0, tk.END)
        if not content.strip():
            messagebox.showwarning("Предупреждение", "Нет данных для экспорта")
            return

        filename = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )

        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content)
                messagebox.showinfo("Успех", f"Результаты экспортированы в {filename}")
            except Exception as e:
                messagebox.showerror("Ошибка", f"Не удалось сохранить файл: {str(e)}")


def main() -> None:
    root = tk.Tk()
    app = UM982GUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()

