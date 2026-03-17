import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import serial.tools.list_ports
from typing import Dict, Any, Optional, List
import json
import inspect
import threading
import queue
import time
from um982_uart import UM982UART
from um982.core import _is_tcp_port_spec
from um982.data_output.observation import extract_one_obsv_message
from um982_commands import get_command_names, get_command_definition

MAX_DATA_ROWS = 1000

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


class UM982GUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("UM982 Control Panel")
        self.root.geometry("1200x800")
        
        self.device: Optional[UM982UART] = None
        self.current_command = None
        self.param_widgets: Dict[str, tk.Widget] = {}
        self._result_queue: queue.Queue = queue.Queue()
        self._streaming_stop = threading.Event()
        self._streaming_active = False
        self._streaming_thread: Optional[threading.Thread] = None
        self._poll_after_id: Optional[str] = None
        
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
        baudrate_combo = ttk.Combobox(connection_frame, textvariable=self.baudrate_var, 
                                     values=["9600", "19200", "38400", "57600", "115200", "230400", "460800", "921600"],
                                     width=10, font=("Arial", 11))
        baudrate_combo.pack(side=tk.LEFT, padx=5)
        
        self.connect_btn = ttk.Button(connection_frame, text="Подключиться", command=self.connect_device)
        self.connect_btn.pack(side=tk.LEFT, padx=5)
        
        self.disconnect_btn = ttk.Button(connection_frame, text="Отключиться", command=self.disconnect_device, state=tk.DISABLED)
        self.disconnect_btn.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(connection_frame, text="Обновить порты", command=self.refresh_ports).pack(side=tk.LEFT, padx=5)
        
        self.status_label = ttk.Label(connection_frame, text="Не подключено", foreground="red")
        self.status_label.pack(side=tk.LEFT, padx=20)
        
        run_frame = ttk.Frame(self.root, padding="5")
        run_frame.pack(fill=tk.X, padx=10, pady=(5, 0))
        
        self.run_btn = ttk.Button(run_frame, text="▶ Запустить команду", command=self.run_command, state=tk.DISABLED)
        self.run_btn.pack(side=tk.RIGHT, padx=5)
        self.stream_live_btn = ttk.Button(run_frame, text="Запустить на лету", command=self.run_command_stream_live, state=tk.DISABLED)
        self.stream_live_btn.pack(side=tk.RIGHT, padx=5)
        self.stop_stream_btn = ttk.Button(run_frame, text="⏹ Остановить", command=self.stop_streaming, state=tk.DISABLED)
        self.stop_stream_btn.pack(side=tk.RIGHT, padx=5)
        
        main_container = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        left_frame = ttk.Frame(main_container)
        main_container.add(left_frame, weight=1)
        
        ttk.Label(left_frame, text="Команды", font=("Arial", 14, "bold")).pack(pady=5)
        
        category_frame = ttk.Frame(left_frame)
        category_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(category_frame, text="Категория:").pack(side=tk.LEFT, padx=(0, 5))
        self.category_var = tk.StringVar(value="MODE")
        self.category_combo = ttk.Combobox(category_frame, textvariable=self.category_var, 
                                           values=["MODE", "CONFIG", "Data Output", "MASK", "System"], 
                                           state="readonly", width=15, font=("Arial", 11))
        self.category_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.category_combo.bind("<<ComboboxSelected>>", lambda e: self.update_command_list())
        
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
        
        right_container = ttk.PanedWindow(main_container, orient=tk.VERTICAL)
        main_container.add(right_container, weight=2)
        
        self.params_frame_container = ttk.Frame(right_container)
        right_container.add(self.params_frame_container, weight=1)
        
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
        
        self.params_canvas.create_window((0, 0), window=self.params_scrollable_frame, anchor="nw")
        self.params_canvas.configure(yscrollcommand=self.params_scrollbar.set)
        
        self.params_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.params_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        def _on_mousewheel(event: tk.Event) -> None:
            self.params_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        self.params_canvas.bind_all("<MouseWheel>", _on_mousewheel)
        
        self.params_expanded = True
        self.params_content_frame = params_content_frame
        
        self.data_frame_container = ttk.Frame(right_container)
        right_container.add(self.data_frame_container, weight=1)
        
        self.data_header = ttk.Frame(self.data_frame_container)
        self.data_header.pack(fill=tk.X)
        
        self.data_toggle_btn = ttk.Button(self.data_header, text="▼ Данные", command=self.toggle_data)
        self.data_toggle_btn.pack(side=tk.LEFT, padx=5, pady=5)
        
        ttk.Button(self.data_header, text="Очистить таблицу", command=self.clear_data_table).pack(side=tk.LEFT, padx=5)
        
        self.data_content_frame = ttk.Frame(self.data_frame_container)
        self.data_content_frame.pack(fill=tk.BOTH, expand=True)
        
        self.data_tree_frame = ttk.Frame(self.data_content_frame)
        
        self.data_tree = ttk.Treeview(self.data_tree_frame, columns=(), show="headings", height=10)
        self.data_tree.heading("#0", text="#")
        self.data_tree.column("#0", width=40, anchor=tk.CENTER, stretch=tk.NO)
        
        self.data_scrollbar_x = ttk.Scrollbar(self.data_tree_frame, orient="horizontal", command=self.data_tree.xview)
        self.data_scrollbar_y = ttk.Scrollbar(self.data_tree_frame, orient="vertical", command=self.data_tree.yview)
        self.data_tree.configure(xscrollcommand=self.data_scrollbar_x.set, yscrollcommand=self.data_scrollbar_y.set)
        
        tree_center_frame = ttk.Frame(self.data_tree_frame)
        tree_center_frame.pack(expand=True, fill=tk.BOTH)
        
        tree_inner_frame = ttk.Frame(tree_center_frame)
        tree_inner_frame.pack(expand=False, anchor=tk.CENTER, pady=10)
        
        tree_horizontal = ttk.Frame(tree_inner_frame)
        tree_horizontal.pack()
        
        self.data_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.data_scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.data_scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X)
        
        self.data_columns = []
        self.data_rows = []
        self.data_expanded = True
        self.data_empty_label = ttk.Label(self.data_content_frame, 
                                         text="Таблица пуста. Выполните query команду для отображения данных.",
                                         foreground="gray", font=("Arial", 11))
        self.data_empty_label.pack(pady=20)
        self.log_frame_container = ttk.Frame(right_container)
        right_container.add(self.log_frame_container, weight=1)
        
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
            messagebox.showinfo("TCP", "Введите адрес в формате host:port или tcp://host:port\nНапример: localhost:5000")
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
                self._update_stream_live_button_state()
            else:
                messagebox.showerror("Ошибка", "Не удалось подключиться к устройству")
                self.device = None
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка подключения: {str(e)}")
            self.device = None
    
    def disconnect_device(self) -> None:
        if self.device:
            self.device.disconnect()
            self.device = None
        self.status_label.config(text="Не подключено", foreground="red")
        self.connect_btn.config(state=tk.NORMAL)
        self.disconnect_btn.config(state=tk.DISABLED)
        self.run_btn.config(state=tk.DISABLED)
        self.stream_live_btn.config(state=tk.DISABLED)
        self.log_result("Отключено от устройства\n")
    
    def load_commands(self) -> None:
        self.commands_by_category = {
            "MODE": [],
            "CONFIG": [],
            "Data Output": [],
            "MASK": [],
            "System": []
        }
        
        mode_commands = [
            ("query_mode", "MODE", "Запрос текущего режима"),
            ("set_mode_rover", "MODE ROVER", "Установить режим ROVER"),
            ("set_mode_base", "MODE BASE", "Установить режим BASE"),
            ("set_mode_base_time", "MODE BASE TIME", "Установить режим BASE с самооптимизацией"),
            ("set_mode_heading2", "MODE HEADING2", "Установить режим HEADING2"),
        ]
        self.commands_by_category["MODE"] = mode_commands
        
        config_commands = [
            ("query_config", "CONFIG", "Запрос конфигурации устройства"),
        ]
        
        try:
            config_names = get_command_names()
            for cmd_name in config_names:
                if cmd_name not in ["MASK", "UNMASK"]:
                    config_commands.append((
                        f"config_command:{cmd_name}",
                        f"CONFIG {cmd_name}",
                        f"Настройка {cmd_name}"
                    ))
        except Exception:
            # Игнорируем ошибки при загрузке команд из системы регистрации
            pass
        
        self.commands_by_category["CONFIG"] = config_commands
        
        dataoutput_commands = [
            ("query_version", "VERSION", "Запрос версии устройства"),
            ("query_obsvm", "OBSVM", "Наблюдения главной антенны"),
            ("query_obsvh", "OBSVH", "Наблюдения ведомой антенны"),
            ("query_obsvmcmp", "OBSVMCMP", "Сжатые наблюдения"),
            ("query_obsvbase", "OBSVBASE", "Наблюдения базовой станции"),
            ("query_baseinfo", "BASEINFO", "Информация о базовой станции"),
            ("query_gpsion", "GPSION", "Параметры ионосферы GPS"),
            ("query_bdsion", "BDSION", "Параметры ионосферы BDS"),
            ("query_bd3ion", "BD3ION", "Параметры ионосферы BDS-3"),
            ("query_galion", "GALION", "Параметры ионосферы Galileo"),
            ("query_gpsutc", "GPSUTC", "Перевод времени GPS в UTC"),
            ("query_bd3utc", "BD3UTC", "Перевод времени BDS-3 в UTC"),
            ("query_adrnav", "ADRNAV", "RTK позиция и скорость (главная антенна)"),
            ("query_adrnavh", "ADRNAVH", "RTK позиция и скорость (ведомая антенна)"),
            ("query_pppnav", "PPPNAV", "Позиция PPP решения"),
            ("query_sppnav", "SPPNAV", "SPP позиция и скорость (главная антенна)"),
            ("query_sppnavh", "SPPNAVH", "SPP позиция и скорость (ведомая антенна)"),
            ("query_stadop", "STADOP", "DOP для решения BESTNAV"),
            ("query_arddop", "ARDDOP", "DOP для ADRNAV"),
            ("query_arddoph", "ARDDOPH", "DOP для ADRNAVH (ведомая антенна)"),
            ("query_agric", "AGRIC", "AGRIC данные"),
            ("query_pvtsln", "PVTSLN", "PVTSLN данные"),
            ("query_uniloglist", "UNILOGLIST", "Список активных логов"),
            ("query_bestnav", "BESTNAV", "Лучшая позиция и скорость"),
            ("query_bestnavxyz", "BESTNAVXYZ", "Лучшая позиция и скорость (ECEF X,Y,Z)"),
            ("query_hwstatus", "HWSTATUS", "Статус оборудования"),
            ("query_agc", "AGC", "Автоматическая регулировка усиления"),
        ]
        self.commands_by_category["Data Output"] = dataoutput_commands
        
        mask_commands = [
            ("query_mask", "MASK", "Запрос конфигурации MASK"),
        ]
        
        try:
            config_names = get_command_names()
            for cmd_name in config_names:
                if cmd_name == "MASK":
                    mask_commands.append((
                        f"config_command:{cmd_name}",
                        f"MASK",
                        f"Настройка маскирования"
                    ))
                elif cmd_name == "UNMASK":
                    mask_commands.append((
                        f"config_command:{cmd_name}",
                        f"UNMASK",
                        f"Снятие маскирования"
                    ))
        except Exception:
            # Игнорируем ошибки при загрузке команд из системы регистрации
            pass
        
        self.commands_by_category["MASK"] = mask_commands
        
        system_commands = [
            ("restore_output", "Restore output", "Включить вывод снова после UNLOG (устройство снова отвечает)"),
            ("log", "LOG", "Включить вывод сообщения (BESTNAVA, GPGGA и т.д.) — противоположность UNLOG"),
            ("unlog", "UNLOG", "Остановить вывод сообщений (port, message)"),
            ("freset", "FRESET", "Очистить NVM и перезапуск (скорость → 115200)"),
            ("reset", "RESET", "Перезапуск с очисткой данных (EPHEM, ALMANAC, IONUTC, POSITION, XOPARAM, ALL)"),
            ("saveconfig", "SAVECONFIG", "Сохранить конфигурацию в NVM"),
        ]
        self.commands_by_category["System"] = system_commands
        
        self.all_commands = []
        for category, commands in self.commands_by_category.items():
            for cmd in commands:
                self.all_commands.append(cmd)
        
        self.update_command_list()
    
    def filter_commands(self, *args: Any) -> None:
        self.update_command_list()
    
    def update_command_list(self) -> None:
        self.command_listbox.delete(0, tk.END)
        
        selected_category = self.category_var.get()
        commands = self.commands_by_category.get(selected_category, [])
        
        filter_text = self.filter_var.get().lower()
        filtered_commands = [
            (cmd_id, display_name, description)
            for cmd_id, display_name, description in commands
            if not filter_text or filter_text in display_name.lower() or filter_text in description.lower()
        ]
        
        for cmd_id, display_name, description in filtered_commands:
            self.command_listbox.insert(tk.END, f"{display_name} - {description}")
        
        if self.current_command:
            for i, (cmd_id, _, _) in enumerate(filtered_commands):
                if cmd_id == self.current_command:
                    self.command_listbox.selection_set(i)
                    self.command_listbox.see(i)
                    self.command_listbox.activate(i)
                    break
    
    def on_command_select(self, event: tk.Event) -> None:
        selection = self.command_listbox.curselection()
        if not selection:
            return
        
        index = selection[0]
        selected_category = self.category_var.get()
        commands = self.commands_by_category.get(selected_category, [])
        
        filter_text = self.filter_var.get().lower()
        filtered_commands = [
            cmd for cmd in commands
            if not filter_text or filter_text in cmd[1].lower() or filter_text in cmd[2].lower()
        ]
        
        if index < len(filtered_commands):
            new_command = filtered_commands[index][0]
            if new_command != self.current_command:
                self.clear_data_table()
            self.current_command = new_command
            self.show_command_params()
            self._update_stream_live_button_state()
    
    def show_command_params(self) -> None:
        for widget in self.params_scrollable_frame.winfo_children():
            widget.destroy()
        self.param_widgets.clear()
        
        if not self.current_command:
            if self.params_expanded:
                self.toggle_params()
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
        has_params = widgets_after > widgets_before and len(self.param_widgets) > 0
        
        if not has_params and self.params_expanded:
            self.toggle_params()
        elif has_params and not self.params_expanded:
            self.toggle_params()
    
    def show_config_command_params(self, cmd_type: str) -> None:
        """Отображение параметров CONFIG команды"""
        try:
            cmd_def = get_command_definition(cmd_type)
            if not cmd_def:
                ttk.Label(self.params_scrollable_frame, text=f"Команда {cmd_type} не найдена").pack()
                return
            
            sig = inspect.signature(cmd_def.command_builder)

            if cmd_type == "COM":
                self.create_param_widget("port", "Порт", "COM1", ["COM1", "COM2", "COM3"])
                self.create_param_widget("baudrate", "Скорость", "115200", 
                                       ["9600", "19200", "38400", "57600", "115200", "230400", "460800", "921600"])
                self.create_param_widget("data_bits", "Биты данных", "8", ["8"])
                self.create_param_widget("parity", "Четность", "N", ["N", "E", "O"])
                self.create_param_widget("stop_bits", "Стоп-биты", "1", ["1", "2"])
            
            elif cmd_type == "PPS":
                self.create_param_widget("enable", "Режим", "DISABLE", ["DISABLE", "ENABLE", "ENABLE2", "ENABLE3"])
                self.create_param_widget("timeref", "Временная ссылка", "GPS", ["GPS", "BDS", "GAL", "GLO"])
                self.create_param_widget("polarity", "Полярность", "POSITIVE", ["POSITIVE", "NEGATIVE"])
                self.create_param_widget("width", "Ширина (мкс)", "500000")
                self.create_param_widget("period", "Период (мс)", "1000")
                self.create_param_widget("rf_delay", "RF задержка (нс)", "0")
                self.create_param_widget("user_delay", "Пользовательская задержка (нс)", "0")
            
            elif cmd_type == "DGPS":
                self.create_param_widget("timeout", "Таймаут (сек)", "300")
            
            elif cmd_type == "RTK":
                self.create_param_widget("subcommand", "Подкоманда", "TIMEOUT", 
                                       ["TIMEOUT", "RELIABILITY", "USER_DEFAULTS", "RESET", "DISABLE"])
                self.create_param_widget("timeout", "Таймаут (сек)", "600")
                self.create_param_widget("param1", "Параметр 1", "")
                self.create_param_widget("param2", "Параметр 2", "")
            
            elif cmd_type == "STANDALONE":
                self.create_param_widget("subcommand", "Подкоманда", "ENABLE", ["ENABLE", "DISABLE"])
                self.create_param_widget("latitude", "Широта", "")
                self.create_param_widget("longitude", "Долгота", "")
                self.create_param_widget("altitude", "Высота", "")
                self.create_param_widget("time", "Время (сек)", "")
            
            elif cmd_type == "HEADING":
                self.create_param_widget("subcommand", "Подкоманда", "FIXLENGTH",
                                       ["FIXLENGTH", "VARIABLELENGTH", "STATIC", "LOWDYNAMIC", "TRACTOR", 
                                        "LENGTH", "RELIABILITY", "OFFSET"])
                self.create_param_widget("param1", "Параметр 1", "")
                self.create_param_widget("param2", "Параметр 2", "")
                self.create_param_widget("heading_offset", "Смещение heading", "")
                self.create_param_widget("pitch_offset", "Смещение pitch", "")
            
            elif cmd_type == "SBAS":
                self.create_param_widget("subcommand", "Подкоманда", "ENABLE", ["ENABLE", "DISABLE", "TIMEOUT"])
                self.create_param_widget("mode", "Режим", "AUTO", ["AUTO", "WAAS", "GAGAN", "MSAS", "EGNOS", "SDCM", "BDS"])
                self.create_param_widget("timeout", "Таймаут (сек)", "600")
            
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
            
            elif cmd_type == "EVENT":
                self.create_param_widget("subcommand", "Подкоманда", "DISABLE", ["ENABLE", "DISABLE"])
                self.create_param_widget("polarity", "Полярность", "POSITIVE", ["POSITIVE", "NEGATIVE"])
                self.create_param_widget("tguard", "TGUARD (мс)", "4")
            
            elif cmd_type == "UNDULATION":
                # CONFIG UNDULATION [parameter]
                # AUTO (по умолчанию) или пользовательское значение разделения геоида (метры)
                self.create_param_widget("mode", "Режим", "AUTO", ["AUTO"])
                self.create_param_widget("separation", "Разделение геоида (м)", "")
            
            elif cmd_type == "SMOOTH":
                # CONFIG SMOOTH [computing_engine] [parameter]
                # computing_engine:
                #   - RTKHEIGHT <0-100 эпох>
                #   - HEADING   <0-100 эпох>
                #   - PSRVEL    enable|disable
                self.create_param_widget(
                    "computing_engine",
                    "Движок вычислений",
                    "RTKHEIGHT",
                    ["RTKHEIGHT", "HEADING", "PSRVEL"],
                )
                self.create_param_widget("parameter", "Параметр", "")
            
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
                self.create_param_widget("mask_type", "Тип маски", "", 
                                       ["", "PRN", "RTCMCNO", "CNO"])
                self.create_param_widget("elevation", "Угол маскирования (°)", "")
                self.create_param_widget("system", "Система", "", 
                                       ["", "GPS", "BDS", "GLO", "GAL", "QZSS", "IRNSS"])
                self.create_param_widget("frequency", "Частота", "", 
                                       ["", "L1", "L1CA", "L1C", "L2", "L2C", "L2P", "L5",
                                        "B1", "B2", "B3", "B1I", "B2I", "B3I", "BD3B1C", "BD3B2A", "BD3B2B",
                                        "R1", "R2", "R3", "E1", "E5A", "E5B", "E6C",
                                        "Q1", "Q2", "Q5", "Q1CA", "Q1C", "Q2C", "I5"])
                self.create_param_widget("prn_id", "PRN ID", "")
                self.create_param_widget("cno", "C/NO", "")
            
            elif cmd_type == "UNMASK":
                self.create_param_widget("system", "Система", "", 
                                       ["", "GPS", "BDS", "GLO", "GAL", "QZSS", "IRNSS"])
                self.create_param_widget("frequency", "Частота", "", 
                                       ["", "L1", "L1CA", "L1C", "L2", "L2C", "L2P", "L5",
                                        "B1", "B2", "B3", "B1I", "B2I", "B3I", "BD3B1C", "BD3B2A", "BD3B2B",
                                        "R1", "R2", "R3", "E1", "E5A", "E5B", "E6C",
                                        "Q1", "Q2", "Q5", "Q1CA", "Q1C", "Q2C", "I5"])
                # Для PRN размаскирования
                self.create_param_widget("prn_id", "PRN ID", "")
            
            else:
                ttk.Label(self.params_scrollable_frame, text=f"Параметры для {cmd_type} не определены").pack()
        
        except Exception as e:
            ttk.Label(self.params_scrollable_frame, text=f"Ошибка: {str(e)}").pack()
    
    def show_query_command_params(self, method_name: str) -> None:
        if method_name in ["query_obsvm", "query_obsvh", "query_obsvmcmp"]:
            self.create_param_widget("port", "Порт", "COM1", ["COM1", "COM2", "COM3"])
            self.create_param_widget("rate", "Частота (вывод устройства, 1 = раз в эпоху)", "1")
            self.create_param_widget("binary", "Бинарный формат", "False", ["True", "False"])
            ttk.Label(self.params_scrollable_frame, text="Для непрерывной записи нажмите кнопку «Запись на лету» в панели выше.", foreground="gray").pack(pady=4)
        
        elif method_name == "query_obsvbase":
            self.create_param_widget("port", "Порт", "COM1", ["COM1", "COM2", "COM3"])
            self.create_param_widget("trigger", "Триггер", "ONCHANGED", ["ONCHANGED"])
            self.create_param_widget("binary", "Бинарный формат", "False", ["True", "False"])
        
        elif method_name in ["query_baseinfo", "query_gpsion", "query_bdsion", "query_bd3ion",
                             "query_galion", "query_gpsutc", "query_bd3utc",
                             "query_adrnav", "query_adrnavh", "query_pppnav",
                             "query_sppnav", "query_sppnavh",
                             "query_stadop", "query_arddop", "query_arddoph"]:
            self.create_param_widget("rate", "Частота", "1")
            self.create_param_widget("trigger", "Триггер", "", ["", "ONCHANGED"])
            self.create_param_widget("binary", "Бинарный формат", "False", ["True", "False"])
        
        elif method_name == "query_agric":
            self.create_param_widget("port", "Порт", "", ["", "COM1", "COM2", "COM3"])
            self.create_param_widget("rate", "Частота", "1")
            self.create_param_widget("binary", "Бинарный формат", "False", ["True", "False"])
        
        elif method_name in ["query_pvtsln", "query_bestnav", "query_bestnavxyz"]:
            self.create_param_widget("rate", "Частота", "1")
            self.create_param_widget("binary", "Бинарный формат", "False", ["True", "False"])
        
        elif method_name in ["query_version", "query_config", "query_uniloglist", "query_mode"]:
            if method_name == "query_version":
                self.create_param_widget("binary", "Бинарный формат", "False", ["True", "False"])
    
    def show_mode_command_params(self, method_name: str) -> None:
        if method_name == "set_mode_rover":
            ttk.Label(self.params_scrollable_frame, 
                     text="Эта команда не требует параметров", 
                     foreground="gray").pack(pady=10)
        
        elif method_name == "set_mode_heading2":
            ttk.Label(self.params_scrollable_frame, 
                     text="Эта команда не требует параметров", 
                     foreground="gray").pack(pady=10)
        
        elif method_name == "set_mode_base":
            ttk.Label(self.params_scrollable_frame, 
                     text="Оставьте поля пустыми для режима BASE по умолчанию", 
                     foreground="gray").pack(pady=5)
            ttk.Label(self.params_scrollable_frame, 
                     text="Или заполните параметры для фиксированных координат:", 
                     foreground="gray").pack(pady=5)
            
            self.create_param_widget("station_id", "Station ID (0-4095)", "")
            
            ttk.Label(self.params_scrollable_frame, 
                     text="Geodetic координаты:", 
                     font=("Arial", 11, "bold")).pack(pady=(10, 2), anchor=tk.W)
            self.create_param_widget("lat", "Широта (-90 до 90)", "")
            self.create_param_widget("lon", "Долгота (-180 до 180)", "")
            self.create_param_widget("hgt", "Высота (м)", "")
            
            ttk.Label(self.params_scrollable_frame, 
                     text="ECEF координаты:", 
                     font=("Arial", 11, "bold")).pack(pady=(10, 2), anchor=tk.W)
            self.create_param_widget("x", "X (ECEF, м)", "")
            self.create_param_widget("y", "Y (ECEF, м)", "")
            self.create_param_widget("z", "Z (ECEF, м)", "")
        
        elif method_name == "set_mode_base_time":
            ttk.Label(self.params_scrollable_frame, 
                     text="Все параметры опциональны", 
                     foreground="gray").pack(pady=5)
            self.create_param_widget("station_id", "Station ID (0-4095)", "")
            self.create_param_widget("time", "Время (сек)", "60")
            self.create_param_widget("distance", "Расстояние (м, 0-10)", "")
    
    def show_system_command_params(self, command: str) -> None:
        if command == "restore_output":
            ttk.Label(self.params_scrollable_frame, text="Включить вывод BESTNAV (если устройство перестало отвечать после UNLOG)",
                     foreground="gray").pack(pady=2)
            self.create_param_widget("port", "Порт (пусто = текущий)", "", ["", "COM1", "COM2", "COM3"])
        elif command == "log":
            ttk.Label(self.params_scrollable_frame, text="Включить вывод сообщения (после FRESET или вместо UNLOG). Пример: BESTNAVA, GPGGA.",
                     foreground="gray").pack(pady=2)
            self.create_param_widget("message", "Сообщение", "BESTNAVA", ["BESTNAVA", "GPGGA", "GNGGA", "BESTNAVXYZB"])
            self.create_param_widget("port", "Порт (пусто = текущий)", "", ["", "COM1", "COM2", "COM3"])
            self.create_param_widget("rate", "Частота (1 = раз в эпоху)", "1")
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
        elif command in ("freset", "saveconfig"):
            ttk.Label(self.params_scrollable_frame, text="Параметры не требуются", foreground="gray").pack(pady=10)
    
    def create_param_widget(self, param_name: str, label: str, default: str = "", choices: Optional[List[str]] = None) -> None:
        frame = ttk.Frame(self.params_scrollable_frame)
        frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(frame, text=f"{label}:", width=25, anchor=tk.W).pack(side=tk.LEFT, padx=5)
        
        if choices:
            var = tk.StringVar(value=default)
            widget = ttk.Combobox(frame, textvariable=var, values=choices, width=30)
        else:
            var = tk.StringVar(value=default)
            widget = ttk.Entry(frame, textvariable=var, width=30)
        
        widget.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        self.param_widgets[param_name] = (var, widget)
    
    def get_params(self) -> Dict[str, Any]:
        params = {}
        for param_name, (var, widget) in self.param_widgets.items():
            value = var.get().strip()
            if value:
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
        
        run_stream = getattr(self, "_run_stream_requested", False)
        self._run_stream_requested = False
        try:
            params = self.get_params()
        except Exception as e:
            self.log_result(f"Ошибка параметров: {e}\n")
            return
        stream_commands = ("query_obsvm", "query_obsvh", "query_obsvmcmp")
        use_stream = run_stream and self.current_command in stream_commands
        
        self.log_result(f"\n{'='*70}\n")
        self.log_result(f"Выполнение: {self.current_command}" + (" (поток, обновление в реальном времени)" if use_stream else "") + "\n")
        self.log_result(f"{'='*70}\n\n")
        if params:
            self.log_result(f"Параметры: {json.dumps(params, ensure_ascii=False, indent=2)}\n\n")
        
        self.run_btn.config(state=tk.DISABLED, text="Выполняется...")
        try:
            if use_stream:
                self._streaming_active = True
                self._streaming_stop.clear()
                self.stop_stream_btn.config(state=tk.NORMAL)
                self.stream_live_btn.config(state=tk.DISABLED)
                self._streaming_thread = threading.Thread(
                    target=self._stream_worker,
                    args=(self.current_command, params),
                    daemon=True
                )
                self._streaming_thread.start()
            else:
                self._streaming_stop.clear()
                thread = threading.Thread(
                    target=self._oneshot_worker,
                    args=(self.current_command, params),
                    daemon=True
                )
                thread.start()
            self._schedule_poll()
        except Exception as e:
            self.log_result(f"Ошибка запуска: {e}\n")
            self.run_btn.config(state=tk.NORMAL, text="▶ Запустить команду")
            self.stop_stream_btn.config(state=tk.DISABLED)
    
    def _stream_worker(self, command: str, params: dict) -> None:
        """Воркер потока: один раз отправить команду, затем только читать с порта и парсить сообщения."""
        stream_type_map = {"query_obsvm": "obsvm", "query_obsvh": "obsvh", "query_obsvmcmp": "obsvmcmp"}
        stream_type = stream_type_map.get(command)
        data_key = stream_type
        if not stream_type:
            self._result_queue.put(("error", "Неизвестная потоковая команда"))
            self._result_queue.put(("stream_stopped",))
            return
        port = (params.get("port") or "COM1").strip()
        try:
            rate = int(params.get("rate") or 1)
        except (TypeError, ValueError):
            rate = 1
        binary = params.get("binary") in (True, "True", "1", "yes")
        if not self.device.send_obsv_stream_command(stream_type, port=port, rate=rate, binary=binary):
            self._result_queue.put(("error", "Не удалось отправить команду вывода"))
            self._result_queue.put(("stream_stopped",))
            return
        time.sleep(1.0)
        buffer = b""
        while not self._streaming_stop.is_set():
            try:
                chunk = self.device.read_response(timeout=0.5)
                if chunk:
                    buffer += chunk
                while True:
                    data, buffer = extract_one_obsv_message(buffer, stream_type, binary)
                    if data is None:
                        break
                    result = {data_key: data}
                    self._result_queue.put(("result", command, result, True))
            except Exception as e:
                self._result_queue.put(("error", str(e)))
        self._result_queue.put(("stream_stopped",))
    
    def _oneshot_worker(self, command: str, params: dict) -> None:
        """Воркер одного запроса: выполняет команду и кладёт результат в очередь."""
        try:
            if command.startswith("config_command:"):
                cmd_type = command.split(":")[1]
                result = self.device.config_command(cmd_type, **params)
            elif command == "query_config":
                result = self.device.query_config(use_lines=True, **params)
            elif command.startswith("query_"):
                method = getattr(self.device, command)
                result = method(**params)
            elif command.startswith("set_mode_"):
                method = getattr(self.device, command)
                if command in ["set_mode_rover", "set_mode_heading2"]:
                    result = method()
                elif command == "set_mode_base":
                    p = {k: v for k, v in params.items() if k in ["lat", "lon", "hgt", "station_id"] and v}
                    if not p and any(k in params for k in ["x", "y", "z"]):
                        p = {k: v for k, v in params.items() if k in ["x", "y", "z", "station_id"] and v}
                    result = method(**p) if p else method()
                elif command == "set_mode_base_time":
                    p = {k: v for k, v in params.items() if v}
                    result = method(**p) if p else method()
                else:
                    result = method(**params)
            elif command in ("restore_output", "log", "unlog", "freset", "reset", "saveconfig"):
                # System commands (раздел 8)
                if command == "restore_output":
                    p = params.get("port", "").strip() or None
                    result = self.device.restore_output(port=p)
                elif command == "log":
                    msg = (params.get("message") or "BESTNAVA").strip()
                    p = params.get("port", "").strip() or None
                    rate = params.get("rate")
                    if rate is not None and isinstance(rate, (int, float)):
                        rate = int(rate)
                    else:
                        rate = 1
                    result = self.device.log(message=msg, port=p, rate=rate)
                elif command == "unlog":
                    p = {k: v for k, v in params.items() if k in ("port", "message") and v}
                    result = self.device.unlog(**p)
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
            self._result_queue.put(("result", command, result, False))
        except Exception as e:
            self._result_queue.put(("error", str(e)))
        self._result_queue.put(("done",))
    
    def _schedule_poll(self) -> None:
        """Запуск периодической проверки очереди результатов."""
        if self._poll_after_id:
            self.root.after_cancel(self._poll_after_id)
        self._poll_queue()
    
    def _poll_queue(self) -> None:
        """Обработка очереди результатов (вызывается по таймеру из главного потока)."""
        try:
            while True:
                item = self._result_queue.get_nowait()
                if item[0] == "result":
                    _, command, result, is_stream = item
                    if "error" in result:
                        self.log_result(f"Ошибка: {result['error']}\n")
                    else:
                        if command.startswith("config_command:"):
                            formatted = self.format_config_result(result)
                        elif command.startswith("set_mode_"):
                            formatted = self.format_set_mode_result(result, command)
                        elif command in ("restore_output", "log", "unlog", "freset", "reset", "saveconfig"):
                            formatted = self._format_system_result(result)
                        else:
                            formatted = self.format_query_result(command, result)
                        if is_stream:
                            self.log_result(f"[ОБНОВЛЕНИЕ] {command}\n")
                        else:
                            self.log_result(formatted)
                        # В потоковом режиме новые данные добавляются в таблицу (без очистки), лимит MAX_DATA_ROWS
                        data_key_map = {
                            "query_mode": "mode", "query_version": "version", "query_config": "config",
                            "query_mask": "mask",
                            "query_obsvm": "obsvm", "query_obsvh": "obsvh", "query_obsvmcmp": "obsvmcmp",
                            "query_obsvbase": "obsvbase", "query_baseinfo": "baseinfo", "query_gpsion": "gpsion",
                            "query_bdsion": "bdsion", "query_bd3ion": "bd3ion", "query_galion": "galion",
                            "query_gpsutc": "gpsutc", "query_bd3utc": "bd3utc", "query_agric": "agric", "query_pvtsln": "pvtsln", "query_uniloglist": "uniloglist",
                            "query_bestnav": "bestnav", "query_bestnavxyz": "bestnavxyz", "query_hwstatus": "hwstatus", "query_agc": "agc",
                        }
                        data_key = data_key_map.get(command)
                        if data_key and data_key in result and "error" not in result:
                            data = result[data_key]
                            if data:
                                formatted_data = self.format_data_for_table(command, data)
                                self.populate_data_table(formatted_data)
                        elif command.startswith("config_command:") and "response" in result and "parsed" in result.get("response", {}):
                            parsed_data = result["response"]["parsed"]
                            if parsed_data:
                                self.populate_data_table(parsed_data)
                        # Уведомление для команд без ожидания ответа или с ответом "сообщение отправлено"
                        show_notification = (
                            not is_stream
                            and (result.get("sent_no_response") or result.get("message_sent_style"))
                        )
                        # MODE ROVER / HEADING2 часто не присылают подтверждение — показываем уведомление при любой успешной отправке
                        if not show_notification and not is_stream and "error" not in result:
                            if command in ("set_mode_rover", "set_mode_heading2"):
                                show_notification = True
                        if show_notification:
                            msg = self._notification_message_for_command(command, result)
                            messagebox.showinfo("Уведомление", msg)
                    if not is_stream:
                        self.run_btn.config(state=tk.NORMAL, text="▶ Запустить команду")
                elif item[0] == "error":
                    self.log_result(f"Исключение: {item[1]}\n")
                    self.run_btn.config(state=tk.NORMAL, text="▶ Запустить команду")
                elif item[0] == "done":
                    self.run_btn.config(state=tk.NORMAL, text="▶ Запустить команду")
                elif item[0] == "stream_stopped":
                    self._streaming_active = False
                    self.stop_stream_btn.config(state=tk.DISABLED)
                    self.run_btn.config(state=tk.NORMAL, text="▶ Запустить команду")
                    self._update_stream_live_button_state()
        except queue.Empty:
            pass
        self._poll_after_id = self.root.after(300, self._poll_queue)
    
    def stop_streaming(self) -> None:
        """Остановка потокового обновления OBSVM/OBSVH/OBSVMCMP."""
        self._streaming_stop.set()
        self.stop_stream_btn.config(state=tk.DISABLED)
    
    def _update_stream_live_button_state(self) -> None:
        """Включить кнопку «Запись на лету» только для query_obsvm/obsvh/obsvmcmp при подключённом устройстве."""
        stream_commands = ("query_obsvm", "query_obsvh", "query_obsvmcmp")
        connected = self.device is not None
        try:
            if self.device and hasattr(self.device, "serial_conn") and self.device.serial_conn:
                connected = getattr(self.device.serial_conn, "is_open", False)
        except Exception:
            connected = False
        can_stream = connected and not self._streaming_active and self.current_command in stream_commands
        self.stream_live_btn.config(state=tk.NORMAL if can_stream else tk.DISABLED)
    
    def run_command_stream_live(self) -> None:
        """Запуск потоковой записи (запись на лету) для OBSVM/OBSVH/OBSVMCMP."""
        self._run_stream_requested = True
        self.run_command()
    
    def format_query_result(self, command: str, result: dict) -> str:
        """Форматирование результата query команды"""
        output = []
        
        # Определяем ключ данных на основе команды
        data_key_map = {
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
            "query_arddop": "arddop",
            "query_arddoph": "arddoph",
            "query_agric": "agric",
            "query_pvtsln": "pvtsln",
            "query_uniloglist": "uniloglist",
            "query_bestnav": "bestnav",
            "query_bestnavxyz": "bestnavxyz",
            "query_hwstatus": "hwstatus",
            "query_agc": "agc",
        }
        
        data_key = data_key_map.get(command)
        if not data_key or data_key not in result:
            return "Данные не получены\n"
        
        data = result[data_key]
        if not data:
            return "Данные не распарсены\n"
        
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
        elif command == "query_arddop":
            return self._format_generic_data("ARDDOP", data)
        elif command == "query_arddoph":
            return self._format_generic_data("ARDDOPH", data)
        elif command == "query_bestnavxyz":
            return self._format_generic_data("BESTNAVXYZ", data)
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
        """Форматирование данных MASK: явно показать, наложены маски или нет."""
        entries = data.get("entries", [])
        elevation_masks = data.get("elevation_masks", [])
        system_masks = data.get("system_masks", [])
        prn_masks = data.get("prn_masks", {})

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

        output = [
            "MASK (конфигурация масок):",
            "  Маски наложены (найдено записей: %d)." % len(entries),
            "",
        ]
        if elevation_masks:
            uniq_el = sorted(set(elevation_masks))
            output.append("  Порог по углу места (град): %s" % ", ".join(f"{v:.3f}" for v in uniq_el))
        if system_masks:
            uniq_sys = sorted(set(system_masks))
            output.append("  Маски по системам: %s" % ", ".join(uniq_sys))
        if prn_masks:
            output.append("  Маски по PRN:")
            for system in sorted(prn_masks.keys()):
                prns = prn_masks[system]
                output.append("    %s: %s" % (system, ", ".join(str(p) for p in sorted(prns))))
        output.append("")
        output.append("  Ответ приёмника (сырые строки):")
        for e in entries:
            raw = e.get("raw", "")
            if raw:
                output.append("    %s" % raw)
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
        output = ["Лучшая позиция и скорость:"]
        if 'position' in data:
            pos = data['position']
            if 'status' in pos:
                output.append(f"  Статус позиции: {pos['status']}")
            if 'type' in pos:
                output.append(f"  Тип: {pos['type']}")
            if 'lat' in pos and 'lon' in pos and 'hgt' in pos:
                output.append(f"  Координаты: Lat={pos['lat']:.9f}, Lon={pos['lon']:.9f}, Hgt={pos['hgt']:.3f}")
        if 'velocity' in data:
            vel = data['velocity']
            if 'status' in vel:
                output.append(f"  Статус скорости: {vel['status']}")
            if 'north' in vel and 'east' in vel and 'up' in vel:
                output.append(f"  Скорость: N={vel['north']:.3f}, E={vel['east']:.3f}, U={vel['up']:.3f}")
        if 'format' in data:
            output.append(f"  Формат: {data['format']}")
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
                output.append(f"    Поддержка: {'Oscillator + Crystal' if bits.get('osc_crystal_support') else 'Только Oscillator'}")
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
        """Форматирование данных AGRIC"""
        output = ["AGRIC данные:"]
        if 'position_status' in data:
            output.append(f"  Статус позиции: {data['position_status']}")
        if 'heading_status' in data:
            output.append(f"  Статус heading: {data['heading_status']}")
        if 'rover_position' in data:
            rp = data['rover_position']
            if 'lat' in rp and 'lon' in rp and 'hgt' in rp:
                output.append(f"  Позиция Rover: Lat={rp['lat']:.9f}, Lon={rp['lon']:.9f}, Hgt={rp['hgt']:.3f}")
        if 'baseline' in data:
            bl = data['baseline']
            if 'north' in bl and 'east' in bl and 'up' in bl:
                output.append(f"  Baseline: N={bl['north']:.3f}, E={bl['east']:.3f}, U={bl['up']:.3f}")
        if 'heading' in data:
            h = data['heading']
            if 'degree' in h:
                output.append(f"  Heading: {h['degree']:.2f}°")
        if 'format' in data:
            output.append(f"  Формат: {data['format']}")
        return "\n".join(output) + "\n"
    
    def _format_pvtsln_data(self, data: dict) -> str:
        """Форматирование данных PVTSLN"""
        output = ["PVTSLN данные:"]
        if 'bestpos' in data:
            bp = data['bestpos']
            if 'type' in bp:
                output.append(f"  Тип позиции: {bp['type']}")
            if 'lat' in bp and 'lon' in bp and 'hgt' in bp:
                output.append(f"  Лучшая позиция: Lat={bp['lat']:.9f}, Lon={bp['lon']:.9f}, Hgt={bp['hgt']:.3f}")
        if 'heading' in data:
            h = data['heading']
            if 'degree' in h:
                output.append(f"  Heading: {h['degree']:.2f}°")
        if 'velocity' in data:
            v = data['velocity']
            if 'north' in v and 'east' in v:
                output.append(f"  Скорость: N={v['north']:.3f}, E={v['east']:.3f}")
        if 'format' in data:
            output.append(f"  Формат: {data['format']}")
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
            output.append(f"\n  Система/Частота: индекс сигнала (0=L1, 1=L2, 2=L5, 3=B1, …). ADR — накопленные циклы фазы с момента захвата (отрицательные и большие значения нормальны).")
            output.append(f"\n  Примеры наблюдений (показано {min(5, len(observations))} из {len(observations)}):")
            for i, obs in enumerate(observations[:5]):
                output.append(f"\n  Наблюдение {i+1}:")
                if 'system_freq' in obs:
                    output.append(f"    Система/Частота: {_system_freq_str(obs['system_freq'])}")
                if 'prn' in obs:
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
            output.append(f"\n  Сжатые записи (показано {min(3, len(compressed_records))} из {len(compressed_records)}):")
            for i, record in enumerate(compressed_records[:3]):
                output.append(f"\n  Запись {i+1}:")
                if 'raw_hex' in record:
                    hex_str = record['raw_hex']
                    if len(hex_str) > 32:
                        output.append(f"    Hex: {hex_str[:32]}... ({len(hex_str)//2} байт)")
                    else:
                        output.append(f"    Hex: {hex_str} ({len(hex_str)//2} байт)")
                dec = record.get('decoded')
                if dec and 'decode_error' not in dec:
                    parts = []
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
                                    # Ищем NMEA сообщения в ASCII (начинаются с $)
                                    if '$' in ascii_preview:
                                        # Ищем все строки, начинающиеся с $
                                        nmea_lines = []
                                        for line in ascii_preview.split('\n'):
                                            line = line.strip()
                                            # Ищем начало NMEA сообщения ($)
                                            if '$' in line:
                                                # Извлекаем только часть после $
                                                dollar_idx = line.find('$')
                                                nmea_part = line[dollar_idx:]
                                                # Очищаем от непечатных символов в начале
                                                nmea_part = ''.join(c if 32 <= ord(c) <= 126 or c in '\r\n' else '' for c in nmea_part)
                                                if nmea_part.startswith('$') and len(nmea_part) > 5:
                                                    nmea_lines.append(nmea_part)
                                        
                                        if nmea_lines:
                                            output.append(f"  Найдены NMEA сообщения в payload (первые 3):")
                                            for line in nmea_lines[:3]:
                                                # Очищаем строку от мусора
                                                clean_line = line.strip()
                                                if clean_line:
                                                    output.append(f"    {clean_line[:70]}{'...' if len(clean_line) > 70 else ''}")
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
            
            # Проверяем наличие NMEA сообщений
            if 'messages' in data:
                messages = data['messages']
                if isinstance(messages, list):
                    output.append(f"  NMEA сообщений: {len(messages)}")
                    
                    # Ищем CONFIG сообщения (это главное, что должно быть в ответе на CONFIG)
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
                        remaining_config = [m for m in config_messages if not any(kw in (m.get('raw', '') if isinstance(m, dict) else str(m)) for kw in found_important)]
                        if remaining_config:
                            output.append(f"\n  Другие CONFIG сообщения ({len(remaining_config)}):")
                            for i, msg in enumerate(remaining_config):  # Показываем все сообщения
                                if isinstance(msg, dict):
                                    raw = msg.get('raw', '')
                                    output.append(f"    {i+1}. {raw[:75]}{'...' if len(raw) > 75 else ''}")
                                else:
                                    output.append(f"    {i+1}. {str(msg)[:75]}{'...' if len(str(msg)) > 75 else ''}")
                    else:
                        output.append(f"\n  ⚠ CONFIG сообщения НЕ найдены!")
                        output.append(f"     Ожидаются сообщения типа: $CONFIG,COM1, $CONFIG,COM2, $CONFIG,COM3, $CONFIG,PPS")
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
                            output.append(f"\n  Другие NMEA сообщения (типы):")
                            for msg_type, count in sorted(msg_types.items(), key=lambda x: x[1], reverse=True)[:10]:
                                output.append(f"    {msg_type}: {count}")
                            if len(msg_types) > 10:
                                output.append(f"    ... и еще {len(msg_types) - 10} типов")
                            
                            # Показываем примеры других сообщений
                            output.append(f"\n  Примеры других сообщений (первые 3):")
                            for i, msg in enumerate(other_messages[:3]):
                                if isinstance(msg, dict):
                                    msg_type = msg.get('type', 'UNKNOWN')
                                    raw = msg.get('raw', '')
                                    if raw:
                                        output.append(f"    {i+1}. {msg_type}: {raw[:60]}{'...' if len(raw) > 60 else ''}")
                                    else:
                                        output.append(f"    {i+1}. {msg_type}: {str(msg)[:60]}{'...' if len(str(msg)) > 60 else ''}")
                                elif isinstance(msg, str):
                                    output.append(f"    {i+1}. {msg[:60]}{'...' if len(msg) > 60 else ''}")
            
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
                output.append(f"    {i+1}. {line[:80]}{'...' if len(line) > 80 else ''}")
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
        """Форматирование результата System команд (restore_output, unlog, freset, reset, saveconfig)"""
        output = []
        if "error" in result:
            output.append(f"Ошибка: {result['error']}")
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
        elif "command" in result:
            output.append(f"Отправлена команда: {result['command']}")
            if result.get("note"):
                output.append(result["note"])
        else:
            output.append("Статус неизвестен")
        return "\n".join(output) + "\n"
    
    def _notification_message_for_command(self, command: str, result: dict) -> str:
        """Текст уведомления для команд без ответа (messagebox)."""
        if command == "set_mode_rover":
            return "Режим ROVER установлен.\nКоманда отправлена."
        if command == "set_mode_heading2":
            return "Режим HEADING2 установлен.\nКоманда отправлена."
        return "Команда отправлена"

    def format_set_mode_result(self, result: dict, command: str = "") -> str:
        """Форматирование результата set_mode команды (лог)."""
        output = []
        if "error" in result:
            output.append(f"Ошибка: {result['error']}")
        elif command in ("set_mode_rover", "set_mode_heading2"):
            mode_name = "ROVER" if command == "set_mode_rover" else "HEADING2"
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
    
    def toggle_data(self) -> None:
        """Сворачивание/разворачивание панели данных"""
        if self.data_expanded:
            self.data_content_frame.pack_forget()
            self.data_toggle_btn.config(text="▶ Данные")
            self.data_expanded = False
        else:
            self.data_content_frame.pack(fill=tk.BOTH, expand=True)
            self.data_toggle_btn.config(text="▼ Данные")
            self.data_expanded = True
    
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
    
    def clear_data_table(self) -> None:
        """Очистка таблицы данных"""
        for item in self.data_tree.get_children():
            self.data_tree.delete(item)
        self.data_rows = []
        self.data_columns = []
        # Очищаем столбцы
        self.data_tree['columns'] = ()
        # Скрываем таблицу и показываем сообщение о пустой таблице
        try:
            self.data_tree_frame.pack_forget()
            self.data_empty_label.pack(pady=20)
        except:
            pass

    def _trim_data_table_to_limit(self) -> None:
        """Оставляем в таблице не более MAX_DATA_ROWS записей (удаляем старые)."""
        while len(self.data_rows) > MAX_DATA_ROWS:
            self.data_rows.pop(0)
            children = self.data_tree.get_children()
            if children:
                self.data_tree.delete(children[0])
        # Перенумеровываем колонку №
        for i, item_id in enumerate(self.data_tree.get_children(), 1):
            self.data_tree.item(item_id, text=str(i))
    
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
        
        elif command == "query_gpsion":
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
                    result['Sigma (lat,lon,hgt)'] = f"{pos['lat_sigma']:.3f}, {pos['lon_sigma']:.3f}, {pos['hgt_sigma']:.3f}"
            if 'velocity' in data:
                vel = data['velocity']
                if 'hor_speed' in vel and 'track_ground' in vel and 'vert_speed' in vel:
                    result['Скорость'] = f"Vh={vel['hor_speed']:.3f}, Trk={vel['track_ground']:.2f}, Vv={vel['vert_speed']:.3f}"
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
                    result['Sigma (lat,lon,hgt)'] = f"{pos['lat_sigma']:.3f}, {pos['lon_sigma']:.3f}, {pos['hgt_sigma']:.3f}"
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
                    result['Sigma (lat,lon,hgt)'] = f"{pos['lat_sigma']:.3f}, {pos['lon_sigma']:.3f}, {pos['hgt_sigma']:.3f}"
            if 'velocity' in data:
                vel = data['velocity']
                if 'hor_speed' in vel and 'track_ground' in vel and 'vert_speed' in vel:
                    result['Скорость'] = f"Vh={vel['hor_speed']:.3f}, Trk={vel['track_ground']:.2f}, Vv={vel['vert_speed']:.3f}"
        
        elif command == "query_stadop" or command == "query_arddop" or command == "query_arddoph":
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
                    result['P sol status'] = pos['P_sol_status']
                if 'pos_type' in pos:
                    result['Тип позиции'] = pos['pos_type']
                if 'P_X' in pos and 'P_Y' in pos and 'P_Z' in pos:
                    result['Позиция ECEF'] = f"X={pos['P_X']:.3f}, Y={pos['P_Y']:.3f}, Z={pos['P_Z']:.3f}"
                if 'P_X_sigma' in pos:
                    result['Sigma X,Y,Z'] = f"{pos.get('P_X_sigma', 0):.4f}, {pos.get('P_Y_sigma', 0):.4f}, {pos.get('P_Z_sigma', 0):.4f}"
            if 'velocity' in data:
                vel = data['velocity']
                if 'V_sol_status' in vel:
                    result['V sol status'] = vel['V_sol_status']
                if 'vel_type' in vel:
                    result['Тип скорости'] = vel['vel_type']
                if 'V_X' in vel and 'V_Y' in vel and 'V_Z' in vel:
                    result['Скорость ECEF'] = f"Vx={vel['V_X']:.4f}, Vy={vel['V_Y']:.4f}, Vz={vel['V_Z']:.4f}"
            if 'metadata' in data:
                meta = data['metadata']
                if 'station_id' in meta:
                    result['Station ID'] = meta['station_id']
                if 'num_sats_used' in meta:
                    result['Спутников в решении'] = meta['num_sats_used']
        
        elif command == "query_agric":
            if 'position_status' in data:
                result['Статус позиции'] = data['position_status']
            if 'heading_status' in data:
                result['Статус heading'] = data['heading_status']
            if 'rover_position' in data:
                rp = data['rover_position']
                if 'lat' in rp and 'lon' in rp and 'hgt' in rp:
                    result['Позиция Rover'] = f"Lat={rp['lat']:.9f}, Lon={rp['lon']:.9f}, Hgt={rp['hgt']:.3f}"
            if 'baseline' in data:
                bl = data['baseline']
                if 'north' in bl and 'east' in bl and 'up' in bl:
                    result['Baseline'] = f"N={bl['north']:.3f}, E={bl['east']:.3f}, U={bl['up']:.3f}"
            if 'heading' in data:
                h = data['heading']
                if 'degree' in h:
                    result['Heading'] = f"{h['degree']:.2f}°"
        
        elif command == "query_pvtsln":
            if 'bestpos' in data:
                bp = data['bestpos']
                if 'type' in bp:
                    result['Тип позиции'] = bp['type']
                if 'lat' in bp and 'lon' in bp and 'hgt' in bp:
                    result['Лучшая позиция'] = f"Lat={bp['lat']:.9f}, Lon={bp['lon']:.9f}, Hgt={bp['hgt']:.3f}"
            if 'heading' in data:
                h = data['heading']
                if 'degree' in h:
                    result['Heading'] = f"{h['degree']:.2f}°"
            if 'velocity' in data:
                v = data['velocity']
                if 'north' in v and 'east' in v:
                    result['Скорость'] = f"N={v['north']:.3f}, E={v['east']:.3f}"
        
        elif command in ["query_obsvm", "query_obsvh", "query_obsvbase"]:
            # Таблица: одна строка на каждое наблюдение (спутник) со всеми полями
            observations = data.get('observations', [])
            if observations:
                result['_table_mode'] = 'observations'
                result['observations'] = []
                for obs in observations:
                    row = {}
                    if 'system_freq' in obs:
                        row['Система/Частота'] = _system_freq_str(obs['system_freq'])
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
                result['obs_number'] = data['obs_number']
        
        elif command == "query_obsvmcmp":
            # Таблица: одна строка на каждую сжатую запись (как для OBSVM/OBSVH)
            compressed_records = data.get('compressed_records', [])
            if compressed_records:
                result['_table_mode'] = 'observations'
                result['observations'] = []
                for rec in compressed_records:
                    decoded = rec.get('decoded') if isinstance(rec.get('decoded'), dict) else {}
                    row = {}
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
                result['Количество наблюдений'] = data['obs_number']
            if 'note' in data:
                result['Примечание'] = data['note']
        
        elif command == "query_config":
            # Для CONFIG используем специальную обработку
            if 'format' in data:
                result['Формат'] = data['format']
            # Остальные поля CONFIG обрабатываются в _format_config_data

        elif command == "query_mask":
            entries = data.get("entries", [])
            if entries:
                result["Строк MASK"] = len(entries)
            mask_lines = data.get("mask_lines", [])
            if mask_lines:
                for i, line in enumerate(mask_lines[:20], 1):
                    result[f"MASK {i}"] = line[:80] + ("..." if len(line) > 80 else "")
        
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
    
    def populate_data_table(self, data: dict) -> None:
        """Заполнение таблицы данных (строки = выполнения или наблюдения, столбцы = поля)"""
        if not isinstance(data, dict):
            return
        
        # Режим "наблюдения": одна строка на каждый спутник, все поля в столбцах
        if data.get('_table_mode') == 'observations':
            observations = data.get('observations', [])
            if not observations:
                return
            # Собираем все столбцы из всех строк (порядок полей фиксированный для единообразия)
            all_keys = []
            for obs in observations:
                for k in obs.keys():
                    if k not in all_keys:
                        all_keys.append(k)
            columns = all_keys
            # Обновляем data_columns если нужно
            new_columns = [c for c in columns if c not in self.data_columns]
            if new_columns:
                self.data_columns = list(self.data_columns) + new_columns
                self.data_tree['columns'] = tuple(self.data_columns)
                base_width = max(100, min(200, 800 // max(1, len(self.data_columns))))
                for col in new_columns:
                    self.data_tree.heading(col, text=col)
                    self.data_tree.column(col, width=base_width, anchor=tk.W, stretch=tk.YES, minwidth=80)
            # Добавляем по одной строке на каждое наблюдение
            for idx, obs in enumerate(observations, 1):
                values = [obs.get(col, "") for col in self.data_columns]
                self.data_tree.insert("", "end", text=str(idx), values=values)
                self.data_rows.append(obs)
            self._trim_data_table_to_limit()
            self._show_data_table()
            return
        
        # Обычный режим: одна строка на выполнение команды
        flat_data = data
        if not flat_data:
            return
        
        # Исключаем служебные ключи из отображения
        flat_data = {k: v for k, v in flat_data.items() if not k.startswith('_')}
        if not flat_data:
            return
        
        # Определяем новые столбцы - проверяем ВСЕ ключи из текущих данных
        new_columns = []
        for key in flat_data.keys():
            if key not in self.data_columns:
                new_columns.append(key)
        
        # Также проверяем все столбцы из предыдущих строк, чтобы не потерять их
        for row_data in self.data_rows:
            for key in row_data.keys():
                if key not in self.data_columns:
                    new_columns.append(key)
        
        # Убираем дубликаты и сортируем для консистентности
        new_columns = sorted(list(set(new_columns)))
        
        # Добавляем новые столбцы
        if new_columns:
            for col in new_columns:
                self.data_columns.append(col)
            
            # Обновляем все столбцы в Treeview
            self.data_tree['columns'] = tuple(self.data_columns)
            
            # Настраиваем заголовки и ширину для новых столбцов
            num_cols = len(self.data_columns)
            base_width = max(100, min(200, 800 // max(1, num_cols))) if num_cols > 0 else 150
            
            for col in new_columns:
                self.data_tree.heading(col, text=col)
                self.data_tree.column(col, width=base_width, anchor=tk.W, stretch=tk.YES, minwidth=80)
            
            # Обновляем все существующие строки, добавляя пустые значения для новых столбцов
            for item_id in self.data_tree.get_children():
                current_values = list(self.data_tree.item(item_id, 'values'))
                while len(current_values) < len(self.data_columns):
                    current_values.append("")
                self.data_tree.item(item_id, values=current_values)
        
        self._show_data_table()
        
        # Добавляем новую строку со значениями для всех столбцов
        row_num = len(self.data_rows) + 1
        values = [flat_data.get(col, "") for col in self.data_columns]
        self.data_tree.insert("", "end", text=str(row_num), values=values)
        self.data_rows.append(flat_data)
        self._trim_data_table_to_limit()
    
    def _show_data_table(self) -> None:
        """Показать таблицу данных и скрыть сообщение о пустой таблице"""
        try:
            if self.data_empty_label.winfo_viewable():
                self.data_empty_label.pack_forget()
            if not self.data_tree_frame.winfo_ismapped():
                self.data_tree_frame.pack(fill=tk.BOTH, expand=True)
            self.data_tree_frame.update_idletasks()
            self.data_content_frame.update_idletasks()
        except Exception:
            try:
                self.data_tree_frame.pack(fill=tk.BOTH, expand=True)
                self.data_tree_frame.update_idletasks()
                self.data_content_frame.update_idletasks()
            except Exception:
                pass
    
    def clear_results(self) -> None:
        """Очистка лога"""
        self.results_text.delete(1.0, tk.END)
    
    def on_closing(self) -> None:
        """Обработка закрытия окна"""
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

