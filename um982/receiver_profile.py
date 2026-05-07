"""
Снимок настроек приёмника (CONFIG / MASK / LOG) в JSON и сравнение с текущим состоянием.

Используется GUI для выгрузки/загрузки профиля без блокировки основного потока
(запросы к устройству выполняются в воркере с внешней блокировкой serial).
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

from um982.utils import format_log_period_wire, parse_log_period_str

if TYPE_CHECKING:
    from um982_uart import UM982UART

PROFILE_SCHEMA_VERSION = 1
DEFAULT_PROFILES_DIRNAME = "receiver_profiles"


def default_profiles_dir(project_root: Optional[Path] = None) -> Path:
    root = project_root if project_root is not None else Path(__file__).resolve().parent.parent
    d = root / DEFAULT_PROFILES_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _norm_cmd(s: str) -> str:
    return " ".join(str(s).strip().split()).upper()


def stanza_from_config_raw(raw: str) -> Optional[str]:
    """Из строки ответа CONFIG (…$CONFIG,…) извлечь текст команды для повторной отправки."""
    line = str(raw).strip()
    if not line.startswith("$CONFIG,"):
        return None
    up = line.upper()
    if ",MASK," in up:
        return None
    parts = line.split(",", 2)
    if len(parts) < 3:
        return None
    payload = parts[2].split("*", 1)[0].strip()
    return payload or None


def mask_wire_command_from_config_raw(raw: str) -> Optional[str]:
    """Из $CONFIG,MASK,… сделать строку MASK/UNMASK для отправки (как в um982_commands)."""
    line = str(raw).strip()
    if ",MASK," not in line.upper() and not line.upper().startswith("$CONFIG,MASK,"):
        return None
    parts = line.split(",", 2)
    if len(parts) < 3:
        return None
    payload = parts[2].split("*", 1)[0].strip()
    if not payload:
        return None
    if payload.upper().startswith("MASK "):
        return payload
    m = re.match(r"([A-Za-z0-9]+)MaskPrn:(\d+)", payload)
    if m:
        return f"MASK {m.group(1).upper()} PRN {int(m.group(2))}"
    return None


def extract_config_set_commands(config_result: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    if not isinstance(config_result, dict) or config_result.get("error"):
        return out
    for msg in config_result.get("parsed", {}).get("messages", []) or []:
        if not isinstance(msg, dict):
            continue
        raw = msg.get("raw")
        if not raw:
            continue
        st = stanza_from_config_raw(str(raw))
        if st:
            out.append(st)
    return out


def mask_commands_from_mask_result(mask_result: Dict[str, Any]) -> List[str]:
    if not isinstance(mask_result, dict) or mask_result.get("error"):
        return []
    md = mask_result.get("mask") or {}
    lines = md.get("mask_lines") or []
    out: List[str] = []
    for raw in lines:
        w = mask_wire_command_from_config_raw(str(raw))
        if w:
            out.append(w)
    return out


def logs_from_uniloglist(unilog_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(unilog_result, dict) or unilog_result.get("error"):
        return []
    block = unilog_result.get("uniloglist") or {}
    logs = block.get("logs") or []
    if not isinstance(logs, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for e in logs:
        if not isinstance(e, dict):
            continue
        port = str(e.get("port") or "").strip().upper()
        message = str(e.get("message") or "").strip().upper()
        if not message:
            continue
        trig = str(e.get("trigger") or "ONTIME").strip().upper()
        item: Dict[str, Any] = {"port": port, "message": message, "trigger": trig}
        if trig == "ONCHANGED":
            normalized.append(item)
            continue
        per = e.get("period")
        if per is not None:
            try:
                item["period"] = float(parse_log_period_str(per))
            except (ValueError, TypeError):
                item["period"] = 1.0
        else:
            item["period"] = 1.0
        normalized.append(item)
    return normalized


def log_commands_from_logs(logs: List[Dict[str, Any]]) -> List[str]:
    """Стабильный список LOG-команд для снимка/сравнения/применения."""
    out: List[str] = []
    for entry in sorted((e for e in logs if isinstance(e, dict)), key=log_sort_key):
        out.append(build_log_wire_command(entry))
    return out


def _extract_log_commands(doc: Dict[str, Any]) -> List[str]:
    """
    Универсально извлекает LOG-команды из профиля:
    - новый формат: uniloglist_commands
    - старый формат: logs (список структур)
    """
    direct = doc.get("uniloglist_commands")
    if isinstance(direct, list):
        cmds = [str(x).strip() for x in direct if str(x).strip()]
        if cmds:
            return cmds
    logs = [e for e in (doc.get("logs") or []) if isinstance(e, dict)]
    return log_commands_from_logs(logs)


def log_sort_key(entry: Dict[str, Any]) -> Tuple[str, str, str, Optional[float]]:
    trig = str(entry.get("trigger") or "ONTIME").strip().upper()
    per = entry.get("period")
    pf: Optional[float]
    if trig == "ONCHANGED":
        pf = None
    else:
        try:
            pf = float(per) if per is not None else 1.0
        except (TypeError, ValueError):
            pf = 1.0
    return (
        str(entry.get("port") or "").strip().upper(),
        str(entry.get("message") or "").strip().upper(),
        trig,
        pf,
    )


def build_log_wire_command(entry: Dict[str, Any]) -> str:
    """Строка для send_ascii_configuration_line / log: MSG [COMn] period | ONCHANGED."""
    msg = str(entry.get("message") or "").strip().upper()
    port = str(entry.get("port") or "").strip().upper()
    trig = str(entry.get("trigger") or "ONTIME").strip().upper()
    if trig == "ONCHANGED":
        if port in ("COM1", "COM2", "COM3"):
            return f"{msg} {port} ONCHANGED"
        return f"{msg} ONCHANGED"
    try:
        rf = parse_log_period_str(entry.get("period", 1))
    except (ValueError, TypeError):
        rf = 1.0
    wire = format_log_period_wire(rf)
    if port in ("COM1", "COM2", "COM3"):
        return f"{msg} {port} {wire}"
    return f"{msg} {wire}"


def capture_profile(uart: UM982UART) -> Dict[str, Any]:
    """Синхронно: CONFIG, MASK, UNILOGLIST (вызывать под блокировкой порта)."""
    warnings: Dict[str, Optional[str]] = {}
    # use_lines=False заметно быстрее для профиля: без readline-хвоста после последней строки.
    cfg = uart.query_config(use_lines=False)
    if cfg.get("error"):
        warnings["config"] = str(cfg["error"])
    mask = uart.query_mask()
    if mask.get("error"):
        warnings["mask"] = str(mask["error"])
    unilog = uart.query_uniloglist()
    if unilog.get("error"):
        warnings["uniloglist"] = str(unilog["error"])

    logs = logs_from_uniloglist(unilog)
    doc: Dict[str, Any] = {
        "_meta": {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "format": "um982_receiver_profile",
        },
        "config_set_commands": extract_config_set_commands(cfg),
        "mask_commands": mask_commands_from_mask_result(mask),
        "logs": logs,
        "uniloglist_commands": log_commands_from_logs(logs),
        "capture_warnings": {k: v for k, v in warnings.items() if v},
    }
    return doc


def profile_document_to_json(doc: Dict[str, Any], *, indent: int = 2) -> str:
    return json.dumps(doc, ensure_ascii=False, indent=indent)


def load_profile_json(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("Корень JSON должен быть объектом")
    return data


def diff_config_commands(live: List[str], saved: List[str]) -> Dict[str, List[str]]:
    ls = {_norm_cmd(x) for x in live}
    ss = {_norm_cmd(x) for x in saved}
    return {
        "only_in_saved": sorted(ss - ls),
        "only_in_live": sorted(ls - ss),
    }


def diff_mask_commands(live: List[str], saved: List[str]) -> Dict[str, List[str]]:
    return diff_config_commands(live, saved)


def diff_logs(
    live: List[Dict[str, Any]],
    saved: List[Dict[str, Any]],
) -> Dict[str, Any]:
    def key_index(pool: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str, Optional[float]], Dict[str, Any]]:
        return {log_sort_key(e): e for e in pool if isinstance(e, dict)}

    lk = set(key_index(live))
    sk = set(key_index(saved))
    saved_by_k = key_index(saved)
    live_by_k = key_index(live)
    only_saved_k = sorted(sk - lk)
    only_live_k = sorted(lk - sk)
    return {
        "only_in_saved": [saved_by_k[k] for k in only_saved_k if k in saved_by_k],
        "only_in_live": [live_by_k[k] for k in only_live_k if k in live_by_k],
        "only_in_saved_keys": [list(t) for t in only_saved_k],
        "only_in_live_keys": [list(t) for t in only_live_k],
    }


def diff_profiles(live: Dict[str, Any], saved: Dict[str, Any]) -> Dict[str, Any]:
    lc = list(live.get("config_set_commands") or [])
    sc = list(saved.get("config_set_commands") or [])
    lm = list(live.get("mask_commands") or [])
    sm = list(saved.get("mask_commands") or [])
    ll = [e for e in (live.get("logs") or []) if isinstance(e, dict)]
    sl = [e for e in (saved.get("logs") or []) if isinstance(e, dict)]
    llog_cmds = _extract_log_commands(live)
    slog_cmds = _extract_log_commands(saved)
    return {
        "config": diff_config_commands(lc, sc),
        "mask": diff_mask_commands(lm, sm),
        "logs": diff_logs(ll, sl),
        "log_commands": diff_config_commands(llog_cmds, slog_cmds),
    }


_CONFIG_APPLY_DENY_PREFIX = (
    "FRESET",
    "RESET ",
    "RESET\t",
)


def _is_denied_config_command(cmd: str) -> bool:
    n = _norm_cmd(cmd)
    for p in _CONFIG_APPLY_DENY_PREFIX:
        if n.startswith(p):
            return True
    return False


def apply_profile_diff(
    uart: UM982UART,
    diff: Dict[str, Any],
    *,
    apply_config: bool = True,
    apply_mask: bool = True,
    apply_logs: bool = True,
    unlog_extra: bool = False,
    send_line: Optional[Callable[[UM982UART, str], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Применить отличия «как в файле» к приёмнику (под внешней блокировкой).

    send_line: по умолчанию uart.send_ascii_configuration_line
    """
    sender = send_line or (lambda u, s: u.send_ascii_configuration_line(s))
    log: List[Dict[str, Any]] = []
    errors: List[str] = []

    def run_cmd(label: str, cmd: str) -> None:
        r = sender(uart, cmd)
        log.append({"step": label, "command": cmd, "result": r})
        if r.get("error"):
            errors.append(f"{label}: {cmd!r} → {r['error']}")

    if apply_config:
        for cmd in diff.get("config", {}).get("only_in_saved", []) or []:
            if _is_denied_config_command(cmd):
                log.append({"step": "config_skip", "command": cmd, "note": "запрещено к авто-применению"})
                continue
            run_cmd("config", cmd)

    if apply_mask:
        for cmd in diff.get("mask", {}).get("only_in_saved", []) or []:
            run_cmd("mask", cmd)

    if apply_logs:
        logs_block = diff.get("logs") or {}
        log_cmds_block = diff.get("log_commands") or {}
        to_log = list(log_cmds_block.get("only_in_saved", []) or [])
        if not to_log:
            # Обратная совместимость: старый diff без log_commands.
            for entry in logs_block.get("only_in_saved", []) or []:
                if isinstance(entry, dict):
                    to_log.append(build_log_wire_command(entry))
        for cmd in to_log:
            run_cmd("log", cmd)
        if unlog_extra:
            # Предпочитаем новый формат сравнения (команды).
            only_live_cmds = list(log_cmds_block.get("only_in_live", []) or [])
            if only_live_cmds:
                for cmd in only_live_cmds:
                    parts = str(cmd).strip().split()
                    if not parts:
                        continue
                    # cmd ожидается в виде "<MSG> [COMn] <period|ONCHANGED>".
                    msg = parts[0].upper()
                    port = parts[1].upper() if len(parts) >= 3 and parts[1].upper() in ("COM1", "COM2", "COM3") else None
                    if port:
                        run_cmd("unlog", f"UNLOG {port} {msg}")
                    else:
                        run_cmd("unlog", f"UNLOG {msg}")
            else:
                # Обратная совместимость: старый diff без log_commands.
                for entry in logs_block.get("only_in_live", []) or []:
                    if not isinstance(entry, dict):
                        continue
                    port = str(entry.get("port") or "").strip().upper()
                    msg = str(entry.get("message") or "").strip().upper()
                    if port in ("COM1", "COM2", "COM3") and msg:
                        run_cmd("unlog", f"UNLOG {port} {msg}")
                    elif msg:
                        run_cmd("unlog", f"UNLOG {msg}")

    return {"ok": not errors, "errors": errors, "steps": log}
