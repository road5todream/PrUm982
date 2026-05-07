"""
Запрос конфигурации MASK. Команда MASK, ответ в формате $CONFIG,MASK,...
"""
import time
from typing import Optional, Dict, Any, List

from .core import Um982Core


def query_mask(core: Um982Core, add_crlf: Optional[bool] = None) -> Dict[str, Any]:
    """
    Query MASK configuration.

    Sends command MASK; device responds with lines like:
        $CONFIG,MASK,MASK 5.000000*15
        $CONFIG,MASK,MASK GPS*4A
        $CONFIG,MASK,GPSMaskPrn:12,*13
    """
    if add_crlf is None:
        add_crlf = getattr(core, "baudrate", 460800) >= 460800

    if core.serial_conn and core.serial_conn.in_waiting > 0:
        core.serial_conn.reset_input_buffer()

    if not core.send_ascii_command("MASK", add_crlf=add_crlf):
        return {"error": "Не удалось отправить команду MASK"}

    time.sleep(0.12)
    response = b""
    t0 = time.time()
    last_data_at = t0
    # Масок может быть много строк подряд; не обрываем приём на одном пустом read.
    idle_after_data_s = 0.35
    max_wait_first_byte_s = 1.2
    max_total_s = 2.8

    while time.time() - t0 < max_total_s:
        chunk = core.read_response(timeout=0.2)
        if chunk:
            response += chunk
            last_data_at = time.time()
        elif response:
            if time.time() - last_data_at >= idle_after_data_s:
                break
        else:
            if time.time() - t0 > max_wait_first_byte_s:
                break
            time.sleep(0.02)

    if not response:
        return {"error": "Ответ на MASK не получен"}

    parsed = core.parse_binary_response(response)
    messages = parsed.get("parsed", {}).get("messages", []) if isinstance(parsed, dict) else []

    mask_entries: List[Dict[str, Any]] = []
    elevation_masks: List[float] = []
    system_masks: List[str] = []
    prn_masks: Dict[str, List[int]] = {}

    for msg in messages:
        if not isinstance(msg, dict):
            continue
        raw = msg.get("raw", "")
        if not raw or not raw.startswith("$CONFIG,MASK,"):
            continue

        parts = raw.split(",")
        if len(parts) < 3:
            continue
        payload = parts[2]
        if "*" in payload:
            payload = payload.split("*", 1)[0]
        payload = payload.strip()

        entry: Dict[str, Any] = {"raw": raw, "value": payload}

        if payload.upper().startswith("MASK "):
            rest = payload[5:].strip()
            try:
                val = float(rest)
                elevation_masks.append(val)
                entry["type"] = "threshold"
                entry["threshold"] = val
            except ValueError:
                sys_name = rest.upper()
                system_masks.append(sys_name)
                entry["type"] = "system"
                entry["system"] = sys_name
        elif "MaskPrn" in payload:
            try:
                name, prn_part = payload.split("MaskPrn:", 1)
                system = name.strip().upper()
                prn_str = prn_part.strip().rstrip(",")
                prn = int(prn_str)
                prn_masks.setdefault(system, []).append(prn)
                entry["type"] = "prn_mask"
                entry["system"] = system
                entry["prn"] = prn
            except Exception:
                entry["type"] = "unknown"
        else:
            entry["type"] = "unknown"

        mask_entries.append(entry)

    mask_data = {
        "entries": mask_entries,
        "elevation_masks": elevation_masks,
        "system_masks": system_masks,
        "prn_masks": prn_masks,
        "mask_lines": [e.get("raw", "") for e in mask_entries],
    }

    return {
        "command": "MASK",
        "response": parsed,
        "mask": mask_data,
        "raw_response_length": len(response),
    }
