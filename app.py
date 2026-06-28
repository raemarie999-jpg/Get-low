import os, json, time, threading
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template_string
import requests

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024

API_KEY = os.environ.get("WETHR_API_KEY", "")
DATA_DIR = "/data"
REFRESH_SEC = 1800  # 30 minutes

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def load_json_file(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return default

def save_json_file(path, data):
    try:
        ensure_data_dir()
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
        return True
    except Exception as e:
        add_log(f"Save error {path}: {e}", "err")
        return False

STATIONS = ["KPHL", "KATL", "KOKC"]
STATION_NAMES = {
    "KPHL": "Philadelphia International Airport",
    "KATL": "Atlanta Hartsfield-Jackson Airport",
    "KOKC": "Oklahoma City Will Rogers World Airport",
}

ALL_KNOWN_MODELS = [
    "ARPEGE","HRRR","UKMO","LAV-MOS","NAM","RAP","GEM-GDPS","NAM-MOS","NBM",
    "NAM4KM","GFS","ICON","GFS-MOS","NBS-MOS","ECMWF-HRES","GEFS","JMA","RDPS","SREF"
]
REFRESH_SEC = 1800

# --- Rate limiting: pace every wethr API request ---
_api_lock = threading.Lock()
_last_request_time = 0
MIN_REQUEST_INTERVAL = 2.5  # seconds between API calls

# --- Manual refresh cooldown: stops external pings / rapid re-clicks from
# bypassing REFRESH_SEC and spawning unlimited fetch_all() runs ---
_manual_refresh_lock = threading.Lock()
_last_manual_refresh = {}
MANUAL_REFRESH_COOLDOWN_SEC = 30  # min seconds between manual refreshes, per station

# --- Hard daily API cap: resets at 19:30 UTC (= 3:30pm EDT / 2:30pm EST) ---
DAILY_REQUEST_CAP = 2500
_CAP_RESET_UTC_HOUR = 19
_CAP_RESET_UTC_MINUTE = 30
_counter_lock = threading.Lock()

class DailyCapExceeded(Exception):
    pass

def _get_period_key():
    """Returns string key for the current quota period.
    Resets at 19:30 UTC (3:30pm EDT in summer; shifts 1hr in winter — acceptable).
    """
    now = datetime.utcnow()
    reset_today = now.replace(hour=_CAP_RESET_UTC_HOUR, minute=_CAP_RESET_UTC_MINUTE, second=0, microsecond=0)
    period_start = reset_today if now >= reset_today else reset_today - timedelta(days=1)
    return period_start.strftime("%Y-%m-%d_%H%M")

def _load_api_counter():
    try:
        with open(f"{DATA_DIR}/api_counter_lows.json") as f:
            return json.load(f)
    except:
        return {}

def _save_api_counter(data):
    try:
        ensure_data_dir()
        tmp = f"{DATA_DIR}/api_counter_lows.json.tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, f"{DATA_DIR}/api_counter_lows.json")
    except Exception as e:
        print(f"Counter save error: {e}")

def _check_and_increment():
    """Raises DailyCapExceeded if at or over cap; otherwise increments and saves."""
    with _counter_lock:
        period = _get_period_key()
        data = _load_api_counter()
        count = data.get(period, 0)
        if count >= DAILY_REQUEST_CAP:
            raise DailyCapExceeded(f"Daily cap ({DAILY_REQUEST_CAP}) reached. Resets at 3:30pm EST.")
        data[period] = count + 1
        keys = sorted(data.keys())
        if len(keys) > 3:
            for k in keys[:-3]:
                del data[k]
        _save_api_counter(data)
        return data[period]

def make_state():
    return {
        "obs": None,
        "wethr_low": None,
        "forecasts": {},
        "accuracy": {},
        "last_updated": None,
        "errors": [],
        "log": [],
        "today_avg_pace": {},
        "consensus_snapshots": [],
    }

states = {s: make_state() for s in STATIONS}

def get_state(station=None):
    return states.get(station or "KPHL", states["KPHL"])

def active_models(station="KPHL"):
    acc = get_state(station).get("accuracy", {})
    return [m for m in acc.keys() if m != "NWS"] if acc else []

def add_log(msg, level="info", station="KPHL"):
    entry = {"t": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
    st = get_state(station)
    st["log"].insert(0, entry)
    st["log"] = st["log"][:100]
    print(f"[{station}][{entry['t']}] {msg}")

def _throttle():
    global _last_request_time
    with _api_lock:
        now = time.monotonic()
        wait = MIN_REQUEST_INTERVAL - (now - _last_request_time)
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.monotonic()

def wethr_get(path):
    _check_and_increment()  # raises DailyCapExceeded before any sleep/request
    _throttle()
    r = requests.get(
        f"https://wethr.net/api/v2/{path}",
        headers={"X-API-Key": API_KEY},
        timeout=6
    )
    r.raise_for_status()
    return r.json()

def get_temp(x):
    for k in ["temperature_f","temperature_display","temperature","temp","value","low"]:
        v = x.get(k)
        if v is not None:
            try: return round(float(v), 1)
            except: pass
    return None

def parse_vt(x):
    vt = str(x.get("valid_time",""))
    try: return datetime.strptime(vt[:16], "%Y-%m-%d %H:%M")
    except: return None

def local_now():
    return datetime.utcnow() - timedelta(hours=5)

def get_low_window():
    now_local = local_now()
    if now_local.hour > 9 or (now_local.hour == 9 and now_local.minute >= 30):
        tomorrow = now_local.replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=1)
        window_start_utc = tomorrow + timedelta(hours=5)
        window_end_utc = window_start_utc + timedelta(hours=24)
    else:
        today_1am = now_local.replace(hour=1, minute=0, second=0, microsecond=0)
        window_start_utc = today_1am + timedelta(hours=5)
        window_end_utc = window_start_utc + timedelta(hours=24)
    return window_start_utc, window_end_utc

def low_window_entries(temps):
    window_start, window_end = get_low_window()
    filtered = [x for x in temps if parse_vt(x) is not None and window_start <= parse_vt(x) < window_end]
    return filtered

def fmt_run(run_raw):
    try:
        if len(run_raw) >= 13:
            return run_raw[11:13] + "Z"
        return run_raw or "—"
    except:
        return "—"

def get_run_data(acc_model, run_key):
    """
    Look up run-specific accuracy data for a model.
    Priority: exact run match -> 'default' fallback -> empty dict.
    Returns (run_data_dict, source_label) where source_label is 'run', 'default', or 'overall'.
    """
    runs = acc_model.get("runs") or {}
    # 1. Exact run match
    rd = runs.get(run_key, {})
    if rd and (rd.get("mae") or rd.get("correction") not in (None, "")):
        return rd, "run"
    # 2. Default fallback run
    default_rd = runs.get("default", {})
    if default_rd and (default_rd.get("mae") or default_rd.get("correction") not in (None, "")):
        return default_rd, "default"
    # 3. Nothing run-specific found
    return {}, "overall"

def fetch_all(station="KPHL"):
    st = get_state(station)
    if not API_KEY:
        add_log("No API key set", "err", station)
        return
    # Check cap before doing anything
    counter_data = _load_api_counter()
    period = _get_period_key()
    current_count = counter_data.get(period, 0)
    if current_count >= DAILY_REQUEST_CAP:
        add_log(f"Daily API cap reached ({current_count}/{DAILY_REQUEST_CAP}) — skipping fetch. Resets 3:30pm EST.", "warn", station)
        return
    add_log("Fetching data...", "info", station)
    errors = []

    # Observation
    try:
        obs = wethr_get(f"observations.php?station_code={station}&mode=latest")
        st["obs"] = obs
        add_log(f"Obs: {obs.get('temperature_display')}F", "ok", station)
    except DailyCapExceeded:
        add_log(f"Daily cap reached — stopping fetch. Resets 3:30pm EST.", "warn", station)
        return
    except Exception as e:
        errors.append(f"Obs: {e}")
        add_log(f"Obs error: {e}", "err", station)

    # Wethr low
    try:
        wl = wethr_get(f"observations.php?station_code={station}&mode=wethr_low&logic=nws")
        st["wethr_low"] = wl
        add_log(f"Wethr Low: {wl.get('wethr_low')}F", "ok", station)
    except DailyCapExceeded:
        add_log(f"Daily cap reached — stopping fetch. Resets 3:30pm EST.", "warn", station)
        return
    except Exception as e:
        errors.append(f"WethrLow: {e}")
        add_log(f"Wethr Low error: {e}", "err", station)

    fetch_targets = active_models(station)
    if not fetch_targets:
        add_log("No accuracy data yet — skipping model fetch", "warn", station)
        return

    utc_now = datetime.utcnow()
    window_start, window_end = get_low_window()

    for model in fetch_targets:
        try:
            data = wethr_get(f"forecasts.php?location_name={station}&model={requests.utils.quote(model)}&run=latest")
            temps = data if isinstance(data, list) else data.get("forecasts", [])
            meta = {} if isinstance(data, list) else data
            if temps:
                window = low_window_entries(temps)
                if not window:
                    add_log(f"{model}: no entries in low window", "warn", station)
                    continue
                min_entries = 12 if model == "HRRR" else 4
                if len(window) < min_entries:
                    add_log(f"{model}: only {len(window)} entries in window — run not fully ingested yet, keeping previous", "warn", station)
                    continue
                min_entry = min(window, key=lambda x: get_temp(x) or 999)
                raw_temp = get_temp(min_entry)
                in_window = [x for x in window if parse_vt(x) is not None and parse_vt(x) <= datetime.utcnow()]
                if in_window:
                    closest = min(in_window, key=lambda x: abs((parse_vt(x) - utc_now).total_seconds()) if parse_vt(x) else 99999)
                else:
                    closest = min(window, key=lambda x: abs((parse_vt(x) - utc_now).total_seconds()) if parse_vt(x) else 99999)
                current_temp = get_temp(closest)
                run_raw = meta.get("run_time") or min_entry.get("run_time") or min_entry.get("run") or ""
                run_fmt = fmt_run(run_raw)
                vt = parse_vt(min_entry)
                low_time = None
                if vt:
                    local_vt = vt - timedelta(hours=5)
                    low_time = local_vt.strftime("%-I:%M%p").lower()

                st["forecasts"][model] = {
                    "low": raw_temp,
                    "current_fcst": current_temp,
                    "run": run_fmt,
                    "low_time": low_time,
                    "window_entries": len(window),
                }
                add_log(f"{model}: low={raw_temp} now={current_temp} run={run_fmt} ({len(window)} entries)", "ok", station)
        except DailyCapExceeded:
            add_log(f"Daily cap reached mid-fetch — stopping. Resets 3:30pm EST.", "warn", station)
            break
        except Exception as e:
            errors.append(f"{model}: {e}")
            add_log(f"{model} error: {str(e)[:80]}", "warn", station)

    st["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st["errors"] = errors
    add_log(f"Done. {len(st['forecasts'])} models loaded.", "ok", station)

    try:
        rows = build_snapshot_rows(station)
        save_pacing_snapshot(rows, station)
    except Exception as e:
        add_log(f"Snapshot error: {e}", "warn", station)

    try:
        now_local = local_now()
        if now_local.minute < 20 or (now_local.minute >= 30 and now_local.minute < 50):
            save_consensus_snapshot(station)
    except Exception as e:
        add_log(f"Consensus snapshot error: {e}", "warn", station)

_memory_snapshots = {}

def save_pacing_snapshot(rows, station="KPHL"):
    st = get_state(station)
    now = local_now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")
    entry = {"time": time_str}
    for r in rows:
        if r.get("pace") is not None:
            entry[r["model"]] = r["pace"]
    if date_str not in _memory_snapshots:
        _memory_snapshots[date_str] = []
    _memory_snapshots[date_str].append(entry)
    avg = {}
    for r in rows:
        m = r["model"]
        vals = [s[m] for s in _memory_snapshots[date_str] if m in s]
        if vals:
            avg[m] = round(sum(vals)/len(vals), 2)
    st["today_avg_pace"] = avg
    try:
        ensure_data_dir()
        disk = load_json_file(f"{DATA_DIR}/pacing_{station}.json", {})
        if date_str not in disk:
            disk[date_str] = []
        disk[date_str].append(entry)
        keys = sorted(disk.keys())
        if len(keys) > 60:
            for k in keys[:-60]:
                del disk[k]
        save_json_file(f"{DATA_DIR}/pacing_{station}.json", disk)
    except Exception as e:
        add_log(f"Disk snapshot error (non-fatal): {e}", "warn", station)
    add_log(f"Snapshot: {len([r for r in rows if r.get('pace') is not None])} models saved", "info", station)

def rollup_daily_history(station="KPHL"):
    now = local_now()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    snapshots = load_json_file(f"{DATA_DIR}/pacing_{station}.json", {})
    if yesterday not in snapshots or not snapshots[yesterday]:
        return
    history = load_json_file(f"{DATA_DIR}/history_{station}.json", {})
    if yesterday in history:
        return
    day_snaps = snapshots[yesterday]
    models = set()
    for s in day_snaps:
        models.update(k for k in s.keys() if k != "time")
    daily_avg = {}
    for m in models:
        vals = [s[m] for s in day_snaps if m in s]
        if vals:
            daily_avg[m] = round(sum(vals)/len(vals), 2)
    history[yesterday] = {"avg_pace": daily_avg, "snapshot_count": len(day_snaps), "date": yesterday}
    save_json_file(f"{DATA_DIR}/history_{station}.json", history)
    add_log(f"Rolled up history for {yesterday} ({len(day_snaps)} snapshots)", "ok", station)

def build_snapshot_rows(station="KPHL"):
    st = get_state(station)
    acc = st["accuracy"]
    models = [m for m in acc.keys() if m != "NWS"] if acc else []
    obs_temp = (st["obs"] or {}).get("temperature_display")
    rows = []
    for model in models:
        fcst = st["forecasts"].get(model, {})
        current_fcst = fcst.get("current_fcst")
        try:
            pace = round(float(obs_temp) - float(current_fcst), 2) if obs_temp and current_fcst else None
        except:
            pace = None
        rows.append({"model": model, "pace": pace})
    return rows

def save_consensus_snapshot(station="KPHL"):
    st = get_state(station)
    now = local_now()
    if (now.hour < 9 or (now.hour == 9 and now.minute < 30)) or now.hour >= 23:
        return
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")
    acc = st.get("accuracy", {})
    forecasts = st.get("forecasts", {})
    models = [m for m in acc.keys() if m != "NWS"]
    w_sum, w_total = 0, 0
    pw_sum, pw_total = 0, 0
    obs_temp = (st.get("obs") or {}).get("temperature_display")
    for model in models:
        a = acc.get(model, {})
        fcst = forecasts.get(model, {})
        raw = fcst.get("low")
        current_run = fcst.get("run", "")
        # Use the helper with default fallback
        run_data, _ = get_run_data(a, current_run)
        corr = run_data.get("correction") if run_data else None
        # Fall back to overall correction if run_data has none
        if corr in (None, ""):
            corr = a.get("correction")
        try:
            mae_val = run_data.get("mae") if run_data else None
            if not mae_val:
                mae_val = a.get("mae")
            mae = float(mae_val or 0)
            adj = round(float(raw) + float(corr), 1) if raw is not None and corr not in (None, "") else None
            if mae > 0 and adj is not None:
                w = 1/mae; w_sum += adj*w; w_total += w
        except: pass
        try:
            current_fcst = fcst.get("current_fcst")
            pace = round(float(obs_temp) - float(current_fcst), 2) if obs_temp and current_fcst else None
            mae_val = run_data.get("mae") if run_data else None
            if not mae_val:
                mae_val = a.get("mae")
            mae = float(mae_val or 0)
            if mae > 0 and pace is not None:
                w = 1/mae; pw_sum += float(pace)*w; pw_total += w
        except: pass
    consensus = round(w_sum/w_total, 1) if w_total > 0 else None
    cons_pace = round(pw_sum/pw_total, 2) if pw_total > 0 else None
    implied = round(consensus + cons_pace, 1) if consensus is not None and cons_pace is not None else None
    if consensus is None:
        return
    entry = {
        "time": time_str, "date": date_str,
        "consensus": consensus, "implied": implied,
        "pace": cons_pace,
        "obs": float(obs_temp) if obs_temp else None,
    }
    snaps = st["consensus_snapshots"]
    snaps = [s for s in snaps if s.get("date") == date_str]
    snaps.append(entry)
    st["consensus_snapshots"] = snaps[-48:]
    try:
        ensure_data_dir()
        path = f"{DATA_DIR}/consensus_{station}.json"
        disk = load_json_file(path, {})
        if date_str not in disk:
            disk[date_str] = []
        disk[date_str].append(entry)
        keys = sorted(disk.keys())
        if len(keys) > 90:
            for k in keys[:-90]: del disk[k]
        save_json_file(path, disk)
    except Exception as e:
        add_log(f"Consensus snapshot error: {e}", "warn", station)

def scheduled_fetch():
    for i, station in enumerate(STATIONS):
        if i > 0:
            time.sleep(30)
        t = threading.Thread(target=fetch_all, args=(station,), daemon=True)
        t.start()
        t.join(timeout=120)
        if t.is_alive():
            add_log("Fetch timed out", "err", station)

def background_loop():
    while True:
        try:
            scheduled_fetch()
        except Exception as e:
            print(f"Loop error: {e}")
        try:
            now = local_now()
            if now.hour == 1:
                for station in STATIONS:
                    rollup_daily_history(station)
        except Exception as e:
            print(f"Rollup error: {e}")
        time.sleep(REFRESH_SEC)

def _get_prev_days(n, station="KPHL"):
    history = load_json_file(f"{DATA_DIR}/history_{station}.json", {})
    keys = sorted(history.keys(), reverse=True)[:n]
    return [{"date": k, "avg_pace": history[k]["avg_pace"], "snapshot_count": history[k].get("snapshot_count", 0)} for k in keys]

@app.route("/api/state")
def api_state():
    station = request.args.get("station", "KPHL").upper()
    if station not in STATIONS:
        station = "KPHL"
    st = get_state(station)
    acc = st["accuracy"]
    models = active_models(station)
    rows = []
    window_start, window_end = get_low_window()
    for i, model in enumerate(models):
        a = acc.get(model, {})
        fcst = st["forecasts"].get(model, {})
        raw = fcst.get("low")
        current_run = fcst.get("run", "")

        # Use helper: exact run -> default fallback -> overall
        run_data, corr_source = get_run_data(a, current_run)
        corr = run_data.get("correction") if run_data else None
        display_mae = run_data.get("mae") if run_data else None

        # If run_data gave nothing useful, fall back to overall
        if corr in (None, ""):
            corr = a.get("correction")
            if corr not in (None, ""):
                corr_source = "overall"
        if not display_mae:
            display_mae = a.get("mae")

        try: adj = round(float(raw) + float(corr), 1) if raw is not None and corr not in (None, "") else None
        except: adj = None
        obs_temp = (st["obs"] or {}).get("temperature_display")
        current_fcst = fcst.get("current_fcst")
        try: pace = round(float(obs_temp) - float(current_fcst), 1) if obs_temp and current_fcst else None
        except: pace = None

        rows.append({
            "rank": i+1, "model": model,
            "run": fcst.get("run", "—"),
            "raw_low": raw, "correction": corr,
            "corr_source": corr_source,
            "adj_low": adj, "pace": pace,
            "low_time": fcst.get("low_time"),
            "mae": display_mae, "rmse": a.get("rmse"),
            "runs": a.get("runs", {}),
        })

    def get_mae(r):
        if r.get("mae") not in (None, ""):
            try: return float(r["mae"])
            except: pass
        return None

    w_sum, w_total = 0, 0
    for r in rows:
        try:
            mae = get_mae(r); adj = r["adj_low"] if r["adj_low"] is not None else r["raw_low"]
            if mae and mae > 0 and adj is not None:
                w = 1/mae; w_sum += adj*w; w_total += w
        except: pass
    consensus = round(w_sum/w_total, 1) if w_total > 0 else None
    pw_sum, pw_total = 0, 0
    for r in rows:
        try:
            mae = get_mae(r); pace = r["pace"]
            if mae and mae > 0 and pace is not None:
                w = 1/mae; pw_sum += float(pace)*w; pw_total += w
        except: pass
    consensus_pace = round(pw_sum/pw_total, 2) if pw_total > 0 else None
    ws_local = window_start - timedelta(hours=5)
    we_local = window_end - timedelta(hours=5)
    window_label = f"{ws_local.strftime('%a %-I%p')} – {we_local.strftime('%a %-I%p')}"
    return jsonify({
        "station": station, "obs": st["obs"], "wethr_low": st["wethr_low"],
        "rows": rows, "consensus": consensus,
        "last_updated": st["last_updated"], "errors": st["errors"],
        "log": st["log"][:30], "models": active_models(station),
        "consensus_pace": consensus_pace,
        "today_avg_pace": st["today_avg_pace"],
        "today_snapshot_count": len(load_json_file(f"{DATA_DIR}/pacing_{station}.json", {}).get(local_now().strftime("%Y-%m-%d"), [])),
        "prev_days": _get_prev_days(3, station),
        "window_label": window_label,
    })

@app.route("/api/history")
def api_history():
    station = request.args.get("station", "KPHL").upper()
    if station not in STATIONS:
        station = "KPHL"
    return jsonify(load_json_file(f"{DATA_DIR}/history_{station}.json", {}))

@app.route("/api/accuracy", methods=["POST"])
def save_accuracy():
    station = request.args.get("station", "KPHL").upper()
    if station not in STATIONS:
        station = "KPHL"
    get_state(station)["accuracy"] = request.json or {}
    add_log("Accuracy data updated", "ok", station)
    save_json_file(f"{DATA_DIR}/accuracy_{station}.json", request.json or {})
    return jsonify({"ok": True})

@app.route("/api/consensus_snapshots")
def api_consensus_snapshots():
    station = request.args.get("station", "KPHL").upper()
    if station not in STATIONS: station = "KPHL"
    st = get_state(station)
    disk = load_json_file(f"{DATA_DIR}/consensus_{station}.json", {})
    return jsonify({"today": st.get("consensus_snapshots", []), "history": disk, "station": station})

@app.route("/api/quota")
def api_quota():
    period = _get_period_key()
    data = _load_api_counter()
    count = data.get(period, 0)
    return jsonify({
        "period": period,
        "count": count,
        "cap": DAILY_REQUEST_CAP,
        "remaining": max(0, DAILY_REQUEST_CAP - count),
        "paused": count >= DAILY_REQUEST_CAP,
        "resets": "3:30pm EST daily",
    })

@app.route("/api/refresh", methods=["POST"])
def manual_refresh():
    station = request.args.get("station", "KPHL").upper()
    if station not in STATIONS:
        station = "KPHL"
    with _manual_refresh_lock:
        now = time.monotonic()
        elapsed = now - _last_manual_refresh.get(station, 0)
        if elapsed < MANUAL_REFRESH_COOLDOWN_SEC:
            remaining = round(MANUAL_REFRESH_COOLDOWN_SEC - elapsed)
            add_log(f"Manual refresh ignored (cooldown, {remaining}s left)", "warn", station)
            return jsonify({"ok": False, "cooldown": True, "remaining_sec": remaining})
        _last_manual_refresh[station] = now
    threading.Thread(target=fetch_all, args=(station,), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/verify", methods=["POST"])
def save_verification():
    station = request.args.get("station", "KPHL").upper()
    if station not in STATIONS:
        station = "KPHL"
    data = request.json or {}
    actual = data.get("actual")
    date = data.get("date")
    if actual is None or not date:
        return jsonify({"ok": False, "error": "Missing actual or date"}), 400
    ensure_data_dir()
    path = f"{DATA_DIR}/verification_{station}.json"
    verif = load_json_file(path, {})
    cons_path = f"{DATA_DIR}/consensus_{station}.json"
    cons_disk = load_json_file(cons_path, {})
    day_snaps = cons_disk.get(date, [])
    calibration = []
    for s in day_snaps:
        consensus = s.get("consensus")
        if consensus is not None:
            error = round(float(actual) - float(consensus), 2)
            calibration.append({
                "time": s.get("time"),
                "consensus": consensus,
                "implied": s.get("implied"),
                "pace": s.get("pace"),
                "actual": float(actual),
                "error": error,
                "abs_error": abs(error),
            })
    verif[date] = {
        "date": date,
        "actual": float(actual),
        "metric": "low",
        "snapshot_count": len(day_snaps),
        "calibration": calibration,
        "entered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    save_json_file(path, verif)
    add_log(f"Verification saved: {date} actual={actual} ({len(calibration)} snapshots calibrated)", "ok", station)
    return jsonify({"ok": True, "calibration_points": len(calibration)})

@app.route("/api/verification")
def get_verification():
    station = request.args.get("station", "KPHL").upper()
    if station not in STATIONS:
        station = "KPHL"
    verif = load_json_file(f"{DATA_DIR}/verification_{station}.json", {})
    return jsonify(verif)

@app.route("/")
def index():
    return render_template_string(HTML)

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Low Temp Tracker</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#080c10;--bg2:#0e1520;--bg3:#0b1118;--border:#1a2535;
  --text:#c9d4e0;--dim:#4a6080;--dimmer:#2a3a50;
  --blue:#38bdf8;--green:#4ade80;--yellow:#facc15;--red:#f87171;--purple:#c084fc;
  --ice:#a5f3fc;--orange:#fb923c;
}
body{background:var(--bg);color:var(--text);font-family:'IBM Plex Mono',monospace;font-size:13px;min-height:100vh}
header{background:var(--bg3);border-bottom:1px solid var(--border);padding:14px 20px;
  position:sticky;top:0;z-index:20;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px}
h1{font-size:18px;color:#e8f0f8;letter-spacing:-.5px}
.sub{font-size:10px;color:var(--dim);letter-spacing:2px;text-transform:uppercase;margin-top:2px}
.hright{display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.sp{width:1px;height:40px;background:var(--border)}
.stat-pill .lbl{font-size:9px;color:var(--dim);letter-spacing:2px;text-transform:uppercase}
.stat-pill .val{font-size:22px;font-weight:700;line-height:1.1}
.stat-pill .sub2{font-size:9px;color:var(--dimmer)}
nav{display:flex;gap:2px;background:var(--bg3);border-bottom:1px solid var(--border);padding:0 20px}
nav button{background:none;border:none;border-bottom:2px solid transparent;color:var(--dim);
  padding:10px 16px;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;
  cursor:pointer;font-family:inherit;transition:color .15s}
nav button.active{border-bottom-color:var(--ice);color:var(--ice)}
main{padding:20px;max-width:1100px;margin:0 auto}
.tab{display:none}.tab.active{display:block}
.card{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:16px 18px;margin-bottom:16px}
.ctitle{font-size:10px;letter-spacing:2.5px;color:var(--ice);text-transform:uppercase;margin-bottom:12px}
.srow{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px}
.sc{background:#0b1520;border:1px solid var(--border);border-radius:6px;padding:12px 16px;flex:1;min-width:120px}
.sc .lbl{font-size:9px;letter-spacing:2px;color:var(--dim);text-transform:uppercase}
.sc .v{font-size:22px;font-weight:700;margin-top:4px;line-height:1}
.sc .s{font-size:10px;color:var(--dimmer);margin-top:3px}
table{width:100%;border-collapse:collapse}
th{padding:7px 10px;text-align:left;font-size:10px;letter-spacing:1.5px;color:var(--dim);
   text-transform:uppercase;border-bottom:1px solid var(--border);white-space:nowrap}
td{padding:8px 10px;border-bottom:1px solid #111922;white-space:nowrap}
tr:nth-child(even) td{background:#0a1018}
input[type=number]{background:var(--bg);border:1px solid #1e2e42;border-radius:4px;
  color:var(--text);padding:4px 8px;font-size:12px;width:70px;font-family:inherit;outline:none}
input[type=number]:focus{border-color:var(--ice)}
.btn{background:none;border:1px solid var(--ice);color:var(--ice);border-radius:4px;
  padding:6px 14px;font-size:11px;letter-spacing:1px;cursor:pointer;text-transform:uppercase;font-family:inherit}
.btn-red{border-color:var(--red);color:var(--red)}
.btn-green{border-color:var(--green);color:var(--green)}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:5px}
.dot-green{background:var(--green);box-shadow:0 0 6px var(--green)}
.dot-red{background:var(--red);box-shadow:0 0 6px var(--red)}
.dot-yellow{background:var(--yellow);box-shadow:0 0 6px var(--yellow)}
.pbars{display:flex;flex-direction:column;gap:7px}
.prow{display:flex;align-items:center;gap:10px}
.plabel{width:80px;font-size:11px;color:#8aabcc}
.pbar{height:10px;border-radius:3px}
.logbox{background:#060a0e;border-radius:4px;padding:12px;max-height:400px;overflow-y:auto}
.pill-y{background:#facc1522;color:var(--yellow);border-radius:3px;padding:2px 7px;font-size:10px;font-weight:600}
.pill-g{background:#4ade8022;color:var(--green);border-radius:3px;padding:2px 7px;font-size:10px;font-weight:600}
.window-badge{background:#a5f3fc22;color:var(--ice);border-radius:3px;padding:2px 8px;font-size:10px;font-weight:600;letter-spacing:1px}
/* Default run column highlight */
.default-col{background:#fb923c0d !important}
th.default-col{color:var(--orange) !important}
</style>
</head>
<body>
<header>
  <div>
    <h1>Low Temp Tracker</h1>
    <div class="sub" id="h-sub">Philadelphia International Airport</div>
  </div>
  <div class="hright">
    <div class="stat-pill">
      <div class="lbl">Live Obs</div>
      <div class="val" id="h-obs" style="color:var(--yellow)">--</div>
      <div class="sub2" id="h-obs-t">awaiting</div>
    </div>
    <div class="sp"></div>
    <div class="stat-pill">
      <div class="lbl">Wethr Low</div>
      <div class="val" id="h-wl" style="color:var(--ice)">--</div>
      <div class="sub2">NWS logic</div>
    </div>
    <div class="sp"></div>
    <div class="stat-pill">
      <div class="lbl">Consensus</div>
      <div class="val" id="h-con" style="color:var(--blue)">--</div>
      <div class="sub2">MAE-weighted</div>
    </div>
    <div class="sp"></div>
    <div class="stat-pill">
      <div class="lbl">Window</div>
      <div style="font-size:11px;font-weight:600;color:var(--ice);margin-top:4px" id="h-window">--</div>
    </div>
    <div class="sp"></div>
    <div style="display:flex;gap:6px;align-items:center">
      <button id="btn-KPHL" onclick="switchStation('KPHL')" style="background:#1e40af;border:1px solid #3b82f6;color:#93c5fd;border-radius:4px;padding:5px 12px;font-size:11px;cursor:pointer;font-family:inherit;letter-spacing:1px">KPHL</button>
      <button id="btn-KATL" onclick="switchStation('KATL')" style="background:none;border:1px solid #334155;color:#64748b;border-radius:4px;padding:5px 12px;font-size:11px;cursor:pointer;font-family:inherit;letter-spacing:1px">KATL</button>
      <button id="btn-KOKC" onclick="switchStation('KOKC')" style="background:none;border:1px solid #334155;color:#64748b;border-radius:4px;padding:5px 12px;font-size:11px;cursor:pointer;font-family:inherit;letter-spacing:1px">KOKC</button>
    </div>
    <div class="sp"></div>
    <div style="text-align:right">
      <div style="display:flex;align-items:center;gap:6px;font-size:10px;color:var(--dim)">
        <span class="dot dot-yellow" id="sdot"></span><span id="stxt">Loading...</span>
      </div>
      <div style="font-size:10px;color:var(--dimmer);margin-top:3px">Next: <span id="cnt">20:00</span></div>
      <button class="btn" style="margin-top:4px;padding:3px 10px;font-size:10px" onclick="manualRefresh()">&#8635; NOW</button>
    </div>
  </div>
</header>

<nav>
  <button class="active" onclick="showTab('dashboard',this)">&#127771; Dashboard</button>
  <button onclick="showTab('entry',this)">&#9728;&#65039; Morning Entry</button>
  <button onclick="showTab('runs',this)">&#128336; Run Accuracy</button>
  <button onclick="showTab('log',this)">&#128319; Log</button>
  <button onclick="showTab('history',this)">&#128196; History</button>
  <button onclick="showTab('snapshots',this);loadSnapshots();">&#128248; Snapshots</button>
  <button onclick="showTab('verification',this);loadVerification();">&#9989; Verification</button>
</nav>

<main>

<!-- DASHBOARD -->
<div class="tab active" id="tab-dashboard">
  <div class="srow">
    <div class="sc"><div class="lbl">Current Temp</div><div class="v" id="s-obs" style="color:var(--yellow)">--</div><div class="s" id="s-obs-t">awaiting</div></div>
    <div class="sc"><div class="lbl">Wethr Low</div><div class="v" id="s-wl" style="color:var(--ice)">--</div><div class="s">NWS logic</div></div>
    <div class="sc"><div class="lbl">Consensus Low</div><div class="v" id="s-con" style="color:var(--blue)">--</div><div class="s">MAE-weighted adj</div></div>
    <div class="sc"><div class="lbl">Models Live</div><div class="v" id="s-mods" style="color:var(--purple)">--</div><div class="s">forecast runs</div></div>
    <div class="sc"><div class="lbl">Target Window</div><div style="font-size:12px;font-weight:600;color:var(--ice);margin-top:6px" id="s-window">--</div></div>
  </div>

  <div class="card">
    <div class="ctitle">
      Today's Models &mdash; Low Forecasts + Accuracy Adjustments
      <span class="pill-y" id="acc-badge" style="display:none">Enter accuracy in Morning Entry</span>
      <span class="pill-g" id="acc-loaded" style="display:none">Accuracy loaded</span>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>#</th><th>Model</th><th>Run</th><th>Fcst Low</th><th>Correction</th><th>Adj Low</th><th>Obs Pace</th><th>Low Time</th><th>MAE</th><th>RMSE</th></tr></thead>
        <tbody id="main-tbody"></tbody>
      </table>
    </div>
  </div>

  <div class="card" id="pace-card" style="display:none">
    <div class="ctitle">Model Pacing vs Current Obs (<span id="pace-obs">--</span>F)</div>
    <div class="pbars" id="pbars"></div>
    <div style="font-size:10px;color:var(--dimmer);margin-top:10px">Pace = current obs minus model forecast for this hour. Negative = model running warm (forecasting too high).</div>
  </div>

  <div class="card" id="cons-pace-card" style="display:none">
    <div class="ctitle">MAE-Weighted Consensus Pace</div>
    <div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">
      <div style="font-size:32px;font-weight:700" id="cons-pace-val">--</div>
      <div style="color:var(--dim);font-size:12px;line-height:1.6">MAE-weighted average of all model obs paces.<br>Apply to consensus low at your discretion.</div>
    </div>
    <div style="margin-top:10px;font-size:11px;color:var(--dim)">
      Implied adjusted low: <span id="cons-pace-implied" style="color:var(--ice);font-weight:600">--</span>
    </div>
  </div>

  <div class="card" id="avg-pace-card">
    <div class="ctitle">Today's Rolling Average Pace</div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Model</th><th>Avg Pace</th><th>Snapshots</th></tr></thead>
        <tbody id="avg-pace-tbody"><tr><td colspan="3" style="color:var(--dim)">Accumulating data...</td></tr></tbody>
      </table>
    </div>
  </div>

  <div class="card" id="prev-days-card">
    <div class="ctitle">Previous 3 Days Average Pace</div>
    <div style="overflow-x:auto"><table><thead id="prev-days-thead"></thead><tbody id="prev-days-tbody"><tr><td style="color:var(--dim)">No history yet</td></tr></tbody></table></div>
  </div>
</div>

<!-- MORNING ENTRY -->
<div class="tab" id="tab-entry">
  <div class="card" style="border-color:#1e3a5f">
    <div class="ctitle">Fast Import &mdash; Paste JSON from Claude</div>
    <p style="color:var(--dim);font-size:12px;line-height:1.7;margin-bottom:12px">Each morning: paste JSON for today's selected models. The model set updates automatically — add or remove models any day.</p>
    <textarea id="json-paste" placeholder="Paste JSON here..." style="width:100%;height:110px;background:#060a0e;border:1px solid #1e3a5f;border-radius:4px;color:var(--text);padding:10px;font-family:inherit;font-size:11px;resize:vertical;outline:none"></textarea>
    <div style="display:flex;gap:10px;align-items:center;margin-top:10px;flex-wrap:wrap">
      <button class="btn" onclick="loadFromJSON()">Load JSON</button>
      <span style="font-size:10px;color:var(--dim)" id="json-status"></span>
    </div>
  </div>

  <!-- DEFAULT FALLBACK ENTRY -->
  <div class="card" style="border-color:#3a2a0a">
    <div class="ctitle" style="color:var(--orange)">&#9888; Default / Fallback Run Values</div>
    <p style="color:var(--dim);font-size:12px;line-height:1.7;margin-bottom:12px">
      Set a fallback MAE &amp; Correction per model. These apply automatically whenever a model's active run
      has <em>no</em> run-specific entry — keeping it out of consensus rather than polluting it with uncalibrated data.
      <br><span style="color:var(--orange)">D</span> badge in the dashboard Correction column indicates the default is active.
    </p>
    <div style="margin-bottom:14px;padding:10px;background:#1a1a2e;border:1px solid #334155;border-radius:6px">
      <div style="font-size:10px;color:var(--orange);letter-spacing:1px;margin-bottom:6px">&#9657; PASTE FROM WETHR.NET</div>
      <div style="font-size:11px;color:var(--dim);margin-bottom:8px">Paste the accuracy table from wethr.net — all models auto-filled in one shot.</div>
      <textarea id="paste-defaults-input" rows="6" style="width:100%;background:#0f0f1a;border:1px solid #334155;color:var(--text);border-radius:4px;padding:8px;font-size:11px;font-family:monospace;box-sizing:border-box;resize:vertical" placeholder="MODEL&#9;MAE&#9;CORRECTION&#9;RMSE&#9;DAYS&#10;NBM&#9;0.7°&#9;-0.5°F&#9;1.1°&#9;6&#10;HRRR&#9;1.1°&#9;+0.1°F&#9;1.5°&#9;6&#10;..."></textarea>
      <div style="display:flex;gap:8px;margin-top:8px;align-items:center;flex-wrap:wrap">
        <button class="btn" onclick="parseAndFillDefaults()">Fill From Paste</button>
        <button class="btn" onclick="fillDefaultsFromLoaded()">Fill From Loaded Accuracy</button>
        <span id="paste-status" style="font-size:10px;color:var(--dim)"></span>
      </div>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th style="color:var(--orange)">Default MAE</th>
            <th style="color:var(--orange)">Default Correction</th>
            <th style="color:var(--dim);font-size:9px">Current Active</th>
          </tr>
        </thead>
        <tbody id="default-tbody"></tbody>
      </table>
    </div>
    <div style="display:flex;gap:10px;align-items:center;margin-top:14px;flex-wrap:wrap">
      <button class="btn btn-green" onclick="saveDefaults()">Save Defaults</button>
      <button class="btn btn-red" onclick="clearDefaults()">Clear Defaults</button>
      <span style="font-size:10px;color:var(--dim)" id="default-status"></span>
    </div>
  </div>

  <details style="margin-bottom:16px">
    <summary style="cursor:pointer;color:var(--dim);font-size:11px;letter-spacing:1px;padding:10px 0;list-style:none">&#9658; Manual entry (fallback)</summary>
    <div style="margin-top:12px">
      <div class="card">
        <div class="ctitle">Overall 7D Accuracy</div>
        <div style="overflow-x:auto">
          <table><thead><tr><th>Model</th><th>MAE</th><th>Correction</th><th>RMSE</th></tr></thead><tbody id="ov-tbody"></tbody></table>
        </div>
      </div>
      <div class="card">
        <div class="ctitle">Run-Specific Corrections</div>
        <div style="overflow-x:auto"><table><thead><tr><th>Model</th><th>00Z</th><th>03Z</th><th>06Z</th><th>09Z</th><th>12Z</th><th>15Z</th><th>18Z</th><th>21Z</th></tr></thead><tbody id="run-tbody"></tbody></table></div>
        <div style="margin-top:14px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
          <button class="btn btn-green" onclick="saveAccuracy()">Save</button>
          <button class="btn btn-red" onclick="clearAccuracy()">Clear All</button>
          <span style="font-size:10px;color:var(--dim)" id="save-status"></span>
        </div>
      </div>
    </div>
  </details>

  <div class="card" id="acc-preview" style="display:none">
    <div class="ctitle">Currently Loaded</div>
    <div style="overflow-x:auto"><table><thead><tr><th>Model</th><th>MAE</th><th>Correction</th><th>RMSE</th><th>Default MAE</th><th>Default Corr</th><th>Named Runs</th></tr></thead><tbody id="prev-tbody"></tbody></table></div>
    <div style="margin-top:10px;display:flex;gap:10px;align-items:center">
      <button class="btn btn-red" onclick="clearAccuracy()">Clear All</button>
      <span style="font-size:10px;color:var(--dim)" id="acc-loaded-time"></span>
    </div>
  </div>
</div>

<!-- RUN ACCURACY -->
<div class="tab" id="tab-runs">
  <div class="card">
    <div class="ctitle">Run-Specific Accuracy (including Default fallback)</div>
    <div style="overflow-x:auto"><table><thead><tr><th>Model</th><th class="default-col">DEFAULT</th><th>00Z</th><th>03Z</th><th>06Z</th><th>09Z</th><th>12Z</th><th>15Z</th><th>18Z</th><th>21Z</th></tr></thead><tbody id="runview-tbody"></tbody></table></div>
    <div class="ctitle" style="margin-top:20px">Current Run per Model</div>
    <div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:8px" id="run-cards"></div>
  </div>
</div>

<!-- LOG -->
<div class="tab" id="tab-log">
  <div class="card">
    <div class="ctitle">Fetch Log</div>
    <div class="logbox" id="logbox"><div style="color:var(--dimmer)">No entries yet.</div></div>
  </div>
</div>

<!-- HISTORY -->
<div class="tab" id="tab-history">
  <div class="card">
    <div class="ctitle">Daily Pacing History</div>
    <p style="color:var(--dim);font-size:11px;margin-bottom:12px">Average pace per model for each completed low period.</p>
    <div style="overflow-x:auto"><table><thead id="hist-thead"></thead><tbody id="hist-tbody"></tbody></table></div>
    <div style="font-size:10px;color:var(--dimmer);margin-top:10px" id="hist-count"></div>
  </div>
</div>

<!-- SNAPSHOTS -->
<div class="tab" id="tab-snapshots">
  <div class="card">
    <div class="ctitle">Today's Consensus Low Snapshots</div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Time</th><th>Consensus Low</th><th>Implied Adj Low</th><th>Pace Adj</th><th>Obs</th></tr></thead>
        <tbody id="snap-tbody"><tr><td colspan="5" style="color:var(--dim)">No snapshots yet today.</td></tr></tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <div class="ctitle">Historical Consensus Snapshots</div>
    <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px">
      <select id="snap-date-select" style="background:var(--bg);border:1px solid var(--border);color:var(--text);padding:5px 8px;border-radius:4px;font-family:inherit;font-size:12px" onchange="loadSnapshotDate()">
        <option value="">Select date...</option>
      </select>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Time</th><th>Consensus Low</th><th>Implied Adj Low</th><th>Pace Adj</th><th>Obs</th></tr></thead>
        <tbody id="snap-hist-tbody"><tr><td colspan="5" style="color:var(--dim)">Select a date above.</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

<!-- VERIFICATION TAB -->
<div class="tab" id="tab-verification">
  <div class="card" style="border-color:#1e3a5f">
    <div class="ctitle">Enter Previous Day Actual Low</div>
    <p style="color:var(--dim);font-size:12px;line-height:1.7;margin-bottom:12px">Enter after the CLI report (~10-11AM). This calibrates your consensus snapshots against reality.</p>
    <div style="display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap">
      <div>
        <div style="font-size:10px;color:var(--dim);letter-spacing:1px;margin-bottom:4px">DATE</div>
        <input type="text" id="verif-date" placeholder="YYYY-MM-DD" style="background:var(--bg);border:1px solid #1e2e42;border-radius:4px;color:var(--text);padding:6px 10px;font-family:inherit;font-size:12px;outline:none;width:130px">
      </div>
      <div>
        <div style="font-size:10px;color:var(--dim);letter-spacing:1px;margin-bottom:4px">ACTUAL LOW (°F)</div>
        <input type="number" step="0.1" id="verif-actual" placeholder="e.g. 58.2" style="width:120px;background:var(--bg);border:1px solid #1e2e42;border-radius:4px;color:var(--text);padding:6px 10px;font-family:inherit;font-size:12px;outline:none">
      </div>
      <button class="btn btn-green" onclick="submitVerification()">Save</button>
      <span style="font-size:10px;color:var(--dim)" id="verif-status"></span>
    </div>
  </div>
  <div class="card" id="verif-results" style="display:none">
    <div class="ctitle">Calibration Results</div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Time</th><th>Consensus</th><th>Implied Adj</th><th>Actual</th><th>Error</th><th>Abs Error</th></tr></thead>
        <tbody id="verif-tbody"></tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <div class="ctitle">Calibration History</div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Date</th><th>Actual</th><th>Snapshots</th><th>Avg Error</th><th>Avg Abs Error</th><th>Best Snap</th></tr></thead>
        <tbody id="verif-hist-tbody"><tr><td colspan="6" style="color:var(--dim)">No verification data yet.</td></tr></tbody>
      </table>
    </div>
  </div>
</div>
</main>

<script>
var STATION = localStorage.getItem("active_station_lows") || "KPHL";
var MODELS = [];
var accData = {};
try { accData = JSON.parse(localStorage.getItem("acc_lows_"+STATION) || "{}"); } catch(e){}
if(Object.keys(accData).length) MODELS = Object.keys(accData).filter(function(m){ return m !== "NWS"; });
var countdown = 1200;
var countdownTimer;

function clearDisplay(){
  ["h-obs","h-wl","h-con","s-obs","s-wl","s-con"].forEach(function(id){
    var el = document.getElementById(id); if(el) el.textContent="--";
  });
  ["h-obs-t","s-obs-t"].forEach(function(id){
    var el = document.getElementById(id); if(el) el.textContent="awaiting";
  });
  var tbody = document.getElementById("main-tbody"); if(tbody) tbody.innerHTML="";
  var pbars = document.getElementById("pbars"); if(pbars) pbars.innerHTML="";
  var pace = document.getElementById("pace-card"); if(pace) pace.style.display="none";
  document.getElementById("stxt").textContent="Switching...";
}

function switchStation(s){
  STATION = s;
  localStorage.setItem("active_station_lows", s);
  clearDisplay();
  try { accData = JSON.parse(localStorage.getItem("acc_lows_"+s) || "{}"); } catch(e){ accData = {}; }
  MODELS = Object.keys(accData).filter(function(m){ return m !== "NWS"; });
  ["KPHL","KATL","KOKC"].forEach(function(st){
    var btn = document.getElementById("btn-"+st);
    if(st === s){
      btn.style.background="#1e40af"; btn.style.borderColor="#3b82f6"; btn.style.color="#93c5fd";
    } else {
      btn.style.background="none"; btn.style.borderColor="#334155"; btn.style.color="#64748b";
    }
  });
  var names = {"KPHL":"Philadelphia International Airport","KATL":"Atlanta Hartsfield-Jackson Airport","KOKC":"Oklahoma City Will Rogers World Airport"};
  document.getElementById("h-sub").textContent = names[s] || s;
  buildForms(); buildDefaultForm(); renderPreview(); poll();
}

var MANUAL_RUNS = ["00Z","03Z","06Z","09Z","12Z","15Z","18Z","21Z"];

function fmt1(v){ return (v==null||v==="") ? "--" : Number(v).toFixed(1); }
function fmtC(v){
  if(v==null||v==="") return "--";
  var n=Number(v); return (n>=0?"+":"")+n.toFixed(1)+"F";
}
function corrColor(v){
  if(v==null||v==="") return "var(--dim)";
  return Number(v)>0?"#60a5fa":Number(v)<0?"#f87171":"var(--dim)";
}
function maeColor(v){
  if(v==null||v==="") return "var(--dim)";
  var n=Number(v); return n<=1?"var(--green)":n<=2?"var(--yellow)":"var(--red)";
}
function paceColor(v){
  var n=Math.abs(Number(v)); return n<=1?"var(--green)":n<=3?"var(--yellow)":"var(--red)";
}

function showTab(id,btn){
  document.querySelectorAll(".tab").forEach(function(t){t.classList.remove("active");});
  document.querySelectorAll("nav button").forEach(function(b){b.classList.remove("active");});
  document.getElementById("tab-"+id).classList.add("active");
  btn.classList.add("active");
}

function buildForms(){
  var ov = document.getElementById("ov-tbody");
  if(!ov) return;
  var mods = MODELS.length ? MODELS : ["HRRR","ARPEGE","NAM","UKMO","LAV-MOS","RAP","GEM-GDPS","NAM-MOS","NBM","NAM4KM"];
  ov.innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var bg = i%2?"background:#0a1018":"";
    return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
      +'<td><input type="number" step="0.1" placeholder="0.0" id="ov-mae-'+m+'" value="'+(a.mae||"")+'"></td>'
      +'<td><input type="number" step="0.1" placeholder="0.0" id="ov-corr-'+m+'" value="'+(a.correction||"")+'"></td>'
      +'<td><input type="number" step="0.1" placeholder="0.0" id="ov-rmse-'+m+'" value="'+(a.rmse||"")+'"></td></tr>';
  }).join("");
  var rb = document.getElementById("run-tbody");
  rb.innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var bg = i%2?"background:#0a1018":"";
    var cells = MANUAL_RUNS.map(function(r){
      var rd = (a.runs||{})[r]||{};
      return '<td style="padding:5px 6px"><div style="display:flex;flex-direction:column;gap:3px">'
        +'<input type="number" step="0.1" placeholder="MAE" style="width:56px;font-size:11px" id="rm-mae-'+m+'-'+r+'" value="'+(rd.mae||"")+'"><br>'
        +'<input type="number" step="0.1" placeholder="Corr" style="width:56px;font-size:11px" id="rm-corr-'+m+'-'+r+'" value="'+(rd.correction||"")+'"></div></td>';
    }).join("");
    return '<tr style="'+bg+'"><td style="color:#8aabcc;font-weight:600">'+m+'</td>'+cells+'</tr>';
  }).join("");
}

// --- DEFAULT FALLBACK FORM ---
function buildDefaultForm(){
  var mods = MODELS.length ? MODELS : ["HRRR","ARPEGE","NAM","UKMO","LAV-MOS","RAP","GEM-GDPS","NAM-MOS","NBM","NAM4KM"];
  var tbody = document.getElementById("default-tbody");
  if(!tbody) return;
  tbody.innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var rd = (a.runs||{})["default"]||{};
    var bg = i%2?"background:#0a1018":"";
    var namedRuns = Object.keys(a.runs||{}).filter(function(r){ return r!=="default"; }).join(", ")||"none";
    return '<tr style="'+bg+'">'
      +'<td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
      +'<td class="default-col"><input type="number" step="0.1" placeholder="e.g. 1.5" style="width:80px" id="def-mae-'+m+'" value="'+(rd.mae||"")+'"></td>'
      +'<td class="default-col"><input type="number" step="0.1" placeholder="e.g. +0.5" style="width:80px" id="def-corr-'+m+'" value="'+(rd.correction||"")+'"></td>'
      +'<td style="color:var(--dim);font-size:11px">'+namedRuns+'</td>'
      +'</tr>';
  }).join("");
}

function fillDefaultsFromLoaded(){
  var status = document.getElementById("paste-status");
  var filled = 0;
  Object.keys(accData).forEach(function(model){
    if(model === "NWS") return;
    var a = accData[model] || {};
    var mae = parseFloat(a.mae);
    var corr = parseFloat(a.correction);
    if(isNaN(mae) && isNaN(corr)) return;
    var maeEl = document.getElementById("def-mae-"+model);
    var corrEl = document.getElementById("def-corr-"+model);
    if(maeEl && !isNaN(mae)) maeEl.value = mae;
    if(corrEl && !isNaN(corr)) corrEl.value = corr;
    if(maeEl || corrEl) filled++;
  });
  status.style.color = filled > 0 ? "var(--green)" : "var(--red)";
  status.textContent = filled + " models filled from loaded accuracy — hit Save Defaults to commit.";
}

function parseAndFillDefaults(){
  var raw = (document.getElementById("paste-defaults-input").value || "").trim();
  var status = document.getElementById("paste-status");
  if(!raw){ status.textContent = "Nothing to parse."; return; }
  var aliases = {
    "NBS-MOS":"NBS-MOS","NBSMOS":"NBS-MOS",
    "GFS-MOS":"GFS-MOS","GFSMOS":"GFS-MOS",
    "LAV-MOS":"LAV-MOS","LAVMOS":"LAV-MOS",
    "NAM-MOS":"NAM-MOS","NAMMOS":"NAM-MOS",
    "GEM-GDPS":"GEM-GDPS","GEMGDPS":"GEM-GDPS",
    "GEM-HRDPS":"GEM-HRDPS","GEMHRDPS":"GEM-HRDPS",
    "ECMWF-IFS":"ECMWF-IFS","ECMWFIFS":"ECMWF-IFS",
    "ECMWF-HRES":"ECMWF-HRES","ECMWFHRES":"ECMWF-HRES",
  };
  var filled = 0, skipped = 0;
  raw.split("\n").forEach(function(line){
    line = line.trim();
    if(!line || line.toLowerCase().startsWith("model")) return;
    var cols = line.split(/\t+|\s{2,}/);
    if(cols.length < 3) return;
    var rawModel = cols[0].trim().toUpperCase();
    var model = aliases[rawModel] || rawModel;
    var mae = parseFloat(cols[1].replace(/[°\s]/g,""));
    var corr = parseFloat(cols[2].replace(/[°F\s]/g,""));
    if(isNaN(mae) || isNaN(corr)){ skipped++; return; }
    var maeEl = document.getElementById("def-mae-"+model);
    var corrEl = document.getElementById("def-corr-"+model);
    if(maeEl && corrEl){
      maeEl.value = mae; corrEl.value = corr; filled++;
    } else {
      if(!accData[model]) accData[model] = {runs:{}};
      if(!accData[model].runs) accData[model].runs = {};
      accData[model].runs["default"] = {mae:mae, correction:corr};
      filled++;
    }
  });
  status.style.color = filled > 0 ? "var(--green)" : "var(--red)";
  status.textContent = filled+" models filled"+(skipped?", "+skipped+" skipped":"")+" — hit Save Defaults to commit.";
}

function saveDefaults(){
  var mods = MODELS.length ? MODELS : [];
  var status = document.getElementById("default-status");
  mods.forEach(function(m){
    if(!accData[m]) accData[m] = {};
    if(!accData[m].runs) accData[m].runs = {};
    var maeEl = document.getElementById("def-mae-"+m);
    var corrEl = document.getElementById("def-corr-"+m);
    var mae = maeEl ? maeEl.value : "";
    var corr = corrEl ? corrEl.value : "";
    if(mae || corr){
      accData[m].runs["default"] = { mae: mae, correction: corr };
    } else {
      delete accData[m].runs["default"];
    }
  });
  localStorage.setItem("acc_lows_"+STATION, JSON.stringify(accData));
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(accData)})
    .then(function(r){ return r.json(); })
    .then(function(){
      status.style.color="var(--green)";
      status.textContent = "Defaults saved at "+new Date().toLocaleTimeString();
      renderPreview();
    }).catch(function(e){
      localStorage.setItem("acc_lows_"+STATION, JSON.stringify(accData));
      status.style.color="var(--yellow)";
      status.textContent = "Saved locally (server: "+e.message+")";
      renderPreview();
    });
}

function clearDefaults(){
  if(!confirm("Clear all default fallback values?")) return;
  var mods = MODELS.length ? MODELS : [];
  mods.forEach(function(m){
    if(accData[m] && accData[m].runs) delete accData[m].runs["default"];
  });
  localStorage.setItem("acc_lows_"+STATION, JSON.stringify(accData));
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(accData)});
  buildDefaultForm();
  document.getElementById("default-status").textContent = "Defaults cleared";
  renderPreview();
}

function loadFromJSON(){
  var raw = document.getElementById("json-paste").value.trim();
  var status = document.getElementById("json-status");
  if(!raw){ status.style.color="var(--red)"; status.textContent="Nothing to paste."; return; }
  try {
    var parsed = JSON.parse(raw);
    var keys = Object.keys(parsed);
    if(!keys.length){ status.style.color="var(--red)"; status.textContent="No models found."; return; }
    keys.forEach(function(m){
      if(accData[m] && accData[m].runs && accData[m].runs["default"]){
        if(!parsed[m].runs) parsed[m].runs = {};
        if(!parsed[m].runs["default"]){
          parsed[m].runs["default"] = accData[m].runs["default"];
        }
      }
    });
    accData = parsed;
    MODELS = keys.filter(function(m){ return m !== "NWS"; });
    localStorage.setItem("acc_lows_"+STATION, JSON.stringify(parsed));
    localStorage.setItem("acc_lows_"+STATION+"_time", new Date().toLocaleString());
    fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(parsed)})
      .then(function(r){
        if(!r.ok) throw new Error("HTTP "+r.status);
        status.style.color="var(--green)";
        status.textContent = "Loaded "+keys.length+" models at "+new Date().toLocaleTimeString();
        document.getElementById("json-paste").value="";
        buildForms(); buildDefaultForm(); renderPreview(); poll();
      }).catch(function(e){
        status.style.color="var(--yellow)";
        status.textContent = "Saved locally (server: "+e.message+"). Will sync on next refresh.";
        buildForms(); buildDefaultForm(); renderPreview();
      });
  } catch(e) {
    status.style.color="var(--red)"; status.textContent="Invalid JSON: "+e.message;
  }
}

function saveAccuracy(){
  var mods = MODELS.length ? MODELS : [];
  var data = {};
  mods.forEach(function(m){
    data[m] = {
      mae: document.getElementById("ov-mae-"+m) ? document.getElementById("ov-mae-"+m).value : "",
      correction: document.getElementById("ov-corr-"+m) ? document.getElementById("ov-corr-"+m).value : "",
      rmse: document.getElementById("ov-rmse-"+m) ? document.getElementById("ov-rmse-"+m).value : "",
      runs: {}
    };
    MANUAL_RUNS.forEach(function(r){
      var mae_el = document.getElementById("rm-mae-"+m+"-"+r);
      var corr_el = document.getElementById("rm-corr-"+m+"-"+r);
      data[m].runs[r] = { mae: mae_el ? mae_el.value : "", correction: corr_el ? corr_el.value : "" };
    });
    if(accData[m] && accData[m].runs && accData[m].runs["default"]){
      data[m].runs["default"] = accData[m].runs["default"];
    }
  });
  accData = data;
  localStorage.setItem("acc_lows_"+STATION, JSON.stringify(data));
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)})
    .then(function(){ document.getElementById("save-status").textContent="Saved "+new Date().toLocaleTimeString(); });
}

function clearAccuracy(){
  if(!confirm("Clear all accuracy data?")) return;
  accData = {}; MODELS = [];
  localStorage.removeItem("acc_lows_"+STATION); localStorage.removeItem("acc_lows_"+STATION+"_time");
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({})});
  buildForms(); buildDefaultForm(); renderPreview();
  document.getElementById("save-status").textContent="Cleared";
}

function renderPreview(){
  var hasAny = Object.keys(accData).some(function(m){ return accData[m] && accData[m].mae; });
  var el = document.getElementById("acc-preview");
  document.getElementById("acc-badge").style.display = hasAny ? "none" : "inline";
  document.getElementById("acc-loaded").style.display = hasAny ? "inline" : "none";
  if(!hasAny){ el.style.display="none"; return; }
  el.style.display="block";
  var t = localStorage.getItem("acc_lows_"+STATION+"_time");
  if(t) document.getElementById("acc-loaded-time").textContent="Loaded: "+t;
  var mods = Object.keys(accData);
  document.getElementById("prev-tbody").innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var defRd = (a.runs||{})["default"]||{};
    var namedRuns = Object.keys(a.runs||{}).filter(function(r){ return r!=="default"; }).filter(function(r){ return (a.runs||{})[r].mae||(a.runs||{})[r].correction; }).join(", ")||"--";
    var bg = i%2?"background:#0a1018":"";
    return '<tr style="'+bg+'">'
      +'<td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
      +'<td style="color:'+maeColor(a.mae)+'">'+(a.mae?fmt1(a.mae)+"F":"--")+'</td>'
      +'<td style="color:'+corrColor(a.correction)+'">'+(a.correction!=null&&a.correction!==""?fmtC(a.correction):"--")+'</td>'
      +'<td style="color:var(--dim)">'+(a.rmse?fmt1(a.rmse)+"F":"--")+'</td>'
      +'<td style="color:'+(defRd.mae?maeColor(defRd.mae):"var(--dimmer)")+'">'+(defRd.mae?fmt1(defRd.mae)+"F":'<span style="color:#2a3a50">—</span>')+'</td>'
      +'<td style="color:'+(defRd.correction!=null&&defRd.correction!==""?corrColor(defRd.correction):"var(--dimmer)")+'">'+(defRd.correction!=null&&defRd.correction!==""?fmtC(defRd.correction):'<span style="color:#2a3a50">—</span>')+'</td>'
      +'<td style="color:var(--dim);font-size:11px">'+namedRuns+'</td></tr>';
  }).join("");
}

function render(data){
  if(data.models && data.models.length) MODELS = data.models.filter(function(m){ return m!=="NWS"; });
  var obs = data.obs;
  var wl = data.wethr_low;
  var rows = data.rows||[];
  var con = data.consensus;
  if(obs){
    var t = obs.temperature_display;
    document.getElementById("h-obs").textContent = t+"F";
    document.getElementById("s-obs").textContent = t+"F";
    var ot = (obs.observation_time||"").slice(11,16)||"--";
    document.getElementById("h-obs-t").textContent = ot;
    document.getElementById("s-obs-t").textContent = ot;
    document.getElementById("pace-obs").textContent = t;
  }
  if(wl && wl.wethr_low != null){
    document.getElementById("h-wl").textContent = wl.wethr_low+"F";
    document.getElementById("s-wl").textContent = wl.wethr_low+"F";
  }
  if(con){
    document.getElementById("h-con").textContent = con+"F";
    document.getElementById("s-con").textContent = con+"F";
  }
  if(data.window_label){
    document.getElementById("h-window").textContent = data.window_label;
    document.getElementById("s-window").textContent = data.window_label;
  }
  document.getElementById("s-mods").textContent = rows.filter(function(r){ return r.raw_low!=null; }).length+"/"+rows.length;

  document.getElementById("main-tbody").innerHTML = rows.map(function(r,i){
    var bg = i%2?"background:#0a1018":"";
    var corrBadge = "";
    if(r.corr_source === "run") corrBadge = ' <span style="font-size:9px;color:#38bdf8">R</span>';
    else if(r.corr_source === "default") corrBadge = ' <span style="font-size:9px;color:var(--orange);font-weight:700" title="Using default fallback">D</span>';
    return '<tr style="'+bg+'">'
      +'<td style="color:var(--dim)">#'+r.rank+'</td>'
      +'<td style="color:#e8f0f8;font-weight:600">'+r.model+'</td>'
      +'<td style="color:var(--dim);font-size:11px">'+(r.run||"--")+'</td>'
      +'<td style="color:var(--ice)">'+(r.raw_low!=null?r.raw_low+"F":"--")+'</td>'
      +'<td style="color:'+corrColor(r.correction)+'">'+(r.correction!=null&&r.correction!==""?fmtC(r.correction)+corrBadge:"--")+'</td>'
      +'<td style="color:var(--blue);font-weight:600">'+(r.adj_low!=null?r.adj_low+"F":"--")+'</td>'
      +'<td style="color:'+(r.pace!=null?paceColor(r.pace):"#1e2e42")+'">'+(r.pace!=null?(r.pace>=0?"+":"")+r.pace+"F":"--")+'</td>'
      +'<td style="color:var(--dim);font-size:11px">'+(r.low_time||"--")+'</td>'
      +'<td style="color:'+maeColor(r.mae)+'">'+(r.mae?fmt1(r.mae)+"F":"--")+'</td>'
      +'<td style="color:var(--dim)">'+(r.rmse?fmt1(r.rmse)+"F":"--")+'</td></tr>';
  }).join("");

  var paceRows = rows.filter(function(r){ return r.pace!=null; });
  if(paceRows.length && obs){
    document.getElementById("pace-card").style.display="block";
    document.getElementById("pbars").innerHTML = paceRows.map(function(r){
      var p=Number(r.pace); var w=Math.min(Math.abs(p)*14,140);
      var col=p>=0?"var(--green)":"var(--red)";
      return '<div class="prow"><div class="plabel">'+r.model+'</div>'
        +'<div style="width:160px"><div class="pbar" style="width:'+w+'px;background:'+col+'33;border:1px solid '+col+'"></div></div>'
        +'<span style="font-size:11px;color:'+paceColor(r.pace)+';font-weight:600">'+(p>=0?"+":"")+r.pace+'F</span></div>';
    }).join("");
  }

  document.getElementById("runview-tbody").innerHTML = rows.map(function(r,i){
    var bg = i%2?"background:#0a1018":"";
    var defRd = (r.runs||{})["default"]||{};
    var defCell = (defRd.mae||defRd.correction)
      ?'<td class="default-col" style="text-align:center"><div style="line-height:1.8">'
        +(defRd.mae?'<div style="color:'+maeColor(defRd.mae)+'">'+fmt1(defRd.mae)+'F</div>':'')
        +(defRd.correction!=null&&defRd.correction!==""?'<div style="color:'+corrColor(defRd.correction)+'">'+fmtC(defRd.correction)+'</div>':'')
        +'</div></td>'
      :'<td class="default-col" style="text-align:center"><span style="color:#1e2e42">--</span></td>';
    var cells = MANUAL_RUNS.map(function(run){
      var rd = (r.runs||{})[run]||{};
      var has = rd.mae||rd.correction;
      return '<td style="text-align:center">'+(has
        ?'<div style="line-height:1.8">'+(rd.mae?'<div style="color:'+maeColor(rd.mae)+'">'+fmt1(rd.mae)+'F</div>':'')+
          (rd.correction!=null&&rd.correction!==""?'<div style="color:'+corrColor(rd.correction)+'">'+fmtC(rd.correction)+'</div>':'')+'</div>'
        :'<span style="color:#1e2e42">--</span>')+'</td>';
    }).join("");
    return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+r.model+'</td>'+defCell+cells+'</tr>';
  }).join("");

  document.getElementById("run-cards").innerHTML = rows.map(function(r){
    var runKey = r.run ? r.run.replace(/[^0-9]/g,"").slice(0,2)+"Z" : "";
    var rd = (r.runs||{})[runKey]||{};
    var hasC = rd.correction!=null&&rd.correction!=="";
    var usingDefault = !hasC && (r.runs||{})["default"] && ((r.runs||{})["default"].correction!=null&&(r.runs||{})["default"].correction!=="");
    var defRd = (r.runs||{})["default"]||{};
    return '<div style="background:#0b1520;border:1px solid '+(usingDefault?"var(--orange)":"var(--border)")+';border-radius:5px;padding:8px 12px;min-width:120px">'
      +'<div style="font-size:11px;color:#8aabcc;font-weight:600">'+r.model+'</div>'
      +'<div style="font-size:13px;color:var(--ice);margin-top:2px">'+(r.run||"--")+'</div>'
      +(hasC?'<div style="font-size:11px;color:'+corrColor(rd.correction)+';margin-top:2px">Corr: '+fmtC(rd.correction)+' <span style="font-size:9px;color:#38bdf8">R</span></div>'
        :usingDefault?'<div style="font-size:11px;color:var(--orange);margin-top:2px">Default: '+fmtC(defRd.correction)+' <span style="font-size:9px">D</span></div>'
        :'<div style="font-size:10px;color:#2a4060;margin-top:2px">No run corr</div>')
      +'</div>';
  }).join("");

  if(data.log&&data.log.length){
    document.getElementById("logbox").innerHTML = data.log.map(function(e){
      var col = e.level==="ok"?"var(--green)":e.level==="err"?"var(--red)":e.level==="warn"?"var(--yellow)":"var(--dim)";
      return '<div style="margin-bottom:5px"><span style="color:var(--dimmer)">['+e.t+']</span> <span style="color:'+col+'">'+e.msg+'</span></div>';
    }).join("");
  }

  var consPace = data.consensus_pace;
  var consPaceCard = document.getElementById("cons-pace-card");
  if(consPace != null && obs){
    consPaceCard.style.display = "block";
    var cpEl = document.getElementById("cons-pace-val");
    cpEl.textContent = (consPace >= 0 ? "+" : "") + consPace + "F";
    cpEl.style.color = consPace >= 0 ? "var(--green)" : "var(--red)";
    var implied = con ? (Math.round((parseFloat(con) + consPace) * 10) / 10) + "F" : "--";
    document.getElementById("cons-pace-implied").textContent = implied;
  } else { consPaceCard.style.display = "none"; }

  var avgPace = data.today_avg_pace || {};
  var avgModels = Object.keys(avgPace);
  var todaySnaps = data.today_snapshot_count || 0;
  if(avgModels.length){
    document.getElementById("avg-pace-tbody").innerHTML = avgModels.map(function(m,i){
      var p = avgPace[m];
      var bg = i%2?"background:#0a1018":"";
      return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
        +'<td style="color:'+paceColor(p)+';font-weight:600">'+(p>=0?"+":"")+p.toFixed(2)+'F</td>'
        +'<td style="color:var(--dim)">'+todaySnaps+'</td></tr>';
    }).join("");
  } else {
    document.getElementById("avg-pace-tbody").innerHTML = '<tr><td colspan="3" style="color:var(--dim)">Accumulating — updates every 20 min</td></tr>';
  }

  var prevDays = data.prev_days || [];
  if(prevDays.length){
    var allModels = [];
    prevDays.forEach(function(d){ Object.keys(d.avg_pace).forEach(function(m){ if(!allModels.includes(m)) allModels.push(m); }); });
    document.getElementById("prev-days-thead").innerHTML = '<tr><th>Model</th>'+prevDays.map(function(d){ return '<th>'+d.date.slice(5)+'</th>'; }).join("")+'</tr>';
    document.getElementById("prev-days-tbody").innerHTML = allModels.map(function(m,i){
      var bg = i%2?"background:#0a1018":"";
      var cells = prevDays.map(function(d){
        var p = d.avg_pace[m];
        if(p==null) return '<td style="color:#1e2e42">--</td>';
        return '<td style="color:'+paceColor(p)+';font-weight:600">'+(p>=0?"+":"")+p.toFixed(2)+'F</td>';
      }).join("");
      return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+m+'</td>'+cells+'</tr>';
    }).join("");
  } else {
    document.getElementById("prev-days-thead").innerHTML = '';
    document.getElementById("prev-days-tbody").innerHTML = '<tr><td style="color:var(--dim)">No history yet — builds after first full day</td></tr>';
  }

  document.getElementById("sdot").className = "dot "+(data.errors&&data.errors.length?"dot-yellow":"dot-green");
  document.getElementById("stxt").textContent = data.last_updated?"Updated "+data.last_updated.slice(11,16):"Live";
}

function poll(){
  fetch("/api/state?station="+STATION).then(function(r){ return r.json(); }).then(render).catch(function(e){ console.error(e); });
}

function manualRefresh(){
  fetch("/api/refresh?station="+STATION,{method:"POST"});
  countdown=1200;
  document.getElementById("stxt").textContent="Fetching...";
  setTimeout(poll,5000);
  setTimeout(poll,20000);
  setTimeout(poll,40000);
  setTimeout(poll,60000);
  setTimeout(poll,90000);
  setTimeout(poll,120000);
  setTimeout(poll,150000);
}

function startCountdown(){
  clearInterval(countdownTimer);
  countdown=1200;
  countdownTimer=setInterval(function(){
    countdown=Math.max(0,countdown-1);
    var m=Math.floor(countdown/60); var s=String(countdown%60).padStart(2,"0");
    document.getElementById("cnt").textContent=m+":"+s;
    if(countdown===0){ poll(); countdown=1200; }
  },1000);
}

buildForms(); buildDefaultForm(); renderPreview();
startCountdown();
if(Object.keys(accData).length){
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(accData)})
    .then(function(){ poll(); }).catch(function(){ poll(); });
} else {
  poll();
}

document.addEventListener("visibilitychange", function(){
  if(document.visibilityState === "visible"){ poll(); }
});
window.addEventListener("focus", function(){ poll(); });

var _snapData = {};
function loadSnapshots(){
  fetch("/api/consensus_snapshots?station="+STATION)
    .then(function(r){ return r.json(); })
    .then(function(data){
      _snapData = data.history || {};
      var today = data.today || [];
      var tbody = document.getElementById("snap-tbody");
      if(today.length){
        tbody.innerHTML = today.slice().reverse().map(function(s,i){
          var bg = i%2?"background:#0a1018":"";
          var pc = s.pace!=null?(s.pace>=0?"var(--green)":"var(--red)"):"var(--dim)";
          var paceStr = s.pace!=null?(s.pace>=0?"+":"")+s.pace+"F":"--";
          return '<tr style="'+bg+'">'
            +'<td style="color:var(--dim)">'+s.time+'</td>'
            +'<td style="color:var(--blue);font-weight:600">'+(s.consensus!=null?s.consensus+"F":"--")+'</td>'
            +'<td style="color:var(--ice);font-weight:600">'+(s.implied!=null?s.implied+"F":"--")+'</td>'
            +'<td style="color:'+pc+'">'+paceStr+'</td>'
            +'<td style="color:var(--yellow)">'+(s.obs!=null?s.obs+"F":"--")+'</td>'
            +'</tr>';
        }).join("");
      } else {
        tbody.innerHTML = '<tr><td colspan="5" style="color:var(--dim)">No snapshots yet today.</td></tr>';
      }
      var dates = Object.keys(_snapData).sort().reverse();
      var sel = document.getElementById("snap-date-select");
      sel.innerHTML = '<option value="">Select date...</option>' +
        dates.map(function(d){ return '<option value="'+d+'">'+d+'</option>'; }).join("");
    }).catch(function(e){ console.error("Snapshot load error",e); });
}

function loadSnapshotDate(){
  var date = document.getElementById("snap-date-select").value;
  var tbody = document.getElementById("snap-hist-tbody");
  if(!date || !_snapData[date]){
    tbody.innerHTML = '<tr><td colspan="5" style="color:var(--dim)">No data for this date.</td></tr>';
    return;
  }
  var snaps = _snapData[date].slice().reverse();
  tbody.innerHTML = snaps.map(function(s,i){
    var bg = i%2?"background:#0a1018":"";
    var pc = s.pace!=null?(s.pace>=0?"var(--green)":"var(--red)"):"var(--dim)";
    var paceStr = s.pace!=null?(s.pace>=0?"+":"")+s.pace+"F":"--";
    return '<tr style="'+bg+'">'
      +'<td style="color:var(--dim)">'+s.time+'</td>'
      +'<td style="color:var(--blue);font-weight:600">'+(s.consensus!=null?s.consensus+"F":"--")+'</td>'
      +'<td style="color:var(--ice);font-weight:600">'+(s.implied!=null?s.implied+"F":"--")+'</td>'
      +'<td style="color:'+pc+'">'+paceStr+'</td>'
      +'<td style="color:var(--yellow)">'+(s.obs!=null?s.obs+"F":"--")+'</td>'
      +'</tr>';
  }).join("");
}

function loadHistory(){
  fetch("/api/history?station="+STATION).then(function(r){ return r.json(); }).then(function(history){
    var dates = Object.keys(history).sort().reverse();
    var thead = document.getElementById("hist-thead");
    var tbody = document.getElementById("hist-tbody");
    var countEl = document.getElementById("hist-count");
    if(!dates.length){
      tbody.innerHTML = '<tr><td colspan="2" style="color:var(--dim)">No history yet.</td></tr>';
      return;
    }
    var allModels = [];
    dates.forEach(function(d){ Object.keys(history[d].avg_pace).forEach(function(m){ if(!allModels.includes(m)) allModels.push(m); }); });
    thead.innerHTML = '<tr><th>Model</th>'+dates.map(function(d){ return '<th>'+d+'</th>'; }).join("")+'</tr>';
    tbody.innerHTML = allModels.map(function(m,i){
      var bg = i%2?"background:#0a1018":"";
      var cells = dates.map(function(d){
        var p = history[d].avg_pace[m];
        if(p==null) return '<td style="color:#1e2e42">--</td>';
        return '<td style="color:'+paceColor(p)+';font-weight:600">'+(p>=0?"+":"")+p.toFixed(2)+'F</td>';
      }).join("");
      return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+m+'</td>'+cells+'</tr>';
    }).join("");
    countEl.textContent = dates.length+" days stored";
  }).catch(function(e){ console.error("History load error",e); });
}

document.querySelectorAll("nav button").forEach(function(btn){
  btn.addEventListener("click", function(){
    if(btn.textContent.includes("History")) loadHistory();
  });
});

function loadVerification(){
  var d = new Date(); d.setDate(d.getDate()-1);
  var ds = d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0");
  document.getElementById("verif-date").value = ds;
  fetch("/api/verification?station="+STATION)
    .then(function(r){ return r.json(); })
    .then(function(data){
      var dates = Object.keys(data).sort().reverse();
      var tbody = document.getElementById("verif-hist-tbody");
      if(!dates.length){
        tbody.innerHTML = '<tr><td colspan="6" style="color:var(--dim)">No verification data yet.</td></tr>';
        return;
      }
      tbody.innerHTML = dates.map(function(d,i){
        var v = data[d];
        var cal = v.calibration || [];
        var avgErr = cal.length ? round1(cal.reduce(function(a,b){ return a+b.error; },0)/cal.length) : "--";
        var avgAbs = cal.length ? round1(cal.reduce(function(a,b){ return a+b.abs_error; },0)/cal.length) : "--";
        var best = cal.length ? cal.reduce(function(a,b){ return a.abs_error < b.abs_error ? a : b; }) : null;
        var bestStr = best ? best.time+" ("+fmtC(best.error)+")" : "--";
        var bg = i%2?"background:#0a1018":"";
        var ec = typeof avgErr === "number" ? corrColor(avgErr) : "var(--dim)";
        return '<tr style="'+bg+'">'
          +'<td style="color:#e8f0f8">'+d+'</td>'
          +'<td style="color:var(--ice)">'+v.actual+'F</td>'
          +'<td style="color:var(--dim)">'+v.snapshot_count+'</td>'
          +'<td style="color:'+ec+'">'+(typeof avgErr==="number"?(avgErr>=0?"+":"")+avgErr+"F":"--")+'</td>'
          +'<td style="color:'+maeColor(avgAbs)+'">'+(typeof avgAbs==="number"?avgAbs+"F":"--")+'</td>'
          +'<td style="color:var(--dim);font-size:11px">'+bestStr+'</td>'
          +'</tr>';
      }).join("");
    }).catch(function(e){ console.error("Verification load error",e); });
}
function round1(v){ return Math.round(v*10)/10; }
function submitVerification(){
  var date = document.getElementById("verif-date").value;
  var actual = document.getElementById("verif-actual").value;
  var status = document.getElementById("verif-status");
  if(!actual){ status.style.color="var(--red)"; status.textContent="Actual value required."; return; }
  if(!date || date.length < 8){
    var d2 = new Date(); d2.setDate(d2.getDate()-1);
    date = d2.toISOString().slice(0,10);
  }
  fetch("/api/verify?station="+STATION,{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({date:date, actual:parseFloat(actual), metric:"low"})
  }).then(function(r){ return r.json(); })
  .then(function(data){
    if(data.ok){
      status.style.color="var(--green)";
      status.textContent = "Saved. "+data.calibration_points+" snapshots calibrated.";
      fetch("/api/verification?station="+STATION)
        .then(function(r){ return r.json(); })
        .then(function(verif){
          var v = verif[date];
          if(v && v.calibration && v.calibration.length){
            document.getElementById("verif-results").style.display="block";
            document.getElementById("verif-tbody").innerHTML = v.calibration.map(function(c,i){
              var bg = i%2?"background:#0a1018":"";
              var ec = corrColor(c.error);
              return '<tr style="'+bg+'">'
                +'<td style="color:var(--dim)">'+c.time+'</td>'
                +'<td style="color:var(--blue)">'+c.consensus+'F</td>'
                +'<td style="color:var(--ice)">'+(c.implied!=null?c.implied+"F":"--")+'</td>'
                +'<td style="color:var(--yellow)">'+c.actual+'F</td>'
                +'<td style="color:'+ec+'">'+(c.error>=0?"+":"")+c.error+'F</td>'
                +'<td style="color:'+maeColor(c.abs_error)+'">'+c.abs_error+'F</td>'
                +'</tr>';
            }).join("");
          }
          loadVerification();
        });
    } else {
      status.style.color="var(--red)"; status.textContent="Error: "+(data.error||"unknown");
    }
  }).catch(function(e){ status.style.color="var(--red)"; status.textContent="Error: "+e.message; });
}
document.querySelectorAll("nav button").forEach(function(btn){
  btn.addEventListener("click", function(){
    if(btn.textContent.includes("Verification")) loadVerification();
  });
});
</script>
</body>
</html>
"""

_started = False
_start_lock = threading.Lock()

def load_accuracy(station):
    data = load_json_file(f"{DATA_DIR}/accuracy_{station}.json", {})
    if data:
        get_state(station)["accuracy"] = data

def start_background():
    global _started
    with _start_lock:
        if not _started:
            _started = True
            for station in STATIONS:
                load_accuracy(station)
            t = threading.Thread(target=background_loop, daemon=True, name="bgloop")
            t.start()
            print("Background loop started")

with app.app_context():
    start_background()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
