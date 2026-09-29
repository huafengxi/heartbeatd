#!/usr/bin/env python3
"""heartbeatd.py — fire the actions declared for THIS machine when their timers
come due.

The registry is two layers of JSON under <workspace>/heartbeats/:
  profiles/<action>.json           what to run: cmd (argv, no shell) + the
                                   optional cwd/timeout/log/env + the
                                   summary/note/notes prose. Shared by every
                                   machine that fires it.
  timers/<host>.<timer>.json       that THIS machine fires it, when, and which
                                   action: `hostname` (the matching authority:
                                   equal to the machine's hostname or a
                                   dot-separated prefix of it), `profile` (the
                                   action to fire), `schedule` (exactly one
                                   declarative form, see below), `enabled`.
                                   One file per (machine, timer) pair; the
                                   `<host>` filename segment is the machine's
                                   canonical name (dot-free, for humans) and is
                                   cross-checked against the host-id map.
A machine's face = its own timer declarations; nothing else is listed, fired or
touched there (no cross-machine rows, no `hosts:`/`exclude_hosts:` lists).

Schedule forms (exactly one key per timer, all times are the machine's LOCAL
time — a deployment that needs another zone sets TZ for the service):
  {"daily":    {"at": "HH:MM"}}
  {"weekly":   {"days": ["mon", ...], "at": "HH:MM"}}
  {"monthly":  {"days": [1..31], "at": "HH:MM"}}   absent dates are skipped
  {"interval": {"seconds": N}}
  {"once":     {"at": "YYYY-MM-DDTHH:MM:SS"}}

Usage:
  heartbeatd.py run [--interval N] [--once]
                                the resident loop: fire every due timer, reap
                                its children, log one line per event. --once =
                                a single round (testing / a manual sweep);
                                --interval caps the poll period (the loop wakes
                                exactly at the nearest due time when that is
                                sooner, so a daily timer is not up to N late).
  heartbeatd.py status          this machine's timer face: schedule, next fire,
                                last fire, last exit code, fire count
  heartbeatd.py status --json   the same data as one JSON line {host, total,
                                timers:[…]} — the input face of an aggregator
  heartbeatd.py fire TIMER      fire one timer now and wait for it (manual
                                re-run / the only catch-up path); exit code =
                                the action's
  heartbeatd.py check           validate the whole registry (every machine's
                                declarations, not just this one) and exit

Pointers — single sources, not restated here:
  registry schema and fields      the registry's own README
                                  (this workspace: heartbeats/README.md)
  how the daemon itself is kept   the deployment's service layer (this
                                  workspace: services/profiles/heartbeatd.json
                                  + serviced/serviced.py)
  start/stop and restart rules    the deployment's operating rules

Invariants (rule bodies):
  1. Misfire policy = SKIP, never catch up: the next occurrence is always
     computed from now, so an occurrence that passed while the daemon was down
     is gone. The only catch-up path is an explicit `fire TIMER`.
  2. A timer never overlaps itself: while its previous action is still running,
     due occurrences are skipped with a log line (a slow action delays the
     cadence instead of stacking processes).
  3. The registry is re-read every round, so an edit is live without a restart;
     a round whose registry fails to validate keeps the LAST GOOD face and logs
     the error once per distinct message — a broken edit degrades observability,
     it never kills the daemon and never silently drops a timer (same rule as
     the service layer: a declaration that fails to parse must not disappear
     from the face it governs).
  4. Any parse/shape failure of a one-shot command (status/fire/check) is a
     config error: exit non-zero with the message, never a silent skip.
  5. Children are spawned as their own session (process-group leader) so a
     timeout can signal the whole tree: TERM, then KILL after the grace period.
     `timeout` absent = unbounded (the action owns its own bound).
  6. Stopping the daemon takes its children with it (TERM → grace → KILL):
     no orphan survives a restart of this process.
  7. The child's environment is this process's environment plus the profile's
     `env`: the daemon is started by the deployment's service layer, which
     already scrubs identity and normalizes PATH, and this code adds no second
     copy of either list. A manual `fire` from a shell therefore passes that
     shell's environment through (the debugging face, by design).
  8. State (last fire / exit code / fire count) lives in one JSON file written
     atomically (tmp + replace); an unreadable or corrupt state file is treated
     as empty and rewritten — losing the counters must not stop the clock.
  9. Single instance: a non-blocking flock on the lock file; a second `run`
     exits 1 with the holder's pid instead of double-firing every timer.
"""

import argparse
import fcntl
import json
import os
import platform
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# --- deployment-facing paths: every one is overridable so a test (or another
# --- workspace) can point the daemon at a synthetic tree.
ROOT = Path(os.environ.get("HEARTBEATD_ROOT")
            or Path(__file__).resolve().parent.parent)
REGISTRY = Path(os.environ.get("HEARTBEATD_REGISTRY") or (ROOT / "heartbeats"))
PROFILES_DIR = REGISTRY / "profiles"
TIMERS_DIR = REGISTRY / "timers"
# Optional canonical-name map (`<hostname> <canonical>` per line): it makes the
# timer filenames human-readable and is cross-checked against them, but it is
# never the matching authority (that is each declaration's `hostname`).
HOST_ID_FILE = Path(os.environ.get("HEARTBEATD_HOST_ID")
                    or (ROOT / "env" / "host-id"))
STATE_FILE = Path(os.environ.get("HEARTBEATD_STATE")
                  or (ROOT / "run" / "heartbeatd" / "state.json"))
LOCK_FILE = Path(os.environ.get("HEARTBEATD_LOCK")
                 or (ROOT / "run" / "locks" / "heartbeatd.lock"))

PROFILE_KEYS = frozenset({
    "name", "summary", "note", "notes", "cmd", "cwd", "timeout", "log", "env"})
TIMER_KEYS = frozenset({"name", "hostname", "profile", "schedule", "enabled",
                        "note"})
SCHEDULE_FORMS = ("daily", "weekly", "monthly", "interval", "once")
WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5,
            "sun": 6}
# Cap on the poll period: the loop also wakes at the nearest due time, so this
# only bounds how late a timer can be when the machine's clock jumps forward.
DEFAULT_INTERVAL = 20
# TERM → KILL grace, used both by the per-action `timeout` and by shutdown.
KILL_GRACE = 10
# How far ahead monthly/weekly candidates are searched (days).
SEARCH_DAYS = 400

TS = "%Y-%m-%d %H:%M:%S"


class ConfigError(Exception):
    """A registry that cannot be trusted. One-shot commands turn it into an
    exit; `run` keeps the last good face and logs it (invariant 3)."""


def log(msg):
    print(f"{time.strftime(TS)} heartbeatd: {msg}", flush=True)


# ---------------------------------------------------------------- registry ---

def _read_json(path):
    """One registry file → dict; unreadable / non-object / bad JSON is a config
    error, never a silent skip."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except OSError as e:
        raise ConfigError(f"cannot read {path}: {e}")
    except ValueError as e:
        raise ConfigError(f"{path}: invalid JSON ({e})")
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: the top level must be a JSON object")
    return data


def _unknown_keys(path, data, allowed):
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ConfigError(f"{path}: unknown key(s) {unknown} (allowed: "
                          f"{sorted(allowed)})")


def host_id_map(host_id_file=None):
    """{hostname(lower) → canonical name} from the optional host-id map:
    `<hostname> <canonical>` per line, `#` comments and blanks skipped. A
    missing or unreadable map yields {} — the canonical name then falls back to
    the hostname itself, and the filename cross-check is skipped."""
    out = {}
    try:
        text = Path(host_id_file or HOST_ID_FILE).read_text(encoding="utf-8")
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            out[parts[0].lower()] = parts[1]
    return out


def this_host(host_id_file=None):
    """(hostname, canonical) for this machine: hostname = platform.node() (the
    value timer declarations are matched against), canonical = its host-id map
    entry when there is one (the value timer filenames are written with)."""
    hn = platform.node()
    return hn, host_id_map(host_id_file).get(hn.lower(), hn)


def _host_matches(hostname, entry):
    """Case-insensitive host match: `entry` matches when it equals the machine
    hostname or is a dot-separated prefix of it."""
    hn, e = str(hostname).lower(), str(entry).lower()
    return hn == e or hn.startswith(e + ".")


def _hms(value, path, key):
    """'HH:MM' (seconds optional) → (h, m, s); anything else is a config
    error naming the file and key."""
    if not isinstance(value, str):
        raise ConfigError(f"{path}: {key} must be a string 'HH:MM', got "
                          f"{value!r}")
    parts = value.split(":")
    if len(parts) not in (2, 3):
        raise ConfigError(f"{path}: {key} must be 'HH:MM' (or 'HH:MM:SS'), "
                          f"got {value!r}")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        raise ConfigError(f"{path}: {key} must be numeric 'HH:MM', got "
                          f"{value!r}")
    h, m = nums[0], nums[1]
    s = nums[2] if len(nums) == 3 else 0
    if not (0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59):
        raise ConfigError(f"{path}: {key} is out of range: {value!r}")
    return h, m, s


def load_profiles(profiles_dir=None):
    """{action name → definition}. The name IS the file stem: a `name` key
    inside the file is redundant and a disagreement is a config error."""
    pdir = Path(profiles_dir or PROFILES_DIR)
    out = {}
    for path in sorted(pdir.glob("*.json")):
        data = _read_json(path)
        name = path.stem
        _unknown_keys(path, data, PROFILE_KEYS)
        if "name" in data and str(data["name"]) != name:
            raise ConfigError(f"{path}: 'name' is {data['name']!r} but the file "
                              f"stem is {name!r} — the filename is the single "
                              f"source")
        cmd = data.get("cmd")
        if not isinstance(cmd, list) or not cmd \
                or not all(isinstance(c, str) and c for c in cmd):
            raise ConfigError(f"{path}: 'cmd' must be a non-empty array of "
                              f"strings (argv, no shell), got {cmd!r}")
        if "timeout" in data and (not isinstance(data["timeout"], (int, float))
                                  or isinstance(data["timeout"], bool)
                                  or data["timeout"] <= 0):
            raise ConfigError(f"{path}: 'timeout' must be a positive number of "
                              f"seconds, got {data['timeout']!r}")
        env = data.get("env")
        if env is not None and (not isinstance(env, dict) or any(
                not isinstance(k, str) or not isinstance(v, str)
                for k, v in env.items())):
            raise ConfigError(f"{path}: 'env' must be an object of "
                              f"string → string, got {env!r}")
        data.pop("name", None)
        data["name"] = name
        out[name] = data
    if not out:
        raise ConfigError(f"no action profiles under {pdir}")
    return out


def load_timers(timers_dir=None, host_id_file=None):
    """[(path, host segment, timer name, declaration)] for every timer
    declaration, sorted by filename. Filename = `<host>.<timer>.json`
    (`<host>` dot-free, `<timer>` dot-free).

    Two config errors are caught here rather than at match time:
      + when the host-id map resolves the declared `hostname`, its canonical
        name must equal the filename's host segment (a declaration copied to a
        new machine without editing `hostname` would otherwise sit there
        matching nobody — an invisible hole, not a loud failure);
      + `schedule` must carry exactly one known form, fully and validly
        specified (a half-typed schedule must not fire at a wrong time)."""
    canon = host_id_map(host_id_file)
    tdir = Path(timers_dir or TIMERS_DIR)
    out = []
    for path in sorted(tdir.glob("*.json")):
        stem = path.stem
        host_seg, sep, name_seg = stem.partition(".")
        if not sep or not host_seg or not name_seg or "." in name_seg:
            raise ConfigError(f"{path}: the filename must be "
                              f"<host>.<timer>.json (both segments dot-free); "
                              f"got {stem!r}")
        data = _read_json(path)
        _unknown_keys(path, data, TIMER_KEYS)
        missing = sorted({"hostname", "profile", "schedule"} - set(data))
        if missing:
            raise ConfigError(f"{path}: missing key(s) {missing}")
        if "name" in data and str(data["name"]) != name_seg:
            raise ConfigError(f"{path}: 'name' is {data['name']!r} but the "
                              f"filename says {name_seg!r} — the filename is "
                              f"the single source")
        if not isinstance(data["profile"], str) or not data["profile"]:
            raise ConfigError(f"{path}: 'profile' must be a non-empty action "
                              f"name, got {data['profile']!r}")
        if not isinstance(data.get("enabled", True), bool):
            raise ConfigError(f"{path}: 'enabled' must be a boolean, got "
                              f"{data['enabled']!r}")
        want = canon.get(str(data["hostname"]).lower())
        if want is not None and want != host_seg:
            raise ConfigError(f"{path}: the filename's host segment is "
                              f"{host_seg!r} but hostname "
                              f"{data['hostname']!r} maps to canonical "
                              f"{want!r} — rename the file or fix hostname")
        validate_schedule(path, data["schedule"])
        data.pop("name", None)
        data["name"] = name_seg
        out.append((path, host_seg, name_seg, data))
    return out


def validate_schedule(path, sched):
    """Exactly one known form, with every field it needs. Raises ConfigError."""
    if not isinstance(sched, dict):
        raise ConfigError(f"{path}: 'schedule' must be a JSON object, got "
                          f"{sched!r}")
    forms = [k for k in sched if k in SCHEDULE_FORMS]
    unknown = sorted(set(sched) - set(SCHEDULE_FORMS))
    if unknown:
        raise ConfigError(f"{path}: 'schedule' has unknown form(s) {unknown} "
                          f"(allowed: {list(SCHEDULE_FORMS)})")
    if len(forms) != 1:
        raise ConfigError(f"{path}: 'schedule' must carry exactly one of "
                          f"{list(SCHEDULE_FORMS)}, got {forms}")
    form = forms[0]
    body = sched[form]
    if not isinstance(body, dict):
        raise ConfigError(f"{path}: schedule.{form} must be a JSON object, "
                          f"got {body!r}")
    if form in ("daily", "weekly", "monthly"):
        _unknown_keys(path, body, {"at", "days"})
        if form == "daily" and "days" in body:
            raise ConfigError(f"{path}: schedule.daily takes no 'days'")
        if form != "daily" and "days" not in body:
            raise ConfigError(f"{path}: schedule.{form} needs 'days'")
        _hms(body.get("at"), path, f"schedule.{form}.at")
        if form == "weekly":
            days = body["days"]
            if not isinstance(days, list) or not days or not all(
                    isinstance(d, str) and d.lower() in WEEKDAYS for d in days):
                raise ConfigError(f"{path}: schedule.weekly.days must be a "
                                  f"non-empty array of {sorted(WEEKDAYS)}, "
                                  f"got {days!r}")
        if form == "monthly":
            days = body["days"]
            if not isinstance(days, list) or not days or not all(
                    isinstance(d, int) and not isinstance(d, bool)
                    and 1 <= d <= 31 for d in days):
                raise ConfigError(f"{path}: schedule.monthly.days must be a "
                                  f"non-empty array of ints 1..31, got "
                                  f"{days!r}")
    elif form == "interval":
        _unknown_keys(path, body, {"seconds"})
        secs = body.get("seconds")
        if not isinstance(secs, (int, float)) or isinstance(secs, bool) \
                or secs <= 0:
            raise ConfigError(f"{path}: schedule.interval.seconds must be a "
                              f"positive number, got {secs!r}")
    elif form == "once":
        _unknown_keys(path, body, {"at"})
        at = body.get("at")
        if not isinstance(at, str):
            raise ConfigError(f"{path}: schedule.once.at must be a string "
                              f"'YYYY-MM-DDTHH:MM:SS', got {at!r}")
        try:
            datetime.fromisoformat(at)
        except ValueError:
            raise ConfigError(f"{path}: schedule.once.at is not "
                              f"'YYYY-MM-DDTHH:MM:SS': {at!r}")


def timers_here(hostname=None, profiles=None, timers=None, host_id_file=None,
                warn=True):
    """This machine's face: [{profile fields…, timer, schedule, enabled}] for
    every timer declaration whose `hostname` matches this machine, sorted by
    timer name. A dangling `profile` and two declarations for the same timer on
    this machine are config errors. Zero matches is not an error (a machine may
    legitimately have no timers) but is warned about loudly — `warn=False` is
    the resident loop's form, which dedupes the warning itself."""
    hn, canonical = this_host(host_id_file)
    hn = hostname or hn
    if profiles is None:
        profiles = load_profiles()
    if timers is None:
        timers = load_timers(host_id_file=host_id_file)
    out, seen = [], {}
    for path, _host_seg, name, d in timers:
        if not _host_matches(hn, d["hostname"]):
            continue
        if name in seen:
            raise ConfigError(f"{path} and {seen[name]} both declare timer "
                              f"{name!r} for this machine ({hn})")
        seen[name] = path
        pname = str(d["profile"])
        if pname not in profiles:
            raise ConfigError(f"{path}: profile {pname!r} does not exist "
                              f"({PROFILES_DIR / (pname + '.json')})")
        entry = dict(profiles[pname])
        entry["timer"] = name
        entry["schedule"] = d["schedule"]
        entry["enabled"] = bool(d.get("enabled", True))
        entry["decl"] = str(path)
        out.append(entry)
    if not out and warn:
        print(f"warning: no timer declaration matches this machine "
              f"(hostname={hn}, canonical={canonical}) — nothing fires here; "
              f"expected files under {TIMERS_DIR}/", file=sys.stderr)
    return out


def load_registry(host_id_file=None, warn=True, hostname=None):
    """(profiles, timers_here) for this machine — the pair `run` refreshes
    every round (invariant 3)."""
    profiles = load_profiles()
    return profiles, timers_here(hostname=hostname, profiles=profiles,
                                 timers=load_timers(host_id_file=host_id_file),
                                 host_id_file=host_id_file, warn=warn)


# ------------------------------------------------------------- schedule面 ---

def describe(sched):
    """One-line human form of a validated schedule (the status table)."""
    form = next(k for k in sched if k in SCHEDULE_FORMS)
    body = sched[form]
    if form == "daily":
        return f"daily {body['at']}"
    if form == "weekly":
        return f"weekly {','.join(body['days'])} {body['at']}"
    if form == "monthly":
        return f"monthly d{',d'.join(str(d) for d in body['days'])} " \
               f"{body['at']}"
    if form == "interval":
        return f"every {body['seconds']:g}s"
    return f"once {body['at']}"


def _at_on(day, hms):
    return datetime(day.year, day.month, day.day, *hms)


def next_fire(sched, now, last_fire=None, run_started=None):
    """The epoch of the next occurrence strictly after the current one, or None
    when this timer has nothing left to fire (a spent `once`).

    `now` is the clock; `last_fire` is the previous fire's epoch (from the
    state) and only matters for `interval` (the cadence continues across a
    restart instead of restarting with the daemon) and `once` (never twice).
    `run_started` anchors an `interval` timer that has never fired: its first
    occurrence is one period after the daemon came up, not immediately."""
    form = next(k for k in sched if k in SCHEDULE_FORMS)
    body = sched[form]
    now_dt = datetime.fromtimestamp(now)

    if form == "interval":
        anchor = last_fire if last_fire else (run_started or now)
        return anchor + float(body["seconds"])

    if form == "once":
        at = datetime.fromisoformat(body["at"]).timestamp()
        if at > now and (last_fire is None or last_fire < at):
            return at
        return None

    hms = _hms(body["at"], "<schedule>", "at")
    if form == "daily":
        cand = _at_on(now_dt.date(), hms)
        if cand.timestamp() <= now:
            cand += timedelta(days=1)
        return cand.timestamp()

    if form == "weekly":
        want = {WEEKDAYS[d.lower()] for d in body["days"]}
        for off in range(SEARCH_DAYS + 1):
            day = (now_dt + timedelta(days=off)).date()
            if day.weekday() not in want:
                continue
            cand = _at_on(day, hms)
            if cand.timestamp() > now:
                return cand.timestamp()
        return None

    # monthly: day-of-month candidates; a month without that day is skipped
    want = set(body["days"])
    for off in range(SEARCH_DAYS + 1):
        day = (now_dt + timedelta(days=off)).date()
        if day.day not in want:
            continue
        cand = _at_on(day, hms)
        if cand.timestamp() > now:
            return cand.timestamp()
    return None


# ------------------------------------------------------------------ state ---

def read_state(path=None):
    """{timer → counters}. A missing/corrupt file reads as {} (invariant 8)."""
    p = Path(path or STATE_FILE)
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def write_state(state, path=None):
    p = Path(path or STATE_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True) + "\n",
                   encoding="utf-8")
    os.replace(tmp, p)


# ------------------------------------------------------------------- fire ---

def _expand(args):
    return [os.path.expanduser(a) for a in args]


def _abspath(spec):
    p = Path(os.path.expanduser(str(spec)))
    return p if p.is_absolute() else (ROOT / p)


def action_env(profile):
    """This process's environment plus the profile's `env` (invariant 7)."""
    env = dict(os.environ)
    for k, v in (profile.get("env") or {}).items():
        env[k] = os.path.expanduser(str(v))
    return env


def spawn(profile, timer):
    """Start one action; returns (Popen, log path or None). The child is a
    session leader so its whole tree can be signalled (invariant 5)."""
    cmd = _expand(profile["cmd"])
    cwd = _abspath(profile.get("cwd") or ".")
    if not cwd.is_dir():
        raise ConfigError(f"{profile['decl'] if 'decl' in profile else timer}: "
                          f"cwd {cwd} is not a directory")
    logf = None
    fh = None
    if profile.get("log"):
        logp = _abspath(profile["log"])
        logp.parent.mkdir(parents=True, exist_ok=True)
        fh = open(logp, "ab")
        fh.write(f"\n--- {timer} fired {time.strftime(TS)}: "
                 f"{' '.join(cmd)}\n".encode())
        fh.flush()
        logf = fh
    out = logf if logf else None
    p = subprocess.Popen(cmd, cwd=str(cwd), env=action_env(profile),
                         stdout=out, stderr=subprocess.STDOUT if out else None,
                         stdin=subprocess.DEVNULL, start_new_session=True)
    if fh:
        fh.close()          # the child holds its own dup'd fd
    return p, (str(logf) if logf else None)


def _signal_group(pid, sig):
    try:
        os.killpg(os.getpgid(pid), sig)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            os.kill(pid, sig)
        except OSError:
            pass


def kill_tree(p, grace=KILL_GRACE):
    """TERM the group, wait `grace`, KILL the group (invariants 5 and 6).
    Takes the Popen object, not a pid: the exit status must be collected by
    subprocess itself — an os.waitpid() here would reap the child first and
    Popen.poll() would then report 0 for a process we just signalled."""
    _signal_group(p.pid, signal.SIGTERM)
    try:
        p.wait(timeout=grace)
        return
    except subprocess.TimeoutExpired:
        pass
    _signal_group(p.pid, signal.SIGKILL)
    try:
        p.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        pass


# --------------------------------------------------------------------- run ---

class Runner:
    """The resident loop's own state: live children, per-timer next fire, and
    the on-disk counters."""

    def __init__(self, state_path=None, hostname=None):
        self.state_path = state_path or STATE_FILE
        self.hostname = hostname            # None = this machine (platform.node())
        self.state = read_state(self.state_path)
        self.children = {}          # timer → (Popen, started, logpath)
        self.nexts = {}             # timer → epoch | None
        self.run_started = time.time()
        self.last_config_error = None
        self.warned_empty = False
        self.face = []

    # -- face refresh (invariant 3) --------------------------------------
    def refresh(self):
        try:
            _profiles, face = load_registry(hostname=self.hostname, warn=False)
        except ConfigError as e:
            msg = str(e)
            if msg != self.last_config_error:
                log(f"CONFIG ERROR (keeping the last good face): {msg}")
                self.last_config_error = msg
            return
        if self.last_config_error:
            log("registry parses again — resuming the declared face")
            self.last_config_error = None
        if face:
            self.warned_empty = False
        elif not self.warned_empty:
            self.warned_empty = True
            log(f"warning: no timer declaration matches this machine "
                f"(hostname={this_host()[0]}) — nothing fires here; expected "
                f"files under {TIMERS_DIR}/")
        self.face = face
        for e in face:
            self.nexts.setdefault(e["timer"], None)
        for gone in set(self.nexts) - {e["timer"] for e in face}:
            self.nexts.pop(gone, None)

    def _sched_next(self, entry, now):
        st = self.state.get(entry["timer"], {})
        return next_fire(entry["schedule"], now, st.get("last_fire"),
                         self.run_started)

    # -- one round -------------------------------------------------------
    def round(self, now=None):
        """Reap finished children, fire every due timer, recompute next fires.
        Returns the number of actions fired."""
        now = time.time() if now is None else now
        self.reap()
        fired = 0
        for entry in self.face:
            timer = entry["timer"]
            if not entry["enabled"]:
                self.nexts[timer] = None
                continue
            nxt = self.nexts.get(timer)
            if nxt is None:
                nxt = self._sched_next(entry, now)
                self.nexts[timer] = nxt
            if nxt is None:
                continue                      # a spent `once`
            if now < nxt:
                continue
            if timer in self.children:
                log(f"{timer}: due but the previous action is still running "
                    f"(pid {self.children[timer][0].pid}) — skipped "
                    f"(invariant 2)")
                # Next slot computed WITHOUT last_fire: an interval timer's
                # cadence would otherwise stay overdue and re-log every round.
                self.nexts[timer] = next_fire(entry["schedule"], time.time(),
                                              None, time.time())
                continue
            self.fire(entry, now=now)
            fired += 1
        return fired

    def fire(self, entry, now=None, wait=False):
        timer = entry["timer"]
        try:
            p, logpath = spawn(entry, timer)
        except (ConfigError, OSError) as e:
            log(f"{timer}: spawn failed: {e}")
            self.state[timer] = dict(self.state.get(timer, {}),
                                     last_fire=time.time(), last_exit=None,
                                     last_error=str(e))
            write_state(self.state, self.state_path)
            self.nexts[timer] = self._sched_next(entry, time.time())
            return None
        self.children[timer] = (p, time.time(), logpath)
        where = f", log {logpath}" if logpath else ""
        log(f"{timer}: fired pid {p.pid} ({describe(entry['schedule'])}"
            f"{where})")
        self.state[timer] = dict(self.state.get(timer, {}),
                                 last_fire=time.time(),
                                 fires=self.state.get(timer, {}).get("fires", 0)
                                 + 1)
        write_state(self.state, self.state_path)
        self.nexts[timer] = self._sched_next(entry, time.time())
        if wait:
            while timer in self.children:
                self.reap()
                time.sleep(0.2)
            return self.state.get(timer, {}).get("last_exit")
        return p

    def reap(self):
        """Collect finished children into the state; enforce `timeout`."""
        for timer, (p, started, logpath) in list(self.children.items()):
            entry = next((e for e in self.face if e["timer"] == timer), None)
            timeout = (entry or {}).get("timeout")
            rc = p.poll()
            if rc is None and timeout and time.time() - started > timeout:
                log(f"{timer}: action exceeded timeout {timeout:g}s — "
                    f"TERM then KILL its group")
                kill_tree(p)
                rc = p.poll()
            if rc is None:
                continue
            dur = time.time() - started
            self.children.pop(timer, None)
            self.state[timer] = dict(self.state.get(timer, {}),
                                     last_exit=rc, last_duration=round(dur, 3),
                                     last_finish=time.time())
            write_state(self.state, self.state_path)
            log(f"{timer}: action exited {rc} after {dur:.1f}s")

    def shutdown(self):
        for timer, (p, _started, _log) in list(self.children.items()):
            log(f"stopping: {timer} (pid {p.pid}) goes with the daemon "
                f"(invariant 6)")
            kill_tree(p)
        self.reap()


def acquire_lock(path=None):
    """Non-blocking single-instance lock; returns the open file (keep a
    reference!) or exits 1 naming the holder (invariant 9)."""
    p = Path(path or LOCK_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    fh = open(p, "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.seek(0)
        holder = fh.read().strip()
        sys.exit(f"error: another heartbeatd holds {p}"
                 + (f" (pid {holder})" if holder else ""))
    fh.seek(0)
    fh.truncate()
    fh.write(f"{os.getpid()}\n")
    fh.flush()
    return fh


def cmd_run(argv):
    ap = argparse.ArgumentParser(prog="heartbeatd.py run")
    ap.add_argument("--interval", type=float, default=DEFAULT_INTERVAL,
                    help="max poll period in seconds (default %(default)s)")
    ap.add_argument("--once", action="store_true",
                    help="a single round, then exit (testing / manual sweep)")
    args = ap.parse_args(argv)

    runner = Runner()
    lock = None if args.once else acquire_lock()
    runner.refresh()
    now = time.time()
    for e in runner.face:
        runner.nexts[e["timer"]] = runner._sched_next(e, now)
    log(f"started (host={this_host()[1]}, interval={args.interval:g}s, "
        f"{len(runner.face)} timer(s) declared here: "
        f"{', '.join(e['timer'] for e in runner.face) or '-'}")
    for e in runner.face:
        nxt = runner.nexts.get(e["timer"])
        log(f"  {e['timer']}: {describe(e['schedule'])}"
            f"{' [disabled]' if not e['enabled'] else ''} → next "
            f"{time.strftime(TS, time.localtime(nxt)) if nxt else 'never'}")

    stop = {"flag": False}

    def _sig(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    try:
        while True:
            runner.refresh()
            runner.round()
            if args.once:
                # A single round must not kill what it just fired: wait the
                # children out (their own `timeout` still applies via reap).
                while runner.children:
                    runner.reap()
                    time.sleep(0.2)
                break
            if stop["flag"]:
                break
            due = [n for n in runner.nexts.values() if n]
            nap = args.interval
            if due:
                nap = min(nap, max(0.05, min(due) - time.time()))
            deadline = time.time() + nap
            while time.time() < deadline and not stop["flag"]:
                time.sleep(min(0.5, max(0.0, deadline - time.time())))
    finally:
        runner.shutdown()
        log("stopped")
        if lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            lock.close()
    return 0


# ------------------------------------------------------------------ status ---

def _row(entry, st, nxt):
    return {
        "timer": entry["timer"],
        "profile": entry["name"],
        "schedule": describe(entry["schedule"]),
        "enabled": entry["enabled"],
        "next": time.strftime(TS, time.localtime(nxt)) if nxt else None,
        "next_epoch": nxt,
        "last_fire": (time.strftime(TS, time.localtime(st["last_fire"]))
                      if st.get("last_fire") else None),
        "last_exit": st.get("last_exit"),
        "fires": st.get("fires", 0),
        "cmd": entry["cmd"],
    }


def _face_rows(state_path=None):
    _profiles, face = load_registry()
    state = read_state(state_path)
    now = time.time()
    rows = []
    for entry in face:
        st = state.get(entry["timer"], {})
        nxt = None if not entry["enabled"] else next_fire(
            entry["schedule"], now, st.get("last_fire"), now)
        rows.append(_row(entry, st, nxt))
    return rows


def cmd_status(argv):
    as_json = "--json" in argv
    try:
        rows = _face_rows()
    except ConfigError as e:
        sys.exit(f"error: {e}")
    hn, canonical = this_host()
    if as_json:
        print(json.dumps({"host": canonical, "hostname": hn,
                          "total": len(rows), "timers": rows},
                         ensure_ascii=False))
        return 0
    print(f"heartbeatd — timers for this machine (hostname={hn}, "
          f"canonical={canonical})")
    if not rows:
        print("  (no timer declaration matches this machine)")
        return 0
    head = ("TIMER", "PROFILE", "SCHEDULE", "NEXT", "LAST FIRE", "EXIT",
            "FIRES")
    widths = [max(len(head[i]), *(len(str(r[k])) for r in rows))
              for i, k in enumerate(("timer", "profile", "schedule", "next",
                                     "last_fire", "last_exit", "fires"))]
    print("  ".join(h.ljust(w) for h, w in zip(head, widths)))
    for r in rows:
        cells = [r["timer"], r["profile"],
                 r["schedule"] + ("" if r["enabled"] else " [disabled]"),
                 r["next"] or "-", r["last_fire"] or "-",
                 "-" if r["last_exit"] is None else str(r["last_exit"]),
                 str(r["fires"])]
        print("  ".join(c.ljust(w) for c, w in zip(cells, widths)))
    print(f"{len(rows)} timer(s), "
          f"{sum(1 for r in rows if not r['enabled'])} disabled")
    return 0


def cmd_fire(argv):
    if not argv:
        sys.exit("error: fire needs a timer name (see `status`)")
    name = argv[0]
    try:
        _profiles, face = load_registry()
    except ConfigError as e:
        sys.exit(f"error: {e}")
    entry = next((e for e in face if e["timer"] == name), None)
    if entry is None:
        sys.exit(f"error: no timer {name!r} declared for this machine "
                 f"(declared: {', '.join(e['timer'] for e in face) or '-'})")
    runner = Runner()
    runner.face = face
    rc = runner.fire(entry, wait=True)
    runner.shutdown()
    print(f"{name}: action exited {rc}")
    return 0 if rc == 0 else 1


def cmd_check(_argv):
    """Validate the whole registry — every machine's declarations, not just
    this one (a timer that matches nobody is still a config error)."""
    try:
        profiles = load_profiles()
        timers = load_timers()
    except ConfigError as e:
        sys.exit(f"error: {e}")
    hn, canonical = this_host()
    print(f"registry {REGISTRY}: {len(profiles)} action profile(s), "
          f"{len(timers)} timer declaration(s)")
    for name, p in sorted(profiles.items()):
        print(f"  profile {name}: {' '.join(p['cmd'])}"
              + (f"  (cwd {p['cwd']})" if p.get("cwd") else "")
              + (f"  (timeout {p['timeout']:g}s)" if p.get("timeout") else "")
              + (f"  (log {p['log']})" if p.get("log") else ""))
    for path, host_seg, name, d in timers:
        live = "this machine" if _host_matches(hn, d["hostname"]) else host_seg
        flag = "" if d.get("enabled", True) else " [disabled]"
        if d["profile"] not in profiles:
            sys.exit(f"error: {path}: profile {d['profile']!r} does not exist")
        print(f"  timer {name} ({live}){flag}: {describe(d['schedule'])} → "
              f"{d['profile']}")
    print(f"ok (host={canonical})")
    return 0


# --------------------------------------------------------------------- cli ---

def _dispatch():
    argv = sys.argv[1:]
    if not argv:
        return __doc__
    verb, rest = argv[0], argv[1:]
    if verb == "run":
        return cmd_run(rest)
    if verb == "status":
        return cmd_status(rest)
    if verb == "fire":
        return cmd_fire(rest)
    if verb == "check":
        return cmd_check(rest)
    return __doc__


def main():
    try:
        rc = _dispatch()
    except SystemExit as e:
        rc = e.code
    if not isinstance(rc, int):
        if rc:
            print(rc, file=sys.stderr)      # sys.exit(<str>) semantics
        rc = 1 if rc else 0
    sys.exit(rc)


if __name__ == "__main__":
    main()
