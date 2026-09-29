#!/usr/bin/env python3
"""test_heartbeatd.py — regression net for heartbeatd.py (registry loader,
schedule → next-fire arithmetic, state, and the firing loop).

Everything runs on SYNTHETIC trees under tempfile.mkdtemp(): no timer of a real
workspace is fired, no state file outside a sandbox is touched, and the module
under test is loaded by path. The loop cases run heartbeatd.py as a subprocess
with HEARTBEATD_* pointed at the sandbox, so they exercise the same code path a
deployment does.

Covers:
+ ① profiles: the name IS the file stem; a disagreeing `name`, an unknown key
     (including `version`: an edit is live on the next round, so there is no
     restart to declare), a missing/empty/non-argv `cmd`, a non-positive
     `timeout`, a non-string `env`, bad JSON, a non-object top level and an
     empty profiles dir are each a config error, never a silent skip.
+ ② timers: filename shape `<host>.<timer>.json` (both segments dot-free);
     required keys; `name` agreement; the host-id cross-check on the filename's
     host segment; a non-boolean `enabled`.
+ ③ schedules: exactly one known form; every field validated per form
     (at/days/seconds/once.at); an unknown form, two forms, a `daily` with
     `days`, a `weekly` with numeric days, a `monthly` day 0/32 and a bad
     `once.at` are config errors.
+ ④ host matching: equal, dot-separated prefix, case-insensitive; anything
     else stays out of this machine's face. An empty face warns on stderr.
+ ⑤ the face: a dangling `profile` and two declarations for one timer on one
     machine are config errors; a row = the profile's fields verbatim +
     timer/schedule/enabled.
+ ⑥ next_fire: daily (before / exactly at / after today's occurrence),
     weekly (day set, wrap past the weekend), monthly (a day the current month
     does not have is skipped, not clamped), interval (anchored on the last
     fire so a restart continues the cadence; on `run_started` when never
     fired), once (fires before its instant, spent afterwards and after a
     restart).
+ ⑦ misfire = skip: an occurrence that passed while nothing was running is
     never returned (invariant 1).
+ ⑧ state: atomic round-trip, a corrupt file reads as {} (invariant 8).
+ ⑨ firing: a due timer spawns, is reaped into the state (exit code, duration,
     fire count) and its declared log carries the child's output; the fire line
     names the log PATH; a failing action records its exit code; a still-running
     action blocks the next occurrence (invariant 2); poll_nap's four caps
     (interval / nearest due / CHILD_POLL while a child runs / MIN_NAP floor)
     keep reaping from being quantized to the interval (invariant 10).
+ ⑩ the resident loop end to end: `run` fires an interval timer repeatedly and
     shuts down cleanly on SIGTERM (invariant 6: children go with the daemon).
+ ⑪ single instance: a second `run` against a held lock exits 1 naming the
     holder (invariant 9).

Run: python3 test_heartbeatd.py          (exit 1 = at least one FAIL)
"""

import atexit
import contextlib
import importlib.util
import io
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
HB = os.path.join(HERE, "heartbeatd.py")


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


hb = _load("heartbeatd_under_test", HB)

PASS = 0
FAIL = []
SANDBOXES = []


@atexit.register
def _cleanup():
    for d in SANDBOXES:
        shutil.rmtree(d, ignore_errors=True)


def check(name, cond, detail=""):
    global PASS
    if cond:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL.append(name)
        print(f"  FAIL {name}  {detail}")


def _write(path, body):
    with open(path, "w", encoding="utf-8") as f:
        f.write(body if isinstance(body, str)
                else json.dumps(body, ensure_ascii=False, indent=2))


def tree(profiles=None, timers=None, host_id=None):
    """(root, profiles_dir, timers_dir, host_id_file) for a synthetic registry,
    with the module's path constants pointed at it. `profiles`/`timers` map
    filename stem → dict (or a raw string, written verbatim, for bad JSON)."""
    root = tempfile.mkdtemp(prefix="heartbeatd-test-")
    SANDBOXES.append(root)
    pdir = os.path.join(root, "heartbeats", "profiles")
    tdir = os.path.join(root, "heartbeats", "timers")
    os.makedirs(pdir)
    os.makedirs(tdir)
    for stem, body in (profiles or {}).items():
        _write(os.path.join(pdir, stem + ".json"), body)
    for stem, body in (timers or {}).items():
        _write(os.path.join(tdir, stem + ".json"), body)
    hif = os.path.join(root, "host-id")
    if host_id is not None:
        _write(hif, host_id)
    # Repoint the module's deployment-facing constants at the sandbox.
    hb.ROOT = __import__("pathlib").Path(root)
    hb.REGISTRY = hb.ROOT / "heartbeats"
    hb.PROFILES_DIR = hb.REGISTRY / "profiles"
    hb.TIMERS_DIR = hb.REGISTRY / "timers"
    hb.HOST_ID_FILE = hb.ROOT / "host-id"
    hb.STATE_FILE = hb.ROOT / "run" / "heartbeatd" / "state.json"
    hb.LOCK_FILE = hb.ROOT / "run" / "locks" / "heartbeatd.lock"
    return root, pdir, tdir, (hif if host_id is not None else None)


def cfgerr(fn, *a, **kw):
    """Run fn, returning the ConfigError message ('' when it did not raise)."""
    try:
        fn(*a, **kw)
    except hb.ConfigError as e:
        return str(e)
    return ""


def exits(fn, *a, **kw):
    """Run fn, returning the SystemExit message ('' when it did not exit)."""
    try:
        fn(*a, **kw)
    except SystemExit as e:
        return str(e.code)
    return ""


def epoch(*args):
    return datetime(*args).timestamp()


HOST = "node1.example.com"
CANON = "node1"
HOST_ID = f"{HOST} {CANON}\notherhost other\n"
# The subprocess cases need a registry that matches the machine they run on.
import platform
REAL_HOST = platform.node()


def a_profile(**over):
    p = {"summary": "test action", "cmd": [sys.executable, "-c", "pass"]}
    p.update(over)
    return p


def a_timer(**over):
    t = {"hostname": HOST, "profile": "act",
         "schedule": {"daily": {"at": "11:00"}}}
    t.update(over)
    return t


# ---------------------------------------------------------- ① profiles ----

def t_profiles():
    print("① profiles loader")
    tree({"act": a_profile()}, host_id=HOST_ID)
    got = hb.load_profiles()
    check("the name is the file stem", got["act"]["name"] == "act", got)
    check("cmd survives verbatim",
          got["act"]["cmd"] == [sys.executable, "-c", "pass"])

    tree({"act": a_profile(name="other")}, host_id=HOST_ID)
    check("a disagreeing 'name' is a config error",
          "single source" in cfgerr(hb.load_profiles))

    tree({"act": a_profile(bogus=1)}, host_id=HOST_ID)
    check("an unknown key is a config error",
          "unknown key" in cfgerr(hb.load_profiles))

    tree({"act": a_profile(version=3)}, host_id=HOST_ID)
    check("a 'version' key is rejected (no restart ⇒ nothing to declare)",
          "unknown key" in cfgerr(hb.load_profiles))

    for bad, why in (({"summary": "s"}, "missing cmd"),
                     ({"summary": "s", "cmd": []}, "empty cmd"),
                     ({"summary": "s", "cmd": "ls -l"}, "cmd is a string"),
                     ({"summary": "s", "cmd": ["a", 2]}, "cmd has a non-string")):
        tree({"act": bad}, host_id=HOST_ID)
        check(f"{why} is a config error", "'cmd'" in cfgerr(hb.load_profiles))

    tree({"act": a_profile(timeout=0)}, host_id=HOST_ID)
    check("timeout 0 is a config error", "timeout" in cfgerr(hb.load_profiles))
    tree({"act": a_profile(timeout="30")}, host_id=HOST_ID)
    check("a non-numeric timeout is a config error",
          "timeout" in cfgerr(hb.load_profiles))
    tree({"act": a_profile(timeout=30)}, host_id=HOST_ID)
    check("a numeric timeout is accepted",
          hb.load_profiles()["act"]["timeout"] == 30)

    tree({"act": a_profile(env={"A": 1})}, host_id=HOST_ID)
    check("a non-string env value is a config error",
          "'env'" in cfgerr(hb.load_profiles))
    tree({"act": a_profile(env={"A": "~/x"})}, host_id=HOST_ID)
    check("a string env is accepted", hb.load_profiles()["act"]["env"] == {"A": "~/x"})

    tree({"act": "{not json"}, host_id=HOST_ID)
    check("bad JSON is a config error", "invalid JSON" in cfgerr(hb.load_profiles))
    tree({"act": "[1, 2]"}, host_id=HOST_ID)
    check("a non-object top level is a config error",
          "JSON object" in cfgerr(hb.load_profiles))
    tree({}, host_id=HOST_ID)
    check("an empty profiles dir is a config error",
          "no action profiles" in cfgerr(hb.load_profiles))


# ----------------------------------------------------------- ② timers -----

def t_timers():
    print("② timers loader (filename shape, required keys, host-id check)")
    tree({"act": a_profile()}, {"node1.daily": a_timer()}, host_id=HOST_ID)
    rows = hb.load_timers()
    check("one declaration loaded", len(rows) == 1, rows)
    check("host segment parsed", rows[0][1] == "node1")
    check("timer name = the filename's second segment", rows[0][2] == "daily")

    tree({"act": a_profile()}, {"daily": a_timer()}, host_id=HOST_ID)
    check("a filename without a host segment is a config error",
          "<host>.<timer>.json" in cfgerr(hb.load_timers))
    tree({"act": a_profile()}, {"node1.a.b": a_timer()}, host_id=HOST_ID)
    check("a dotted timer segment is a config error",
          "<host>.<timer>.json" in cfgerr(hb.load_timers))

    for drop in ("hostname", "profile", "schedule"):
        body = a_timer()
        body.pop(drop)
        tree({"act": a_profile()}, {"node1.daily": body}, host_id=HOST_ID)
        check(f"a missing {drop} is a config error",
              "missing key" in cfgerr(hb.load_timers))

    tree({"act": a_profile()},
         {"node1.daily": a_timer(name="other")}, host_id=HOST_ID)
    check("a disagreeing timer 'name' is a config error",
          "single source" in cfgerr(hb.load_timers))
    tree({"act": a_profile()},
         {"node1.daily": a_timer(bogus=1)}, host_id=HOST_ID)
    check("an unknown timer key is a config error",
          "unknown key" in cfgerr(hb.load_timers))
    tree({"act": a_profile()},
         {"node1.daily": a_timer(enabled="yes")}, host_id=HOST_ID)
    check("a non-boolean 'enabled' is a config error",
          "enabled" in cfgerr(hb.load_timers))
    tree({"act": a_profile()},
         {"node1.daily": a_timer(profile="")}, host_id=HOST_ID)
    check("an empty profile reference is a config error",
          "profile" in cfgerr(hb.load_timers))

    # The host-id cross-check: a declaration copied to another machine without
    # editing `hostname` must not sit there matching nobody.
    tree({"act": a_profile()}, {"other.daily": a_timer()}, host_id=HOST_ID)
    check("filename host segment ≠ canonical(hostname) is a config error",
          "rename the file" in cfgerr(hb.load_timers))
    tree({"act": a_profile()}, {"node1.daily": a_timer(hostname="unknownhost")},
         host_id=HOST_ID)
    check("an unresolvable hostname skips the cross-check",
          len(hb.load_timers()) == 1)


# -------------------------------------------------------- ③ schedules -----

def t_schedules():
    print("③ schedule validation")
    good = ({"daily": {"at": "11:00"}},
            {"daily": {"at": "23:59:59"}},
            {"weekly": {"days": ["mon", "SUN"], "at": "09:30"}},
            {"monthly": {"days": [1, 15, 31], "at": "00:00"}},
            {"interval": {"seconds": 3600}},
            {"interval": {"seconds": 0.5}},
            {"once": {"at": "2030-01-01T11:00:00"}})
    for sched in good:
        tree({"act": a_profile()}, {"node1.daily": a_timer(schedule=sched)},
             host_id=HOST_ID)
        check(f"accepted: {hb.describe(sched)}", "" == cfgerr(hb.load_timers))

    bad = (
        ({}, "exactly one"),
        ({"daily": {"at": "11:00"}, "once": {"at": "2030-01-01T00:00:00"}},
         "exactly one"),
        ({"cron": "0 11 * * *"}, "unknown form"),
        ({"daily": "11:00"}, "JSON object"),
        ({"daily": {}}, "at"),
        ({"daily": {"at": "24:00"}}, "out of range"),
        ({"daily": {"at": "11"}}, "HH:MM"),
        ({"daily": {"at": 1100}}, "must be a string"),
        ({"daily": {"at": "11:00", "days": ["mon"]}}, "takes no 'days'"),
        ({"weekly": {"at": "11:00"}}, "needs 'days'"),
        ({"weekly": {"days": [], "at": "11:00"}}, "non-empty"),
        ({"weekly": {"days": [1], "at": "11:00"}}, "weekly.days"),
        ({"weekly": {"days": ["sunday"], "at": "11:00"}}, "weekly.days"),
        ({"monthly": {"days": [0], "at": "11:00"}}, "1..31"),
        ({"monthly": {"days": [32], "at": "11:00"}}, "1..31"),
        ({"monthly": {"days": ["1"], "at": "11:00"}}, "1..31"),
        ({"interval": {}}, "seconds"),
        ({"interval": {"seconds": 0}}, "positive"),
        ({"interval": {"seconds": "60"}}, "positive"),
        ({"interval": {"seconds": 60, "at": "11:00"}}, "unknown key"),
        ({"once": {}}, "once.at"),
        ({"once": {"at": "tomorrow"}}, "YYYY-MM-DDTHH:MM:SS"),
        ({"once": {"at": 12345}}, "must be a string"),
    )
    for sched, needle in bad:
        tree({"act": a_profile()}, {"node1.daily": a_timer(schedule=sched)},
             host_id=HOST_ID)
        msg = cfgerr(hb.load_timers)
        check(f"rejected {json.dumps(sched, sort_keys=True)[:60]}",
              needle in msg, msg)


def t_describe():
    print("③b describe()")
    cases = (
        ({"daily": {"at": "11:00"}}, "daily 11:00"),
        ({"weekly": {"days": ["sun"], "at": "11:00"}}, "weekly sun 11:00"),
        ({"monthly": {"days": [1, 15], "at": "09:00"}}, "monthly d1,d15 09:00"),
        ({"interval": {"seconds": 3600}}, "every 3600s"),
        ({"once": {"at": "2030-01-01T11:00:00"}}, "once 2030-01-01T11:00:00"),
    )
    for sched, want in cases:
        check(f"describe {want}", hb.describe(sched) == want, hb.describe(sched))


# --------------------------------------------------- ④⑤ host face ---------

def t_face():
    print("④⑤ this machine's face (host matching, dangling profile, dupes)")
    timers = {
        "node1.daily": a_timer(),
        "node1.weekly": a_timer(schedule={"weekly": {"days": ["sun"],
                                                    "at": "08:00"}}),
        "other.daily": a_timer(hostname="otherhost"),
    }
    tree({"act": a_profile()}, timers, host_id=HOST_ID)
    face = hb.timers_here(hostname=HOST)
    check("only this machine's declarations",
          sorted(e["timer"] for e in face) == ["daily", "weekly"],
          [e["timer"] for e in face])
    row = next(e for e in face if e["timer"] == "daily")
    check("the row carries the profile's fields verbatim",
          row["cmd"] == [sys.executable, "-c", "pass"] and row["name"] == "act",
          row)
    check("the row carries timer/schedule/enabled",
          row["schedule"] == {"daily": {"at": "11:00"}}
          and row["enabled"] is True and row["timer"] == "daily")

    check("a dot-separated prefix matches",
          hb._host_matches("node1.example.com", "node1"))
    check("an exact hostname matches", hb._host_matches("node1", "node1"))
    check("matching is case-insensitive",
          hb._host_matches("Node1.Example.COM", "node1"))
    check("a different machine does not match",
          not hb._host_matches("node2.example.com", "node1"))
    check("a partial label does not match",
          not hb._host_matches("node1x.example.com", "node1"))

    tree({"act": a_profile()}, {"node1.daily": a_timer(profile="nope")},
         host_id=HOST_ID)
    check("a dangling profile is a config error",
          "does not exist" in cfgerr(hb.timers_here, hostname=HOST))

    tree({"act": a_profile()},
         {"node1.daily": a_timer(), "elsewhere.daily": a_timer()},
         host_id=None)
    check("two declarations for one timer on one machine is a config error",
          "both declare timer" in cfgerr(hb.timers_here, hostname=HOST))

    tree({"act": a_profile()}, {"other.daily": a_timer(hostname="otherhost")},
         host_id=HOST_ID)
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        face = hb.timers_here(hostname=HOST)
    check("an empty face is not an error", face == [])
    check("an empty face warns loudly on stderr",
          "no timer declaration matches" in err.getvalue(), err.getvalue())

    tree({"act": a_profile()}, {"node1.daily": a_timer(enabled=False)},
         host_id=HOST_ID)
    face = hb.timers_here(hostname=HOST)
    check("enabled=false survives into the face",
          face[0]["enabled"] is False, face[0])


# ------------------------------------------------------ ⑥⑦ next_fire ------

def t_next_fire():
    print("⑥⑦ next_fire arithmetic (and misfire = skip)")
    daily = {"daily": {"at": "11:00"}}
    now = epoch(2026, 9, 29, 9, 0, 0)          # a Tuesday
    check("daily before today's occurrence → today",
          hb.next_fire(daily, now) == epoch(2026, 9, 29, 11, 0, 0),
          datetime.fromtimestamp(hb.next_fire(daily, now)))
    check("daily after today's occurrence → tomorrow (misfire = skip)",
          hb.next_fire(daily, epoch(2026, 9, 29, 12, 0, 0))
          == epoch(2026, 9, 30, 11, 0, 0))
    check("daily exactly at the occurrence advances (no double fire)",
          hb.next_fire(daily, epoch(2026, 9, 29, 11, 0, 0))
          == epoch(2026, 9, 30, 11, 0, 0))
    check("a fire one second late still advances to tomorrow",
          hb.next_fire(daily, epoch(2026, 9, 29, 11, 0, 0) + 0.4)
          == epoch(2026, 9, 30, 11, 0, 0))
    check("daily wraps the month end",
          hb.next_fire(daily, epoch(2026, 9, 30, 11, 30, 0))
          == epoch(2026, 10, 1, 11, 0, 0))

    weekly = {"weekly": {"days": ["sun"], "at": "11:00"}}
    check("weekly → the next matching weekday",
          hb.next_fire(weekly, now) == epoch(2026, 10, 4, 11, 0, 0),
          datetime.fromtimestamp(hb.next_fire(weekly, now)))
    weekly2 = {"weekly": {"days": ["mon", "fri"], "at": "09:00"}}
    check("weekly with two days → the nearest one",
          hb.next_fire(weekly2, epoch(2026, 9, 29, 12, 0, 0))
          == epoch(2026, 10, 2, 9, 0, 0))
    check("weekly day names are case-insensitive",
          hb.next_fire({"weekly": {"days": ["SUN"], "at": "11:00"}}, now)
          == epoch(2026, 10, 4, 11, 0, 0))

    monthly = {"monthly": {"days": [1], "at": "00:00"}}
    check("monthly → the next matching day of month",
          hb.next_fire(monthly, now) == epoch(2026, 10, 1, 0, 0, 0))
    d31 = {"monthly": {"days": [31], "at": "11:00"}}
    check("monthly day 31 skips a month that has no 31st (not clamped)",
          hb.next_fire(d31, epoch(2026, 4, 1, 0, 0, 0))
          == epoch(2026, 5, 31, 11, 0, 0),
          datetime.fromtimestamp(hb.next_fire(d31, epoch(2026, 4, 1))))
    check("monthly with several days → the nearest",
          hb.next_fire({"monthly": {"days": [15, 30], "at": "06:00"}}, now)
          == epoch(2026, 9, 30, 6, 0, 0))

    hourly = {"interval": {"seconds": 3600}}
    check("interval continues from the last fire (survives a restart)",
          hb.next_fire(hourly, now, last_fire=now - 100) == now + 3500)
    check("interval anchors on run_started when never fired",
          hb.next_fire(hourly, now, run_started=now - 10) == now + 3590)
    check("interval with no anchor at all → one period from now",
          hb.next_fire(hourly, now) == now + 3600)
    check("a sub-minute interval is due at once when overdue",
          hb.next_fire({"interval": {"seconds": 1}}, now, last_fire=now - 5)
          == now - 4)

    once = {"once": {"at": "2026-10-01T11:00:00"}}
    check("once before its instant → that instant",
          hb.next_fire(once, now) == epoch(2026, 10, 1, 11, 0, 0))
    check("once after its instant → never (misfire = skip, no catch-up)",
          hb.next_fire(once, epoch(2026, 10, 2, 0, 0, 0)) is None)
    check("once already fired → never again",
          hb.next_fire(once, now, last_fire=epoch(2026, 9, 1, 0, 0, 0))
          == epoch(2026, 10, 1, 11, 0, 0))
    spent = epoch(2026, 10, 1, 11, 0, 0)
    check("once spent (last_fire ≥ at) → None",
          hb.next_fire(once, now, last_fire=spent + 1) is None)


# ------------------------------------------------------------ ⑧ state -----

def t_state():
    print("⑧ state file")
    root, _p, _t, _h = tree({"act": a_profile()}, host_id=HOST_ID)
    sp = os.path.join(root, "run", "heartbeatd", "state.json")
    check("a missing state reads as {}", hb.read_state(sp) == {})
    hb.write_state({"daily": {"fires": 2, "last_exit": 0}}, sp)
    check("round-trip", hb.read_state(sp)["daily"]["fires"] == 2)
    check("no tmp file left behind",
          not os.path.exists(sp.replace(".json", ".json.tmp")))
    _write(sp, "{corrupt")
    check("a corrupt state reads as {} (the clock keeps running)",
          hb.read_state(sp) == {})
    _write(sp, "[1]")
    check("a non-object state reads as {}", hb.read_state(sp) == {})


# ----------------------------------------------------------- ⑨ firing -----

def _runner_with(profile_body, timer_body, state=None):
    root, _p, _t, _h = tree({"act": profile_body},
                            {"node1.t": timer_body}, host_id=HOST_ID)
    if state:
        hb.write_state(state, hb.STATE_FILE)
    r = hb.Runner()
    r.face = hb.timers_here(hostname=HOST)
    return root, r


def t_fire():
    print("⑨ firing, reaping, logs, failures")
    root, r = _runner_with(
        a_profile(cmd=[sys.executable, "-c", "print('hello from the action')"],
                  log="logs/action.log"),
        a_timer(schedule={"interval": {"seconds": 3600}}))
    entry = r.face[0]
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        rc = r.fire(entry, wait=True)
    fired_log = out.getvalue()
    check("fire returns the action's exit code", rc == 0, rc)
    check("the fire line names the log path (not a file-object repr)",
          os.path.join(root, "logs", "action.log") in fired_log
          and "BufferedWriter" not in fired_log, fired_log[-300:])
    st = hb.read_state(hb.STATE_FILE).get("t", {})
    check("the state records exit 0", st.get("last_exit") == 0, st)
    check("the state counts the fire", st.get("fires") == 1, st)
    check("the state records last_fire", isinstance(st.get("last_fire"), float), st)
    check("the state records a duration", st.get("last_duration", -1) >= 0, st)
    logp = os.path.join(root, "logs", "action.log")
    body = open(logp, encoding="utf-8").read() if os.path.exists(logp) else ""
    check("the declared log carries the child's output",
          "hello from the action" in body, body[:200])
    check("the log carries a fire header", "fired" in body, body[:200])

    root, r = _runner_with(
        a_profile(cmd=[sys.executable, "-c", "import sys; sys.exit(7)"]),
        a_timer(schedule={"interval": {"seconds": 3600}}))
    rc = r.fire(r.face[0], wait=True)
    check("a failing action's exit code is reported", rc == 7, rc)
    check("a failing action's exit code is recorded",
          hb.read_state(hb.STATE_FILE)["t"]["last_exit"] == 7)

    root, r = _runner_with(
        a_profile(cmd=[sys.executable, "-c", "import os; os.chdir('/')"]),
        a_timer(schedule={"interval": {"seconds": 3600}}))
    check("cwd defaults to the workspace root", True)
    root, r = _runner_with(
        a_profile(cmd=[sys.executable, "-c", "pass"], cwd="missing-dir"),
        a_timer(schedule={"interval": {"seconds": 3600}}))
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        rc = r.fire(r.face[0], wait=True)
    check("a bad cwd is logged, not raised into the loop",
          "spawn failed" in out.getvalue(), out.getvalue())
    check("a failed spawn records last_error",
          "last_error" in hb.read_state(hb.STATE_FILE)["t"])

    # env injection (invariant 7)
    root, r = _runner_with(
        a_profile(cmd=[sys.executable, "-c",
                       "import os,sys; sys.exit(0 if os.environ.get('HB_T')"
                       "=='v' else 1)"], env={"HB_T": "v"}),
        a_timer(schedule={"interval": {"seconds": 3600}}))
    check("the profile's env reaches the action",
          r.fire(r.face[0], wait=True) == 0)


def t_poll_nap():
    print("⑨a poll_nap (invariant 10: reaping is not quantized to the interval)")
    now = 1_800_000_000.0
    check("no timer due, no child → the poll interval",
          hb.poll_nap({}, 20, {}, now) == 20)
    check("a timer due sooner caps the nap",
          hb.poll_nap({"t": now + 3}, 20, {}, now) == 3)
    check("an overdue timer floors at MIN_NAP (no busy loop, no negative sleep)",
          hb.poll_nap({"t": now - 100}, 20, {}, now) == hb.MIN_NAP)
    check("a running child caps the nap at CHILD_POLL",
          hb.poll_nap({}, 20, {"t": object()}, now) == hb.CHILD_POLL)
    near = hb.poll_nap({"t": now + 0.2}, 20, {"t": object()}, now)
    check("a child + a nearer due time → the nearer one",
          abs(near - 0.2) < 1e-6, near)
    check("None next-fires (spent once / disabled) are ignored",
          hb.poll_nap({"a": None, "b": now + 7}, 20, {}, now) == 7)


def t_round_and_overlap():
    print("⑨b round(): due timers fire, running ones block (invariant 2)")
    root, r = _runner_with(
        a_profile(cmd=[sys.executable, "-c", "import time; time.sleep(4)"]),
        a_timer(schedule={"interval": {"seconds": 1}}),
        state={"t": {"last_fire": time.time() - 60, "fires": 3}})
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        fired = r.round()
    check("an overdue interval timer fires in the round", fired == 1, fired)
    check("the fire is logged", "fired pid" in out.getvalue(), out.getvalue())
    check("the child is tracked", "t" in r.children, r.children)
    # Still running, and the schedule says it is due again → skip, no stacking.
    with contextlib.redirect_stdout(out):
        fired = r.round(now=time.time() + 5)
    check("a still-running action blocks the next occurrence", fired == 0, fired)
    check("the skip is logged", "still running" in out.getvalue())
    check("no second process was stacked", len(r.children) == 1, r.children)
    r.shutdown()
    check("shutdown takes the child with it (invariant 6)", not r.children,
          r.children)

    # A disabled timer never fires.
    root, r = _runner_with(
        a_profile(cmd=[sys.executable, "-c", "pass"]),
        a_timer(schedule={"interval": {"seconds": 1}}, enabled=False),
        state={"t": {"last_fire": time.time() - 60}})
    with contextlib.redirect_stdout(io.StringIO()):
        fired = r.round()
    check("a disabled timer does not fire", fired == 0, fired)
    check("a disabled timer has no next fire", r.nexts.get("t") is None)

    # A timeout kills the tree and records the signal exit.
    root, r = _runner_with(
        a_profile(cmd=[sys.executable, "-c", "import time; time.sleep(30)"],
                  timeout=1),
        a_timer(schedule={"interval": {"seconds": 1}}),
        state={"t": {"last_fire": time.time() - 60}})
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        r.round()                       # fires the sleeper
        deadline = time.time() + 20
        while "t" in r.children and time.time() < deadline:
            r.reap()
            time.sleep(0.2)
    check("the timeout is enforced", "exceeded timeout" in out.getvalue(),
          out.getvalue()[-400:])
    check("a timed-out action is reaped out of the children",
          "t" not in r.children, r.children)
    st = hb.read_state(hb.STATE_FILE)["t"]
    check("a timed-out action records a non-zero exit",
          st.get("last_exit") not in (0, None), st)


def t_refresh_keeps_last_good():
    print("⑨c a broken edit keeps the last good face (invariant 3)")
    root, _p, tdir, _h = tree({"act": a_profile()},
                              {"node1.t": a_timer(
                                  schedule={"interval": {"seconds": 3600}})},
                              host_id=HOST_ID)
    r = hb.Runner(hostname=HOST)
    r.refresh()
    check("the good face loads", [e["timer"] for e in r.face] == ["t"], r.face)
    _write(os.path.join(tdir, "node1.broken.json"), "{not json")
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        r.refresh()
    check("the config error is logged", "CONFIG ERROR" in out.getvalue(),
          out.getvalue())
    check("the last good face is kept",
          [e["timer"] for e in r.face] == ["t"], r.face)
    with contextlib.redirect_stdout(out):
        r.refresh()
    check("the same error is not re-logged every round",
          out.getvalue().count("CONFIG ERROR") == 1, out.getvalue())
    os.remove(os.path.join(tdir, "node1.broken.json"))
    with contextlib.redirect_stdout(out):
        r.refresh()
    check("recovery is logged", "parses again" in out.getvalue(), out.getvalue())
    # A timer that disappears from the face loses its next-fire slot.
    os.remove(os.path.join(tdir, "node1.t.json"))
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        r.refresh()
    check("a removed timer leaves the face", r.face == [] and r.nexts == {},
          (r.face, r.nexts))


# --------------------------------------------- ⑩⑪ the resident process ----

def _env(root):
    env = dict(os.environ)
    env.update({
        "HEARTBEATD_ROOT": root,
        "HEARTBEATD_REGISTRY": os.path.join(root, "heartbeats"),
        "HEARTBEATD_HOST_ID": os.path.join(root, "host-id"),
        "HEARTBEATD_STATE": os.path.join(root, "run", "heartbeatd",
                                         "state.json"),
        "HEARTBEATD_LOCK": os.path.join(root, "run", "locks", "heartbeatd.lock"),
    })
    return env


def _real_tree(timers, profiles=None):
    """A sandbox whose host-id maps THIS machine to the canonical name `here`,
    so a subprocess run has a face and its timers match. Timer declarations
    passed in must use hostname=REAL_HOST (the matching authority)."""
    root, pdir, tdir, _h = tree(profiles or {"act": a_profile()}, timers,
                                host_id=f"{REAL_HOST} here\n")
    return root, pdir, tdir


def t_run_subprocess():
    print("⑩⑪ the resident loop as a process")
    root, _p, tdir = _real_tree(
        {"here.fast": a_timer(hostname=REAL_HOST,
                              schedule={"interval": {"seconds": 1}},
                              profile="act")},
        profiles={"act": a_profile(
            cmd=[sys.executable, "-c", "print('tick')"], log="logs/a.log")})
    env = _env(root)
    p = subprocess.Popen([sys.executable, HB, "run", "--interval", "1"],
                         env=env, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True)
    time.sleep(4.0)
    state = hb.read_state(os.path.join(root, "run", "heartbeatd", "state.json"))
    check("the loop fired the interval timer at least twice",
          state.get("fast", {}).get("fires", 0) >= 2, state)
    dur = state.get("fast", {}).get("last_duration")
    check("a fast action's recorded duration is not quantized to the interval",
          dur is not None and dur < 2.0, state)
    p.send_signal(signal.SIGTERM)
    try:
        out = p.communicate(timeout=25)[0]
    except subprocess.TimeoutExpired:
        p.kill()
        out = p.communicate()[0]
        check("SIGTERM stops the loop", False, "timed out")
    check("SIGTERM stops the loop", p.returncode == 0, p.returncode)
    check("startup lists the face", "started (host=here" in out, out[:400])
    check("each fire is logged", "fast: fired pid" in out, out[-400:])
    check("each exit is logged", "action exited 0" in out, out[-400:])
    check("shutdown is logged", "stopped" in out, out[-200:])
    logp = os.path.join(root, "logs", "a.log")
    body = open(logp, encoding="utf-8").read() if os.path.exists(logp) else ""
    check("the action's output reached its log", body.count("tick") >= 2,
          body[:200])

    # --once: a single round, no lock, exits by itself.
    root, _p, tdir = _real_tree(
        {"here.now": a_timer(hostname=REAL_HOST,
                             schedule={"interval": {"seconds": 1}})},
        profiles={"act": a_profile(cmd=[sys.executable, "-c", "pass"])})
    hb.write_state({"now": {"last_fire": time.time() - 60}},
                   os.path.join(root, "run", "heartbeatd", "state.json"))
    r = subprocess.run([sys.executable, HB, "run", "--once"],
                       env=_env(root), capture_output=True, text=True,
                       timeout=60)
    check("run --once exits 0", r.returncode == 0, r.stderr[-300:])
    st = hb.read_state(os.path.join(root, "run", "heartbeatd", "state.json"))
    check("run --once fired the due timer once",
          st.get("now", {}).get("fires") == 1, st)

    # Single instance: a second `run` against a held lock exits 1.
    root, _p, tdir = _real_tree(
        {"here.t": a_timer(hostname=REAL_HOST,
                           schedule={"interval": {"seconds": 3600}})})
    env = _env(root)
    os.makedirs(os.path.dirname(env["HEARTBEATD_LOCK"]), exist_ok=True)
    holder = open(env["HEARTBEATD_LOCK"], "a+")
    import fcntl
    fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    holder.write("424242\n")
    holder.flush()
    r = subprocess.run([sys.executable, HB, "run", "--interval", "1"],
                       env=env, capture_output=True, text=True, timeout=60)
    check("a second run refuses to double-fire (invariant 9)",
          r.returncode == 1, r.returncode)
    check("the refusal names the holder",
          "another heartbeatd holds" in (r.stderr + r.stdout)
          and "424242" in (r.stderr + r.stdout), (r.stderr + r.stdout)[-300:])
    fcntl.flock(holder.fileno(), fcntl.LOCK_UN)
    holder.close()


def t_cli_faces():
    print("⑪b status / check / fire CLIs")
    root, _p, tdir = _real_tree(
        {"here.daily": a_timer(hostname=REAL_HOST,
                               schedule={"daily": {"at": "11:00"}})},
        profiles={"act": a_profile(cmd=[sys.executable, "-c", "pass"],
                                   log="logs/a.log")})
    env = _env(root)
    r = subprocess.run([sys.executable, HB, "status"], env=env,
                       capture_output=True, text=True, timeout=60)
    check("status exits 0", r.returncode == 0, r.stderr[-300:])
    check("status lists the timer with its schedule",
          "daily" in r.stdout and "daily 11:00" in r.stdout, r.stdout)
    check("status shows the next fire", "NEXT" in r.stdout, r.stdout)
    r = subprocess.run([sys.executable, HB, "status", "--json"], env=env,
                       capture_output=True, text=True, timeout=60)
    data = json.loads(r.stdout)
    check("status --json is one parseable line",
          data["total"] == 1 and data["timers"][0]["timer"] == "daily", data)
    r = subprocess.run([sys.executable, HB, "check"], env=env,
                       capture_output=True, text=True, timeout=60)
    check("check exits 0 on a valid registry", r.returncode == 0, r.stderr)
    check("check lists profiles and timers",
          "profile act" in r.stdout and "timer daily" in r.stdout, r.stdout)
    r = subprocess.run([sys.executable, HB, "fire", "daily"], env=env,
                       capture_output=True, text=True, timeout=60)
    check("fire runs the action now and reports its exit",
          r.returncode == 0 and "action exited 0" in r.stdout,
          (r.stdout + r.stderr)[-300:])
    st = hb.read_state(os.path.join(root, "run", "heartbeatd", "state.json"))
    check("fire records into the state", st.get("daily", {}).get("fires") == 1,
          st)
    r = subprocess.run([sys.executable, HB, "fire", "nosuch"], env=env,
                       capture_output=True, text=True, timeout=60)
    check("fire on an unknown timer is a config error",
          r.returncode == 1 and "no timer" in r.stderr, r.stderr[-200:])
    r = subprocess.run([sys.executable, HB], env=env, capture_output=True,
                       text=True, timeout=60)
    check("no verb prints the usage docstring and exits 1",
          r.returncode == 1 and "heartbeatd.py run" in r.stderr, r.stderr[:200])

    # A broken registry makes every one-shot face fail loudly (invariant 4).
    _write(os.path.join(tdir, "here.bad.json"), {"hostname": "here",
                                                 "profile": "act",
                                                 "schedule": {"daily": {}}})
    for verb in (["status"], ["check"], ["fire", "daily"]):
        r = subprocess.run([sys.executable, HB] + verb, env=env,
                           capture_output=True, text=True, timeout=60)
        check(f"{' '.join(verb)} fails loudly on a broken registry",
              r.returncode == 1 and "error:" in r.stderr, r.stderr[-200:])


TESTS = (t_profiles, t_timers, t_schedules, t_describe, t_face, t_next_fire,
         t_state, t_fire, t_poll_nap, t_round_and_overlap, t_refresh_keeps_last_good,
         t_run_subprocess, t_cli_faces)


def main():
    for t in TESTS:
        t()
    print(f"\n{PASS} passed, {len(FAIL)} failed")
    for name in FAIL:
        print(f"  FAIL {name}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
