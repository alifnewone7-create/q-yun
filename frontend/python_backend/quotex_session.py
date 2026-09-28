"""
Thin async wrapper around pyquotex.

Login happens in the terminal (main.py), so this module only exposes the
surface main.py needs:

    session.connect()            -> ("ok" | "2fa_required" | "error", detail)
    session.submit_2fa(code)     -> (True | False, error_detail)
    session.logged_in            -> bool
    session.account_info         -> dict
    session.get_assets()         -> list[dict]
    session.start_candles_stream(asset, period)
    session.stop_candles_stream(asset, period)
    session.get_history(asset, period, count) -> list[candle]
    session.get_latest_candle(asset, period)  -> candle | None

The candle helpers are defensive: different pyquotex forks expose slightly
different method signatures, so we try several shapes and fall back to
reading the internal `client.api.realtime_candles` dict when the public
getters return nothing.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------- #
# websockets 14+ compatibility shim
# --------------------------------------------------------------------------- #
# pyquotex's ws/client.py does `if self._ws and not self._ws.closed:` which
# relied on the legacy `WebSocketClientProtocol.closed` attribute. In
# websockets >= 14 the default client is `ClientConnection` and the `closed`
# attribute was removed (replaced by `state`). If we don't patch it back in,
# every ws send from pyquotex raises:
#     AttributeError: 'ClientConnection' object has no attribute 'closed'
# We also need `additional_headers` which only exists in websockets >= 13,
# so downgrading is not an option — we add the missing `closed` property
# back here and move on.
def _install_websockets_closed_shim() -> None:
    try:
        from websockets.protocol import State  # type: ignore
    except Exception:
        State = None  # type: ignore[assignment]

    def _closed(self):  # type: ignore[no-untyped-def]
        state = getattr(self, "state", None)
        if state is None:
            return False
        if State is not None:
            # True once we pass OPEN (i.e. CLOSING or CLOSED).
            return state is State.CLOSING or state is State.CLOSED
        # Fallback: compare by name.
        return getattr(state, "name", "") in ("CLOSING", "CLOSED")

    for mod_path, cls_name in (
        ("websockets.asyncio.client", "ClientConnection"),
        ("websockets.asyncio.server", "ServerConnection"),
        ("websockets.client", "ClientConnection"),
    ):
        try:
            mod = __import__(mod_path, fromlist=[cls_name])
            cls = getattr(mod, cls_name, None)
            if cls is not None and not hasattr(cls, "closed"):
                cls.closed = property(_closed)  # type: ignore[attr-defined]
        except Exception:
            continue


_install_websockets_closed_shim()


try:
    from pyquotex.stable_api import Quotex  # type: ignore
except Exception:  # pragma: no cover
    try:
        from quotexapi.stable_api import Quotex  # type: ignore
    except Exception as exc:
        raise ImportError(
            "pyquotex is not installed. Install with:\n"
            "    pip install git+https://github.com/cleitonleonel/pyquotex.git"
        ) from exc


log = logging.getLogger("quotex-session")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


async def _maybe_await(value: Any) -> Any:
    if inspect.iscoroutine(value) or inspect.isawaitable(value):
        return await value  # type: ignore[return-value]
    return value


async def _call(fn: Any, *variants: tuple) -> tuple[bool, Any]:
    """
    Try calling `fn` with each argument tuple in `variants`. Return
    `(True, result)` on the first successful call, otherwise `(False, last_exc)`.
    """
    last_exc: Any = None
    for args in variants:
        try:
            res = fn(*args)
            res = await _maybe_await(res)
            return True, res
        except TypeError as exc:
            last_exc = exc
            continue
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            continue
    return False, last_exc


def _is_jsonable(v: Any) -> bool:
    return isinstance(v, (str, int, float, bool, type(None), list, dict))


def _drop_trailing_partial_candles(
    candles: list[dict[str, Any]], period: int
) -> list[dict[str, Any]]:
    """
    Drop the trailing 1-N partial / "dash" candles at the live edge.

    Why this is necessary
    ---------------------
    pyquotex's own REST endpoint (``get_candles`` / ``get_historical_candles``)
    returns the most recent 1-7 buckets in a partially-formed state — often
    just a single tick per bucket, sometimes with a timestamp landing inside
    the currently-forming bucket. Those surface in our aggregator as flat
    candles where ``open == high == low == close`` ("dash" candles) AND/OR
    a candle whose bucket boundary equals the now-bucket start (i.e. the
    bar that's still being built right now).

    pyquotex's own ``process_candles`` helper avoids this by ending with
    ``return candles[:-1]`` — it unconditionally drops the last bucket
    because it's always considered "still forming". The reference repo
    (qxlivechart) pipes every history fetch through that helper, which
    is why it never shows the dash bars our chart was showing.

    This helper reproduces (and slightly extends) that behaviour:

      1. Drop any bucket whose ``time`` is >= the current wall-clock
         bucket boundary — that's the live, still-forming candle. The
         live WS tick stream will rebuild it from scratch.
      2. Drop further trailing buckets while they are flat
         (``high == low``). Real OHLC bars on a 1-minute USD/BRL OTC
         feed are essentially never perfectly flat; if we see one at
         the tail, it's because the broker delivered a single tick
         for that bucket and the rest of the ticks are still in
         flight. We cap this trailing-flat trim at 10 candles so we
         never eat into the genuinely-complete history if the market
         really did go flat for one bar.
    """
    if not candles or period <= 0:
        return candles
    out = sorted(candles, key=lambda c: int(c.get("time", 0)))
    now_bucket = int(time.time() // period) * period

    # (1) Drop any non-bucket-aligned candle. Broker REST responses
    #     occasionally include the live tick as a "candle" with a raw
    #     wall-clock timestamp (e.g. 19:12:35 inside the 19:12:00 bucket).
    #     Those NEVER belong in a closed-history slice.
    out = [c for c in out if int(c.get("time", 0)) % period == 0]

    # (2) Drop the currently-forming bucket(s) — anything timestamped at
    #     or after the now-bucket start is by definition still forming.
    while out and int(out[-1].get("time", 0)) >= now_bucket:
        out.pop()

    # (3) Drop any candle whose bucket lies in the FUTURE relative to
    #     ``now_bucket`` (clock skew from broker side — rare but real).
    out = [c for c in out if int(c.get("time", 0)) <= now_bucket - period]

    # (4) ADAPTIVE TRIM by ``ticks`` count.
    #
    # ROOT CAUSE (verified via pyquotex source-trace):
    #   pyquotex/utils/processor.py::calculate_candles builds each
    #   candle as::
    #
    #     candle = { time, open, close, high, low, ticks: num_ticks }
    #     candles = candles[:-1]   # drops ONLY the current forming bucket
    #
    #   For **high-liquidity** assets (USD/BRL OTC, EUR/USD OTC, etc.)
    #   the broker delivers hundreds of ticks per bucket, so every
    #   candle — including the trailing 2-7 buckets — has a healthy
    #   tick count and real OHLC variation. No problem.
    #
    #   For **low-liquidity** assets (BHD/CNY OTC, AED/CNY OTC, niche
    #   OTC pairs) the broker's REST aggregator lags: the trailing 2-7
    #   minutes arrive with only 1-5 ticks each. Those become "candles"
    #   with ``open ≈ high ≈ low ≈ close`` — but NOT perfectly equal
    #   (e.g. 1.0567 → 1.0568 between two ticks), so the strict
    #   ``h == l and o == c and o == h`` check below MISSES them and
    #   they leak to the chart as the 2-7 "dash" candles the user is
    #   still seeing on those markets.
    #
    # FIX: compare each trailing candle's ``ticks`` count to the
    # median of the older (presumed-stable) candles in the buffer. If
    # the trailing bucket has fewer than 25% of the median tick count
    # AND the bucket boundary is in the most recent 10 bars, it's a
    # broker-side partial — trim it. This adapts automatically to the
    # asset's natural liquidity:
    #
    #   * EUR/USD OTC median ticks ≈ 400/min → threshold ≈ 100.
    #     Genuine trailing candles still have 300-500 ticks → kept.
    #   * BHD/CNY OTC median ticks ≈ 30/min → threshold ≈ 7.
    #     Partial trailing candles have 1-5 ticks → trimmed.
    #
    # Fall back to the strict-flat check when ``ticks`` is missing
    # (e.g. our local aggregator path, or older pyquotex forks).
    if out and len(out) >= 20:
        # Use the 5th-to-15th-from-last candles as the "stable
        # reference" — far enough from the live edge to not be
        # contaminated by partials but recent enough to reflect
        # current liquidity.
        ref_slice = out[-15:-5] if len(out) >= 25 else out[-20:-5]
        ref_ticks = sorted(
            int(c.get("ticks", 0)) for c in ref_slice if c.get("ticks")
        )
        median_ticks = (
            ref_ticks[len(ref_ticks) // 2] if ref_ticks else 0
        )
        if median_ticks > 0:
            threshold = max(2, median_ticks // 4)
            trimmed = 0
            while out and trimmed < 10:
                last = out[-1]
                last_ticks = int(last.get("ticks", 0))
                if last_ticks > 0 and last_ticks < threshold:
                    out.pop()
                    trimmed += 1
                    continue
                break

    # (5) ADAPTIVE RANGE-BASED TRIM for low-liquidity markets.
    #
    # ROOT CAUSE of remaining "dash candles" on niche OTC pairs:
    # The tick-based trim (4) works great when ``ticks`` is available,
    # but on low-liquidity markets the broker often returns trailing
    # candles with 2-5 ticks where ``high != low`` (e.g., 1.0567 vs
    # 1.0568) — a minuscule range that's technically "not flat" but
    # clearly indicates an unfinalised bucket. The strict-flat check
    # (6) below misses these because ``h > l``.
    #
    # FIX: compute the median price range (high - low) of the stable
    # reference candles. If a trailing candle's range is < 5% of the
    # median range AND its range is effectively negligible relative
    # to price, it's a partial — trim it.
    if out and len(out) >= 15:
        ref_slice = out[-15:-5] if len(out) >= 20 else out[:-5]
        ref_ranges = []
        for c in ref_slice:
            try:
                rng = float(c["high"]) - float(c["low"])
                if rng > 0:
                    ref_ranges.append(rng)
            except (KeyError, TypeError, ValueError):
                continue
        if ref_ranges:
            ref_ranges.sort()
            median_range = ref_ranges[len(ref_ranges) // 2]
            # Only apply range-based trim if we have meaningful reference
            # data (median range > 0). The threshold is 5% of median range
            # OR an absolute minimum of 1e-8 (for very low-priced pairs).
            if median_range > 0:
                range_threshold = max(median_range * 0.05, 1e-8)
                trimmed = 0
                while out and trimmed < 10:
                    last = out[-1]
                    try:
                        last_h = float(last["high"])
                        last_l = float(last["low"])
                        last_range = last_h - last_l
                    except (KeyError, TypeError, ValueError):
                        break
                    # If range is tiny compared to median, it's likely partial
                    if last_range < range_threshold:
                        out.pop()
                        trimmed += 1
                        continue
                    break

    # (6) STRICT-FLAT FALLBACK trim (legacy path / older pyquotex
    # without ``ticks``). Real flat candles on a 1-min OTC bar
    # essentially never happen; if we see one at the tail it's a
    # broker-side partial.
    trimmed = 0
    while out and trimmed < 10:
        last = out[-1]
        try:
            o = float(last["open"])
            h = float(last["high"])
            l = float(last["low"])
            c = float(last["close"])
        except (KeyError, TypeError, ValueError):
            break
        if h == l and o == c and o == h:
            out.pop()
            trimmed += 1
            continue
        break
    return out


def _aggregate_ticks_to_candles(
    raw_items: list[Any], period: int
) -> list[dict[str, Any]] | None:
    """
    Re-aggregate pyquotex's REST history output into proper OHLC candles.

    Why this exists
    ---------------
    Pyquotex's own *Utilities and Helpers* documentation (section 8) ships
    a helper called ``process_candles(history, period)`` precisely because
    ``get_candles`` / ``get_historical_candles`` on the Quotex protocol
    DO NOT return ready-made OHLC bars — they return **raw price ticks**
    with shape ``{time, price}`` (with various key aliases per fork:
    ``at``/``timestamp``, ``c``/``value``, etc). The caller is expected
    to bucket those ticks by ``period`` and compute open/high/low/close
    locally.

    Our previous code skipped that aggregation step. Each tick was
    fed through :func:`_normalize_candle`, which — when a tick item has
    no ``high``/``low`` keys — fills ``high = low = open = close = price``.
    Then the per-time dedup in ``accumulated[int(t)] = norm`` kept ONLY
    the last tick per second/bucket. Net result on the chart: every
    closed bar arrived with ``open == high == low == close``, i.e. the
    horizontal-line rendering the user has been chasing on USD/BRL OTC.

    This function reproduces ``process_candles``'s behaviour:
      * groups every tick into a bucket aligned to ``period``
      * tracks the first price as ``open``, last as ``close``, and
        running min/max as ``low`` / ``high``
      * skips inputs that are already proper OHLC (returns ``None`` so
        the caller falls back to the legacy per-item normalize path)

    Returns
    -------
    list[dict] of OHLC candles when aggregation was applied, OR
    ``None`` when the input was detected to already be aggregated
    bars (so the caller knows to use the legacy path verbatim).
    """
    if not raw_items or period <= 0:
        return None

    # ---- Detection: ticks vs already-aggregated candles --------------
    # If *every* item carries non-equal high/low values, the broker
    # already aggregated for us (rare but observed on a couple of
    # pyquotex forks). In that case we MUST NOT re-aggregate — it
    # would discard the high/low information by re-running min/max
    # on closes only.
    looks_already_aggregated = False
    sample = 0
    proper_count = 0
    for raw in raw_items[:32]:  # cheap sniff on first 32 items
        if not isinstance(raw, dict):
            continue
        sample += 1
        hi = raw.get("high", raw.get("max", raw.get("h")))
        lo = raw.get("low", raw.get("min", raw.get("l")))
        if hi is not None and lo is not None:
            try:
                if float(hi) > float(lo):
                    proper_count += 1
            except (TypeError, ValueError):
                pass
    if sample > 0 and proper_count >= max(1, sample // 2):
        looks_already_aggregated = True
    if looks_already_aggregated:
        return None

    # ---- Aggregate ----------------------------------------------------
    buckets: dict[int, dict[str, Any]] = {}
    for raw in raw_items:
        t_raw: Any = None
        p_raw: Any = None
        if isinstance(raw, dict):
            t_raw = (
                raw.get("time")
                or raw.get("from")
                or raw.get("at")
                or raw.get("timestamp")
            )
            p_raw = (
                raw.get("price")
                or raw.get("close")
                or raw.get("c")
                or raw.get("value")
                or raw.get("open")
            )
        elif isinstance(raw, (list, tuple)) and len(raw) >= 2:
            t_raw, p_raw = raw[0], raw[1]
        if t_raw is None or p_raw is None:
            continue
        try:
            t_f = float(t_raw)
            price = float(p_raw)
        except (TypeError, ValueError):
            continue
        # ms → seconds
        if t_f > 10_000_000_000:
            t_f /= 1000.0
        bucket = int(t_f // period) * period
        b = buckets.get(bucket)
        if b is None:
            buckets[bucket] = {
                "time": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1.0,
                "_last_ts": t_f,
                "_first_ts": t_f,
            }
        else:
            if price > b["high"]:
                b["high"] = price
            if price < b["low"]:
                b["low"] = price
            # Track the chronologically-first / chronologically-last
            # tick within the bucket so open + close are stable
            # regardless of input ordering.
            if t_f < b["_first_ts"]:
                b["_first_ts"] = t_f
                b["open"] = price
            if t_f >= b["_last_ts"]:
                b["_last_ts"] = t_f
                b["close"] = price
            b["volume"] += 1.0

    # Strip the internal tracking keys before returning.
    out: list[dict[str, Any]] = []
    for b in buckets.values():
        b.pop("_last_ts", None)
        b.pop("_first_ts", None)
        out.append(b)
    out.sort(key=lambda c: c["time"])
    return out


def _parse_history_load(msg: dict[str, Any], period: int) -> list[dict[str, Any]]:
    """Convert a raw ``history/load`` response into closed OHLC candles (no filtering)."""
    raw = msg.get("data") or msg.get("candles") or []
    out: dict[int, dict[str, Any]] = {}
    ticks: list[Any] = []
    for c in raw if isinstance(raw, list) else []:
        try:
            if isinstance(c, (list, tuple)) and len(c) >= 5:
                t, o, cl, h, lo = c[0], c[1], c[2], c[3], c[4]
            elif isinstance(c, dict) and c.get("open") is not None:
                t, o, cl, h, lo = c["time"], c["open"], c["close"], c["high"], c["low"]
            else:
                ticks.append(c)
                continue
            t = int(float(t))
            o, cl, h, lo = float(o), float(cl), float(h), float(lo)
        except (KeyError, TypeError, ValueError, IndexError):
            continue
        bucket = (t // period) * period
        out[bucket] = {
            "time": bucket,
            "open": o,
            "high": max(h, o, cl),
            "low": min(lo, o, cl),
            "close": cl,
        }

    from_ticks = False
    if not out:
        tick_src = ticks or msg.get("history") or []
        agg = _aggregate_ticks_to_candles(tick_src, period) if tick_src else None
        if agg:
            from_ticks = True
            for c in agg:
                out[int(c["time"])] = {k: c[k] for k in ("time", "open", "high", "low", "close")}

    candles = sorted(out.values(), key=lambda c: c["time"])
    # Tick windows start mid-bucket, so the first tick-built bucket is partial.
    if from_ticks and candles:
        candles = candles[1:]
    # The still-forming bucket is owned by the live tick stream.
    current_bucket = (int(time.time()) // period) * period
    return [c for c in candles if c["time"] < current_bucket]


def _normalize_candle(c: Any, default_time: int | None = None) -> dict[str, Any] | None:
    """Coerce whatever pyquotex returns into {time, open, high, low, close, volume}."""

    def pick(d: Any, *keys: str) -> Any:
        if isinstance(d, dict):
            for k in keys:
                if k in d and d[k] is not None:
                    return d[k]
        return None

    if isinstance(c, dict):
        t = pick(c, "time", "from", "timestamp", "at") or default_time
        o = pick(c, "open", "o")
        h = pick(c, "high", "max", "h")
        lo = pick(c, "low", "min", "l")
        cl = pick(c, "close", "c", "price")
        v = pick(c, "volume", "ticks", "vol") or 0
    elif isinstance(c, (list, tuple)) and len(c) >= 5:
        # [time, open, high, low, close, (volume)]
        t, o, h, lo, cl = c[:5]
        v = c[5] if len(c) > 5 else 0
    else:
        return None

    try:
        t = int(float(t))
    except Exception:
        return None

    # A lot of feeds report time in ms — lightweight-charts expects seconds.
    if t > 10_000_000_000:  # > year 2286 if seconds -> definitely ms
        t //= 1000

    if o is None and cl is not None:
        o = cl
    if cl is None and o is not None:
        cl = o
    if o is None or cl is None:
        return None

    nums = [x for x in (o, cl) if x is not None]
    if h is None:
        h = max(nums) if nums else cl
    if lo is None:
        lo = min(nums) if nums else cl

    try:
        return {
            "time": int(t),
            "open": float(o),
            "high": float(h),
            "low": float(lo),
            "close": float(cl),
            "volume": float(v) if v is not None else 0.0,
        }
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Session
# --------------------------------------------------------------------------- #


class QuotexSession:
    def __init__(self, email: str, password: str, host: str = "qxbroker.com") -> None:
        self.email = email
        self.password = password
        self.host = host
        self.client: Quotex | None = None
        self.logged_in: bool = False
        self.account_info: dict[str, Any] = {}
        self._subscribed: dict[tuple[str, int], bool] = {}
        # Raw tick capture: asset -> (timestamp_seconds_float, price)
        self._last_tick: dict[str, tuple[float, float]] = {}
        # Forming candle cache: (asset, period) -> dict
        self._forming_cache: dict[tuple[str, int], dict[str, Any]] = {}
        # Per-(asset,period) rolling bucket state for deterministic forming
        # candle building. Key -> {bucket, open, high, low, close, last_ts}.
        self._bucket_state: dict[tuple[str, int], dict[str, Any]] = {}
        # Per-(asset,period) FIFO of buckets that just closed and still need
        # to be broadcast with their final accumulated OHLC. Without this
        # queue, the wall-clock bucket advance below would simply overwrite
        # ``_bucket_state[key]`` and the closed bucket's full high/low would
        # never be delivered to the chart — leaving closed candles rendered
        # with whatever OHLC they had at the *previous* polling cycle (i.e.
        # missing the last 100–250 ms of wick information). The polling loop
        # in main.py drains this queue every tick and emits each entry to
        # subscribers BEFORE emitting the new forming candle, so every bar's
        # final wick lands on the chart exactly once at bucket boundary.
        self._closed_buckets_pending: dict[
            tuple[str, int], list[dict[str, Any]]
        ] = {}
        # Per-(asset,period) set of bucket times for which a broker-
        # authoritative REST refetch is currently in flight. Used to dedup
        # so a single bucket close never schedules two concurrent refetch
        # tasks (which would race to enqueue the same authoritative
        # candle and waste broker quota).
        self._authoritative_in_flight: dict[tuple[str, int], set[int]] = {}
        # Flag so we only install the ws hook once.
        self._hook_installed: bool = False
        # Wall-clock timestamp of the last real price tick we observed. The
        # watchdog in main.py uses this to detect a silent stream and trigger
        # a full re-login — same mechanism as the reference project.
        self.last_tick_time: float = time.time()
        # Per-asset (NOT per-asset-period) asyncio locks that serialize every
        # call into pyquotex's get_candles() for a given symbol. Pyquotex
        # stores the response on ``self.api.candles.candles_data[asset]`` and
        # uses a single ``candles_ready_{asset}`` event — both keyed by
        # asset only — so two concurrent fetches for the SAME asset (even
        # at different periods, or one from get_history() and one from
        # fetch_forming_candle_rest()) race and wipe each other's state.
        # That race is the root cause of the VPS-only "199 candle ashe na"
        # bug: high latency lets the second caller's
        # ``candles_data = None`` reset fire before the first caller's WS
        # response arrives, so both attempts return empty. Local PCs got
        # lucky on the latency.
        self._asset_history_locks: dict[str, asyncio.Lock] = {}

    # --------------------------------------------------------- credential store

    @staticmethod
    def _creds_path() -> Path:
        return Path.home() / ".pyquotex" / "credentials.json"

    def _save_credentials(self) -> None:
        """Save email/password to ~/.pyquotex/credentials.json so the next run
        can auto-login without prompting (reference-project behaviour)."""
        if not self.email or not self.password:
            return
        path = self._creds_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"email": self.email, "password": self.password, "host": self.host},
                f,
            )
        try:
            # Best-effort: tighten file permissions on POSIX (0600).
            path.chmod(0o600)
        except Exception:
            pass

    @classmethod
    def load_saved_credentials(cls) -> tuple[str | None, str | None, str | None]:
        """Return (email, password, host) from the saved credentials file,
        or (None, None, None) if nothing is stored."""
        path = cls._creds_path()
        if not path.exists():
            return None, None, None
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return (
                data.get("email"),
                data.get("password"),
                data.get("host") or "qxbroker.com",
            )
        except Exception:
            return None, None, None

    @classmethod
    def clear_saved_credentials(cls) -> None:
        path = cls._creds_path()
        if path.exists():
            try:
                path.unlink()
            except Exception:
                pass

    # ----------------------------------------------- pyquotex session files

    def _pyquotex_session_file_candidates(self) -> list[Path]:
        """Every location pyquotex is known to persist its SSID / session
        token across the various community forks. Called when the server
        rejects our stored SSID so the next connect() starts clean.
        """
        paths: list[Path] = []

        # 1. Project working directory (most common — the reference project
        #    writes session.json straight into cwd).
        cwd = Path.cwd()
        paths.append(cwd / "session.json")
        paths.append(cwd / "settings" / "session.json")
        paths.append(cwd / "settings" / "session_data.json")

        # 2. Right next to this source file (python_backend/ when the script
        #    is launched from a parent directory).
        here = Path(__file__).resolve().parent
        paths.append(here / "session.json")
        paths.append(here / "settings" / "session.json")
        paths.append(here / "settings" / "session_data.json")

        # 3. Inside the installed pyquotex package (some forks write there).
        try:
            import pyquotex  # type: ignore

            pkg_dir = Path(pyquotex.__file__).resolve().parent
            paths.append(pkg_dir / "session.json")
            paths.append(pkg_dir / "settings" / "session.json")
            paths.append(pkg_dir / "settings" / "session_data.json")
        except Exception:
            pass

        # 4. User home cache dir (less common but safe to include).
        home = Path.home()
        paths.append(home / ".pyquotex" / "session.json")
        paths.append(home / ".pyquotex" / "session_data.json")

        # De-dupe while preserving order.
        seen: set[str] = set()
        unique: list[Path] = []
        for p in paths:
            sp = str(p)
            if sp in seen:
                continue
            seen.add(sp)
            unique.append(p)
        return unique

    def _clear_pyquotex_session_files(self) -> int:
        """Delete every saved pyquotex session token we can find, and also
        reset any SSID/cookie state held in memory on the current client
        object. Returns the count of files actually removed.
        """
        removed = 0
        for p in self._pyquotex_session_file_candidates():
            if not p.exists():
                continue
            try:
                p.unlink()
                log.info("cleared expired pyquotex session file: %s", p)
                removed += 1
            except Exception as exc:  # noqa: BLE001
                log.debug("could not delete %s: %s", p, exc)

        # Also flush any in-memory session state the client may be holding
        # so the very next connect() cannot short-circuit to the bad SSID.
        if self.client is not None:
            targets = [self.client, getattr(self.client, "api", None)]
            for target in targets:
                if target is None:
                    continue
                for attr in ("ssid", "session_data", "cookies", "_session", "token"):
                    if hasattr(target, attr):
                        try:
                            setattr(target, attr, None)
                        except Exception:
                            pass
        return removed

    # ---------------------------------------------------------- full reconnect

    async def reconnect(self) -> bool:
        """Tear down the current pyquotex client and rebuild it from scratch.
        Used by the watchdog when the price stream goes silent for too long —
        matches the `full_reconnect()` helper in the working reference project.
        """
        log.warning("full reconnect requested for %s", self.email)
        old = self.client
        self.client = None
        self.logged_in = False
        self._subscribed.clear()
        self._last_tick.clear()
        self._bucket_state.clear()
        self._forming_cache.clear()
        self._hook_installed = False

        if old is not None:
            close_fn = getattr(old, "close", None)
            if callable(close_fn):
                try:
                    await asyncio.wait_for(_maybe_await(close_fn()), timeout=3)
                except Exception:
                    pass
            api = getattr(old, "api", None)
            if api is not None:
                api_close = getattr(api, "close", None)
                if callable(api_close):
                    try:
                        await asyncio.wait_for(_maybe_await(api_close()), timeout=3)
                    except Exception:
                        pass

        await asyncio.sleep(1.5)
        status, _ = await self.connect()
        if status == "ok":
            self.last_tick_time = time.time()
            log.info("reconnect: login succeeded")
            return True
        log.error("reconnect: login failed (%s)", status)
        return False

    # ------------------------------------------------------------------ login

    async def connect(self) -> tuple[str, Any]:
        return await self._connect_inner(allow_session_reset=True)

    async def _connect_inner(self, allow_session_reset: bool) -> tuple[str, Any]:
        """
        Inner connect loop.

        When the Quotex server rejects a saved SSID token (common after VPN
        switches, 24-72 h of inactivity, or a "log out all devices" action),
        pyquotex returns with a reason containing "token rejected" /
        "rejected". Instead of bubbling that up as a fatal error and killing
        the script, we automatically:

          1. Wipe every pyquotex session file we can find on disk.
          2. Flush in-memory SSID / cookie state on the client.
          3. Rebuild the Quotex client instance from scratch.
          4. Retry connect() exactly ONCE more.

        The second attempt will come back as either "ok" (if the server was
        willing to mint a fresh session for our saved credentials) or
        "2fa_required" (the normal case — the server emails a fresh PIN,
        main.py prompts the user, then re-enters this method).

        `allow_session_reset` guards against an infinite recursion if the
        second attempt also gets rejected.
        """
        try:
            if self.client is None:
                kwargs: dict[str, Any] = {
                    "email": self.email,
                    "password": self.password,
                    "lang": "en",
                }
                try:
                    self.client = Quotex(host=self.host, **kwargs)  # type: ignore[arg-type]
                except TypeError:
                    self.client = Quotex(**kwargs)

            check, reason = await self.client.connect()  # type: ignore[misc]
            if check:
                self.logged_in = True
                await self._populate_account_info()
                # CRITICAL for VPS: warm up pyquotex's internal state.
                # See _post_login_warmup() docstring — without this the
                # very first get_candles() call returns empty on VPS
                # links, which is the "199 candle fetch hocche na" bug.
                await self._post_login_warmup()
                # Persist credentials for auto-login on next run (same pattern
                # the working reference project uses).
                try:
                    self._save_credentials()
                except Exception as exc:  # noqa: BLE001
                    log.debug("save credentials failed: %s", exc)
                return "ok", self.account_info

            reason_str = str(reason or "").lower()

            # --- Expired / server-rejected session auto-heal ----------------
            # Detect the distinctive "Token rejected" / "websocket rejected"
            # responses and wipe the stale session file set, then retry once.
            rejection_markers = (
                "token rejected",
                "rejected",
                "websocket rejected",
                "websocket connection rejected",
                "websocket failed to connect",
            )
            is_rejection = any(m in reason_str for m in rejection_markers)
            # "token" alone (without "2fa"/"code") also means a saved SSID
            # was sent but the server didn't accept it.
            looks_like_saved_token_issue = (
                "token" in reason_str
                and "2fa" not in reason_str
                and "code" not in reason_str
            )

            if allow_session_reset and (is_rejection or looks_like_saved_token_issue):
                removed = self._clear_pyquotex_session_files()
                log.warning(
                    "server rejected saved session (%s) — cleared %d stale "
                    "session file(s) and retrying with a fresh login",
                    reason_str.strip() or "no reason",
                    removed,
                )
                print(
                    "\n[!] Saved Quotex session expired or was rejected by the server."
                )
                if removed:
                    print(f"[*] Cleared {removed} stale session file(s).")
                print("[*] Retrying with a fresh login (you may be asked for a new PIN)...\n")

                # Rebuild the client — some forks keep the bad SSID bound
                # to the instance even after we null out attributes.
                old = self.client
                self.client = None
                self.logged_in = False
                if old is not None:
                    close_fn = getattr(old, "close", None)
                    if callable(close_fn):
                        try:
                            await asyncio.wait_for(
                                _maybe_await(close_fn()), timeout=3
                            )
                        except Exception:
                            pass

                await asyncio.sleep(1.0)
                return await self._connect_inner(allow_session_reset=False)

            # Real 2FA challenge (server emailed us a code).
            if "2fa" in reason_str or "code" in reason_str or "pin" in reason_str:
                return "2fa_required", reason

            # Last-ditch: an unexplained "token" response on the retry path —
            # surface as 2FA so the user can paste whatever code arrives.
            if "token" in reason_str:
                return "2fa_required", reason

            return "error", reason

        except Exception as exc:  # noqa: BLE001
            log.exception("connect() failed")
            return "error", str(exc)

    async def submit_2fa(self, code: str) -> tuple[bool, Any]:
        if self.client is None:
            return False, "not connected"
        for attr in ("set_2fa_code", "set_code", "send_2fa_code", "submit_2fa"):
            fn = getattr(self.client, attr, None)
            if callable(fn):
                try:
                    res = fn(code)
                    await _maybe_await(res)
                    return True, None
                except Exception as exc:  # noqa: BLE001
                    return False, str(exc)
        return False, (
            "This pyquotex fork expects the 2FA code to be typed in the "
            "terminal that runs this script."
        )

    async def _populate_account_info(self) -> None:
        if not self.client:
            return
        info: dict[str, Any] = {"email": self.email, "host": self.host}

        for attr in ("get_profile", "profile", "get_account_info"):
            fn = getattr(self.client, attr, None)
            if not callable(fn):
                continue
            try:
                data = await _maybe_await(fn())
                if isinstance(data, dict):
                    info.update({k: v for k, v in data.items() if _is_jsonable(v)})
                    break
                if data is not None:
                    # flatten a handful of common profile attrs
                    for a in ("nickname", "country", "currency", "avatar"):
                        v = getattr(data, a, None)
                        if _is_jsonable(v):
                            info[a] = v
                    break
            except Exception as exc:  # noqa: BLE001
                log.debug("profile fetch via %s failed: %s", attr, exc)

        for attr in ("get_balance", "balance"):
            fn = getattr(self.client, attr, None)
            if not callable(fn):
                continue
            try:
                bal = await _maybe_await(fn())
                if _is_jsonable(bal):
                    info["balance"] = bal
                    break
            except Exception:
                pass

        self.account_info = info

    async def _post_login_warmup(self) -> None:
        """
        Mirror the reference repo's post-login sequence so pyquotex's
        internal state is fully populated BEFORE the first get_candles()
        call. This is the single most important fix for the
        "VPS e 199 candle fetch hocche na" bug.

        Reference engine.py does this right after a successful connect::

            await CLIENT.change_account("PRACTICE")
            await CLIENT.get_all_assets()

        On a local PC the user moves the mouse / clicks an asset within
        milliseconds, which triggers similar warmup calls implicitly.
        On a headless VPS nothing else touches pyquotex between connect()
        and the first get_candles() — so the internal ``api.asset_data``
        / ``api.candles_data`` dicts are still ``None`` / empty, and the
        get_candles() call returns ``[]`` with no error.

        Steps:
          1. ``change_account(QUOTEX_ACCOUNT)`` — defaults to PRACTICE
             (matches the reference repo). Set ``QUOTEX_ACCOUNT=REAL``
             in .env to use the live account instead. Fork-tolerant:
             also tries ``change_balance`` which some forks use.
          2. ``get_all_assets()`` — populates pyquotex's internal asset
             registry. Without this, ``get_candles`` for OTC pairs
             returns empty on the first call from a cold VPS session.

        Both calls are best-effort: failures are logged but never abort
        the login flow, because some pyquotex forks rename or remove
        these methods.
        """
        import os as _os

        client = self.client
        if client is None:
            return

        # --- Step 1: change account context ---------------------------------
        desired_account = (_os.getenv("QUOTEX_ACCOUNT") or "PRACTICE").strip().upper()
        if desired_account not in ("PRACTICE", "REAL"):
            desired_account = "PRACTICE"

        for attr in ("change_account", "change_balance"):
            fn = getattr(client, attr, None)
            if not callable(fn):
                continue
            ok, res = await _call(fn, (desired_account,))
            if ok:
                log.info("warmup: %s(%s) ok", attr, desired_account)
                break
            log.debug("warmup: %s(%s) failed: %s", attr, desired_account, res)
        else:
            log.debug("warmup: no change_account/change_balance method on client")

        # Give the server a moment to switch contexts before the next call.
        await asyncio.sleep(0.3)

        # --- Step 2: populate pyquotex's internal asset registry ------------
        # This is THE critical call for the VPS 199-candle bug. The
        # reference repo's working flow proves that without it,
        # get_candles() returns empty on the first cold call.
        for attr in ("get_all_assets", "get_all_asset", "fetch_all_assets"):
            fn = getattr(client, attr, None)
            if not callable(fn):
                continue
            ok, res = await _call(fn, ())
            if ok:
                try:
                    count = len(res) if hasattr(res, "__len__") else 0
                except Exception:
                    count = 0
                log.info("warmup: %s() -> %d assets", attr, count)
                break
            log.debug("warmup: %s() failed: %s", attr, res)
        else:
            log.debug("warmup: no get_all_assets method on client")

        # Brief settle window. pyquotex's WS handler needs ~0.5 s to
        # finish parsing the asset payload into its internal dicts.
        await asyncio.sleep(0.5)

        # --- Step 3: neutralise pyquotex's v2-cache merge -------------------
        #
        # ROOT-CAUSE FIX for "running candle er ager 2-7 ta vul/dash candle"
        # on less-active markets. Deep-trace of pyquotex master
        # (github.com/cleitonleonel/pyquotex) shows that
        # ``client.get_candles(...)`` ultimately routes through:
        #
        #   pyquotex/_api/history.py::prepare_candles
        #     candles_data = calculate_candles(history, period)        # [:-1]
        #     candles_v2_data = process_candles_v2(
        #         self.api.candle_v2_data, asset, candles_data
        #     )
        #     return merge_candles(candles_v2_data)
        #
        # ``self.api.candle_v2_data`` is a SHARED dict continuously
        # populated by ``candle/v2/data`` WS frames. On a long-running
        # multiplexed session like ours, by the time ``prepare_candles``
        # runs, this dict contains forming-bucket and recently-closed-
        # bucket partials (some with only 2-3 ticks) that
        # ``process_candles_v2`` PREPENDS to the trimmed historical
        # list. ``merge_candles`` dedupes by ``time`` but DOES NOT drop
        # partials — net effect: 2-7 still-forming-on-broker-side
        # candles leak into the result, showing as ``open ≈ high ≈ low
        # ≈ close`` dash bars right before the live candle.
        #
        # Clearing ``candle_v2_data[asset]`` immediately before each
        # ``get_candles`` call is racey: the WS stream re-populates the
        # cache during the multi-second wait for the history response,
        # which is exactly why some markets are still affected.
        #
        # Permanent fix: replace ``client.prepare_candles`` with a
        # version that returns ``calculate_candles(history, period)``
        # directly — i.e. skip ``process_candles_v2`` and
        # ``merge_candles`` entirely. The trimmed historical list is
        # all we ever wanted, and our existing
        # ``_drop_trailing_partial_candles`` keeps acting as a
        # belt-and-suspenders safety net. The live tick stream is
        # untouched because it reads from a completely different
        # cache (``api.realtime_candles``).
        try:
            from pyquotex.utils.processor import (  # type: ignore
                calculate_candles as _pq_calculate_candles,
            )

            def _prepare_candles_no_v2(
                _asset: str, period: int, history: Any = None
            ) -> list[dict[str, Any]]:
                # MUST be a plain sync function — pyquotex's get_candles()
                # calls ``self.prepare_candles(asset, period, history)``
                # WITHOUT await.  If this were async, the caller would
                # receive a bare coroutine object instead of a list,
                # effectively making the patch a no-op and letting the
                # original (v2-cache-merging) code path run — which is
                # the root cause of the dash-candle leak.
                #
                # Pyquotex's own ``calculate_candles`` ends with
                # ``return candles[:-1]`` — i.e. the still-forming bucket
                # is already dropped for us. No need for the v2 cache merge.
                return _pq_calculate_candles(history, period)

            # Bind the override as a bound method on this client instance
            # only — never patch the pyquotex module globally, because
            # other consumers of the same Quotex class (if any) might
            # rely on the v2 merge for their own use case.
            client.prepare_candles = _prepare_candles_no_v2  # type: ignore[assignment]
            log.info(
                "warmup: client.prepare_candles patched to skip "
                "process_candles_v2 / merge_candles (eliminates "
                "trailing partial-bucket leak from candle_v2_data)"
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "warmup: failed to patch prepare_candles (%s) — "
                "_drop_trailing_partial_candles will still scrub "
                "trailing partials, but with reduced reliability "
                "on low-activity markets",
                exc,
            )

    # ------------------------------------------------------------------ assets

    def _require(self) -> Quotex:
        if not self.client or not self.logged_in:
            raise RuntimeError("Quotex session is not connected.")
        return self.client

    async def get_assets(self) -> list[dict[str, Any]]:
        client = self._require()
        out: list[dict[str, Any]] = []

        try:
            names: list[Any] = []
            fn = getattr(client, "get_all_asset_name", None)
            if callable(fn):
                res = await _maybe_await(fn())
                names = list(res or [])

            payments: dict[str, Any] = {}
            pay_fn = getattr(client, "get_payment", None)
            if callable(pay_fn):
                try:
                    res = await _maybe_await(pay_fn())
                    if isinstance(res, dict):
                        payments = res
                except Exception as exc:  # noqa: BLE001
                    log.debug("get_payment failed: %s", exc)

            seen: set[str] = set()
            for entry in names:
                if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                    symbol, human = str(entry[0]), str(entry[1])
                else:
                    symbol = human = str(entry)
                if symbol in seen:
                    continue
                seen.add(symbol)

                pay = payments.get(symbol) if isinstance(payments, dict) else None
                payout = None
                is_open: bool | None = None
                market_type = "forex"
                if isinstance(pay, dict):
                    is_open = pay.get("open")
                    profit = pay.get("profit")
                    if isinstance(profit, dict):
                        for tf in ("1M", "5M", "30S", "5S", "15S"):
                            if profit.get(tf) is not None:
                                payout = profit[tf]
                                break
                        if payout is None:
                            for v in profit.values():
                                if isinstance(v, (int, float)):
                                    payout = v
                                    break
                    elif "payment" in pay:
                        payout = pay.get("payment")
                    mt = pay.get("type") or pay.get("category")
                    if isinstance(mt, str):
                        market_type = mt.lower()

                low = symbol.lower()
                if "_otc" in low:
                    market_type = "otc"
                elif low.startswith(("btc", "eth", "xrp", "ltc", "doge")):
                    market_type = "crypto"
                elif symbol.upper().startswith(
                    ("AAPL", "TSLA", "AMZN", "META", "GOOG", "MSFT", "NFLX")
                ):
                    market_type = "stocks"

                out.append(
                    {
                        "symbol": symbol,
                        "name": human,
                        "payout": payout,
                        "is_open": is_open,
                        "type": market_type,
                    }
                )
        except Exception as exc:  # noqa: BLE001
            log.exception("get_assets failed: %s", exc)

        if not out:
            log.warning(
                "get_assets returned no markets — falling back to a short preset list"
            )
            for sym in (
                "EURUSD",
                "EURUSD_otc",
                "GBPUSD",
                "USDJPY",
                "AUDCAD_otc",
                "BTCUSD",
            ):
                out.append(
                    {
                        "symbol": sym,
                        "name": sym,
                        "payout": None,
                        "is_open": None,
                        "type": "otc" if "_otc" in sym else "forex",
                    }
                )

        return out

    # ------------------------------------------------------------- resolve_asset

    async def resolve_asset(
        self, asset: str, force_open: bool = True
    ) -> tuple[str, bool]:
        """
        Look up an asset and tell the caller whether it's currently
        tradeable, with automatic OTC fallback per upstream
        ``Quotex.get_available_asset``::

            async def get_available_asset(self, asset_name, force_open=False):
                _, asset_open = await self.check_asset_open(asset_name)
                if force_open and (not asset_open or not asset_open[2]):
                    # toggle "_otc" suffix and retry
                    ...
                return asset_name, asset_open

        Returns a ``(resolved_asset, is_open)`` tuple. ``resolved_asset``
        may differ from the input — e.g. when ``EURUSD`` is closed and
        ``force_open=True`` flipped it to ``EURUSD_otc``. ``is_open`` is
        ``True`` only when the broker currently accepts ticks for the
        resolved asset; subscribing to a closed asset returns no ticks
        and silently wedges the chart in the "waiting for running
        candle" overlay forever, which is exactly the UX bug this
        helper exists to prevent.

        On forks that don't expose ``get_available_asset`` we degrade
        gracefully: return ``(asset, True)`` and let the caller proceed.
        Better than failing closed when the helper isn't available.
        """
        client = self._require()
        fn = getattr(client, "get_available_asset", None)
        if not callable(fn):
            return asset, True
        try:
            res = await _maybe_await(fn(asset, force_open))
        except Exception as exc:  # noqa: BLE001
            log.debug("get_available_asset(%s) failed: %s", asset, exc)
            return asset, True

        # Upstream returns ``(name, (id, name2, open_status))``. Some forks
        # return ``(name, [id, name2, open_status, ...])`` or just the
        # tuple inner. Be defensive.
        resolved_name = asset
        is_open = True
        if isinstance(res, (list, tuple)) and len(res) >= 2:
            n, info = res[0], res[1]
            if isinstance(n, str) and n:
                resolved_name = n
            if isinstance(info, (list, tuple)) and len(info) >= 3:
                is_open = bool(info[2])
            elif isinstance(info, dict):
                is_open = bool(info.get("open", info.get("is_open", True)))
        return resolved_name, is_open

    # ------------------------------------------------------------------ close

    async def close(self) -> None:
        """
        Cleanly tear down the underlying pyquotex client. Per docs this
        releases the WS connection, flushes session cookies, and lets
        the next process start with a clean slate. Skipping this on
        shutdown leaves orphaned WS connections on the broker side and
        occasionally causes the next session to be rejected with a
        ``Token rejected`` response.
        """
        client = self.client
        if client is None:
            return
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            try:
                await asyncio.wait_for(_maybe_await(close_fn()), timeout=3)
            except Exception as exc:  # noqa: BLE001
                log.debug("session close failed: %s", exc)
        api = getattr(client, "api", None)
        if api is not None:
            api_close = getattr(api, "close", None)
            if callable(api_close):
                try:
                    await asyncio.wait_for(_maybe_await(api_close()), timeout=3)
                except Exception:
                    pass
        self.logged_in = False

    # ------------------------------------------------------------------ candles

    # --------------------------------------------------------- tick intercept

    def _install_tick_hook(self) -> None:
        """
        Monkey-patch pyquotex's websocket message handler so we can capture
        EVERY raw tick. Different forks expose the ws object in different
        places, so try them all. Works once per session.
        """
        if self._hook_installed:
            return
        client = self.client
        if client is None:
            return

        # Locate the websocket object. Expanded candidates list to cover more
        # pyquotex forks and internal structures.
        candidates: list[Any] = []
        api = getattr(client, "api", None)
        if api is not None:
            # Try direct WS attributes on api
            for attr in ("ws", "websocket", "ws_client", "client", "ws_conn", "socket", "_ws", "_websocket", "wss"):
                obj = getattr(api, attr, None)
                if obj is not None:
                    candidates.append((f"api.{attr}", obj))
            # Some forks nest the WS inside a sub-object
            for sub_attr in ("connection", "conn", "transport", "channel", "stream"):
                sub = getattr(api, sub_attr, None)
                if sub is not None:
                    for ws_attr in ("ws", "websocket", "_ws", "socket"):
                        obj = getattr(sub, ws_attr, None)
                        if obj is not None:
                            candidates.append((f"api.{sub_attr}.{ws_attr}", obj))
        # Try direct on client
        for attr in ("ws", "websocket", "ws_client", "_ws", "_websocket", "wss"):
            obj = getattr(client, attr, None)
            if obj is not None:
                candidates.append((attr, obj))
        # Some forks have a separate connection manager
        for sub_attr in ("connection", "conn", "api_client"):
            sub = getattr(client, sub_attr, None)
            if sub is not None:
                for ws_attr in ("ws", "websocket", "_ws"):
                    obj = getattr(sub, ws_attr, None)
                    if obj is not None:
                        candidates.append((f"{sub_attr}.{ws_attr}", obj))

        for label, ws in candidates:
            if self._try_wrap(ws, label):
                self._hook_installed = True
                log.info("tick hook installed on %s", label)
                return

        log.warning(
            "could not locate a websocket object to install tick hook; "
            "live pulses will depend on REST polling only"
        )

    def _try_wrap(self, ws: Any, label: str) -> bool:
        """Wrap `ws.on_message` (or similar) with our tick extractor."""
        for attr in ("on_message", "_on_message", "message_received"):
            original = getattr(ws, attr, None)
            if not callable(original):
                continue
            session = self

            def wrapper(*args, _orig=original, **kwargs):  # noqa: ANN001, ANN003
                # Original signature can be on_message(message) or
                # on_message(ws, message). The message is typically the last
                # positional arg that is str/bytes.
                try:
                    message: Any = None
                    if args:
                        for a in reversed(args):
                            if isinstance(a, (str, bytes, bytearray)):
                                message = a
                                break
                    if message is not None:
                        session._extract_tick_from_message(message)
                except Exception:  # noqa: BLE001
                    pass
                return _orig(*args, **kwargs)

            try:
                setattr(ws, attr, wrapper)
                log.info("wrapped %s.%s", label, attr)
                return True
            except Exception as exc:  # noqa: BLE001
                log.debug("failed to wrap %s.%s: %s", label, attr, exc)
        return False

    def _extract_tick_from_message(self, message: Any) -> None:
        """
        Parse a raw ws message and pull out (asset, timestamp, price) tuples.
        Quotex sends socket.io frames like:

            42["instruments/list",[...]]
            42["price",{"asset":"EURUSD_otc","time":1745..,"price":1.08}]
            451-["s_pending",{...}]

        plus many other shapes across forks. We look for any JSON blob that
        has an "asset" + "price" pair and optionally a "time".
        """
        import json
        import re

        text: str
        if isinstance(message, (bytes, bytearray)):
            try:
                text = message.decode("utf-8", errors="ignore")
            except Exception:
                return
        elif isinstance(message, str):
            text = message
        else:
            return

        # Strip socket.io frame prefix like 42, 42/nsp, 42["event",...] etc.
        # Find the first [ or { and try JSON parsing of the rest.
        idx = -1
        for i, ch in enumerate(text):
            if ch in "[{":
                idx = i
                break
        if idx < 0:
            return
        payload_text = text[idx:]
        try:
            payload = json.loads(payload_text)
        except Exception:
            # Some forks wrap in [event, data] arrays but with extras — try
            # to snip trailing junk.
            m = re.search(r"(\{.*\}|\[.*\])", payload_text)
            if not m:
                return
            try:
                payload = json.loads(m.group(1))
            except Exception:
                return

        self._walk_payload_for_ticks(payload)

    def _walk_payload_for_ticks(self, obj: Any, depth: int = 0) -> None:
        if depth > 6:
            return
        if isinstance(obj, dict):
            asset = (
                obj.get("asset")
                or obj.get("symbol")
                or obj.get("active")
                or obj.get("activeId")
            )
            price = obj.get("price") or obj.get("value") or obj.get("close")
            t = obj.get("time") or obj.get("timestamp") or obj.get("ts")
            if isinstance(asset, str) and isinstance(price, (int, float)):
                # ----- LATE-FRAME GUARD ---------------------------------
                # The raw-tick hook is global on the websocket and
                # cannot be unhooked per-asset. After we send the broker
                # ``unsubscribe_realtime_candle`` + ``unfollow_candle``
                # there is still a short window during which straggler
                # frames for that pair land here. Without this guard
                # those frames repopulate ``self._last_tick[asset]`` and
                # the higher layers think the pair is still streaming —
                # that's the exact "unsubscribe hoye gelo kintu backend
                # tick nite thake" leak the user reported.
                #
                # Drop any tick whose asset has no active ``_subscribed``
                # entry for this session. We deliberately check
                # ``_subscribed`` (the in-flight registry) rather than a
                # boolean flag because period switches briefly leave the
                # asset with zero entries between the old period's
                # unsubscribe and the new period's subscribe; that one-
                # frame gap is fine — we'd rather drop a tick or two
                # than keep a leak going for hours.
                if not any(a == asset for (a, _p) in self._subscribed):
                    return
                ts: float
                try:
                    ts = float(t) if t is not None else time.time()
                except Exception:
                    ts = time.time()
                if ts > 10_000_000_000:
                    ts /= 1000.0
                self._last_tick[asset] = (ts, float(price))
            for v in obj.values():
                self._walk_payload_for_ticks(v, depth + 1)
        elif isinstance(obj, list):
            for v in obj:
                self._walk_payload_for_ticks(v, depth + 1)

    # ---------------------------------------------------------- candle streams

    async def start_candles_stream(self, asset: str, period: int) -> None:
        """
        Subscribe to candle + tick streams for ``(asset, period)`` using ONLY
        the documented pyquotex API surface.

        Background: per the upstream
        ``pyquotex/stable_api.py::Quotex.start_candles_stream`` source, this
        single call ALREADY internally fires the canonical 3-message
        subscribe sequence:

            await self.api.subscribe_realtime_candle(asset, period)
            await self.api.chart_notification(asset)
            await self.api.follow_candle(asset)

        Earlier revisions of this wrapper *also* called every one of those
        methods directly, AND additionally awaited ``start_realtime_price``
        (which itself re-runs ``start_candles_stream`` and then blocks for
        up to 30 s waiting for the first tick). Net effect was 2-3x the
        broker-side subscribe traffic per pair plus a worst-case 30 s
        stall per ``_subscribe`` call ��� i.e. the exact reason bulk
        auto-subscribe of ~50 pairs silently failed.

        The shape now matches the upstream contract:
          1. Install the raw tick hook (still needed — gives us
             per-tick precision the public API doesn't expose).
          2. Call ``start_candles_stream(asset, period)`` exactly once.
          3. Mark the pair as subscribed and return immediately.

        Tick polling continues to be driven by ``get_realtime_price`` from
        :meth:`get_latest_candle`, which is the documented public path
        and does not require any extra subscribe calls — the WS server
        starts pushing ticks the moment ``follow_candle`` runs inside
        ``start_candles_stream``.
        """
        client = self._require()
        key = (asset, period)
        if self._subscribed.get(key):
            return

        # Install the raw tick hook once — lets us capture ticks directly from
        # the websocket stream regardless of how pyquotex stores them.
        # First attempt before subscribe.
        self._install_tick_hook()
        hook_installed_before = self._hook_installed

        # Documented call. Some community forks renamed it; we still
        # accept the two known aliases. The argument shape is fixed at
        # ``(asset, period)`` per upstream; we keep a (asset, period, 120)
        # / (asset, 120, period) fallback strictly for the older fork
        # that took an extra "size" param.
        candle_variants = [(asset, period), (asset, period, 120), (asset, 120, period)]
        started = False
        last_err: Any = None
        for attr in (
            "start_candles_stream",
            "start_candles_one_stream",
            "start_candle_stream",
        ):
            fn = getattr(client, attr, None)
            if not callable(fn):
                continue
            ok, res = await _call(fn, *candle_variants)
            if ok:
                log.info("candle stream via %s(%s, %s)", attr, asset, period)
                started = True
                break
            last_err = res

        if not started:
            log.warning(
                "no candle stream method worked for %s/%s (last err: %s)",
                asset,
                period,
                last_err,
            )

        # Retry tick hook if it failed before subscribe. The subscribe call
        # often establishes the WS connection, so the hook may succeed now.
        if not hook_installed_before and not self._hook_installed:
            await asyncio.sleep(0.5)  # Give WS time to fully connect
            self._install_tick_hook()
            if not self._hook_installed:
                # One more retry after a longer wait
                await asyncio.sleep(1.0)
                self._install_tick_hook()

        self._subscribed[key] = True
        # Don't block here waiting for the first tick — the fast/rest pollers
        # in main.py will retry until data flows, and any extra sleep delays
        # the initial chart render by that much. We schedule the debug dump
        # asynchronously instead so it still runs after ~1.5s.
        asyncio.create_task(self._deferred_state_dump(asset, period))

    async def _deferred_state_dump(self, asset: str, period: int) -> None:
        try:
            await asyncio.sleep(1.5)
            self._log_internal_state(asset, period)
        except Exception:
            pass

    def _log_internal_state(self, asset: str, period: int) -> None:
        client = self.client
        if client is None:
            return
        api = getattr(client, "api", None)
        for name in ("realtime_candles", "realtime_price", "candles"):
            root = getattr(api, name, None) if api is not None else None
            if root is None:
                root = getattr(client, name, None)
            if root is None:
                log.info("[state] client.api.%s = <missing>", name)
                continue
            if isinstance(root, dict):
                assets_keys = list(root.keys())[:5]
                sample_for_asset = root.get(asset)
                if isinstance(sample_for_asset, dict):
                    inner_keys = list(sample_for_asset.keys())[:5]
                    log.info(
                        "[state] %s: assets=%s | %s keys=%s",
                        name,
                        assets_keys,
                        asset,
                        inner_keys,
                    )
                else:
                    log.info(
                        "[state] %s: assets=%s | %s=%r",
                        name,
                        assets_keys,
                        asset,
                        type(sample_for_asset).__name__,
                    )
            else:
                log.info("[state] %s type=%s", name, type(root).__name__)

    async def stop_candles_stream(self, asset: str, period: int) -> None:
        """
        Tear down both the candle and tick subscriptions for ``asset``.

        Per upstream ``pyquotex/stable_api.py::Quotex.stop_candles_stream``:

            async def stop_candles_stream(self, asset):
                await self.api.unsubscribe_realtime_candle(asset)
                await self.api.unfollow_candle(asset)

        Note that ``unsubscribe_realtime_candle`` takes ONLY ``asset`` —
        no period — and that ``unfollow_candle`` MUST also run or the
        broker keeps streaming ticks for that pair (a slow leak that
        eventually trips the per-account rate limit on long-running
        backends).

        We try the high-level ``stop_candles_stream`` first; on forks
        that don't expose it (or expose only a partial alias), we fall
        through to calling **both** low-level methods directly, on
        ``client`` and ``client.api`` since different forks bind them
        in different places.

        IMPORTANT — local-state purge:
        ------------------------------
        The broker-side unsubscribe is only half the job. The previous
        implementation stopped here, which left a slow leak the user
        reported as: *"jodi kono user kono pair subscribe kore and
        tarpore shei pair change kore ba onno page a jay tarpore
        unsubscribe hoy but backend shei pair er realtime tick data
        nite thake and realtime tick er list eu rakhe"*. Even though
        the broker had been told to stop, three things kept the data
        flowing in memory:

          1. ``self._last_tick[asset]`` — the raw-tick hook is global
             on the websocket and any straggling frames the broker
             emits before it actually stops would re-populate it.
          2. ``self._bucket_state[(asset, period)]`` and
             ``self._forming_cache[(asset, period)]`` — our internal
             OHLC builder kept the last forming bucket forever.
          3. ``client.api.realtime_candles[asset]`` and
             ``client.api.realtime_price[asset]`` — pyquotex's own
             dicts keep growing on every late frame.

        After we send the broker-side unsubscribe we therefore purge
        all four locations for ``(asset, period)``. ``_last_tick``
        and the pyquotex dicts are keyed only by ``asset`` so we only
        wipe them when no other ``(asset, *)`` subscription remains
        on this session — otherwise we'd starve a sibling timeframe
        that the same user is still watching.
        """
        client = self.client
        if client is None:
            return
        self._subscribed.pop((asset, period), None)

        broker_unsub_ok = False

        # Preferred path: one call that internally does both teardowns.
        for attr in ("stop_candles_stream", "stop_candle_stream", "stop_candles_one_stream"):
            fn = getattr(client, attr, None)
            if not callable(fn):
                continue
            # Upstream signature is (asset); a couple of forks accept
            # (asset, period). Try both.
            ok, _ = await _call(fn, (asset,), (asset, period))
            if ok:
                broker_unsub_ok = True
                break

        if not broker_unsub_ok:
            # Fallback path: call BOTH low-level methods explicitly so we
            # never leak a ``follow_candle`` subscription. Each method may
            # live on either ``client`` or ``client.api`` depending on the
            # fork — try both binding sites.
            api = getattr(client, "api", None)
            for method_name in ("unsubscribe_realtime_candle", "unfollow_candle"):
                for owner in (client, api):
                    if owner is None:
                        continue
                    fn = getattr(owner, method_name, None)
                    if not callable(fn):
                        continue
                    # Both methods take only ``asset`` per upstream; older
                    # forks of unsubscribe accepted ``(asset, period)``.
                    ok, _ = await _call(fn, (asset,), (asset, period))
                    if ok:
                        break  # don't double-fire on client + api

        # ----- LOCAL-STATE PURGE -------------------------------------------
        # Always run, even if the broker-side unsubscribe failed: the
        # whole point is to stop *us* from accumulating stale data for a
        # pair nobody is watching anymore.

        key = (asset, period)
        # Per-(asset, period) state — safe to drop unconditionally:
        # nothing else on this session keys by both.
        self._bucket_state.pop(key, None)
        self._forming_cache.pop(key, None)

        # Per-asset state — only drop when this asset has no other
        # active timeframe in flight. Otherwise we'd reset the live
        # tick price for a sibling 5s/15s/60s subscriber.
        asset_still_used = any(a == asset for (a, _p) in self._subscribed)
        if not asset_still_used:
            self._last_tick.pop(asset, None)

            # pyquotex's own caches. They are public attributes on
            # ``client.api`` (and sometimes ``client``); each fork has
            # a slightly different mix, so we sweep every name we know
            # of and ignore missing ones. ``realtime_candles`` and
            # ``realtime_price`` are the two that grow without bound on
            # every tick — leaving them set is what made the user say
            # "realtime tick er list eu rakhe".
            api = getattr(client, "api", None)
            for owner in (client, api):
                if owner is None:
                    continue
                for cache_name in (
                    "realtime_candles",
                    "realtime_price",
                    "realtime_price_data",
                ):
                    cache = getattr(owner, cache_name, None)
                    if isinstance(cache, dict):
                        cache.pop(asset, None)

            # Drop our per-asset history TTL cache and asset lock too.
            # If the user comes back to this pair later we want a fresh
            # fetch, not a cached snapshot from before the unsubscribe.
            history_cache = getattr(self, "_history_cache", None)
            if isinstance(history_cache, dict):
                for ck in [k for k in history_cache if k[0] == asset]:
                    history_cache.pop(ck, None)
            asset_locks = getattr(self, "_asset_history_locks", None)
            if isinstance(asset_locks, dict):
                asset_locks.pop(asset, None)


    async def history_load(
        self, asset: str, period: int, end_time: float, offset: int, timeout: float = 10.0
    ) -> list[dict[str, Any]]:
        """Send a raw ``history/load`` request and return the broker's candles as-is."""
        client = self.client
        api = getattr(client, "api", None) if client is not None else None
        if api is None:
            log.warning("history/load skipped for %s/%s: not connected", asset, period)
            return []
        store = getattr(api, "history_load_data", None)
        if not isinstance(store, dict):
            # Older pyquotex/api.py without the index store — create it here.
            store = {}
            api.history_load_data = store

        index = int(time.time() * 100)
        last = getattr(self, "_history_load_last_index", 0)
        if index <= last:
            index = last + 1
        self._history_load_last_index = index

        payload = {
            "asset": asset,
            "index": index,
            "time": int(end_time),
            "offset": int(offset),
            "period": int(period),
        }
        try:
            await _maybe_await(
                api.send_websocket_request(f'42["history/load",{json.dumps(payload)}]')
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("history/load send failed for %s/%s: %s", asset, period, exc)
            return []

        deadline = time.time() + timeout
        msg = None
        while time.time() < deadline:
            msg = store.pop(index, None)
            if msg is None:
                # Older ws/client.py only fills the single historical_candles slot.
                hc = getattr(api, "historical_candles", None)
                if isinstance(hc, dict) and hc.get("index") == index:
                    msg = hc
            if msg is not None:
                break
            await asyncio.sleep(0.1)
        if not isinstance(msg, dict):
            log.warning("history/load timed out for %s/%s (index=%d)", asset, period, index)
            return []
        candles = _parse_history_load(msg, int(period))
        if not candles:
            log.warning(
                "history/load empty for %s/%s (keys=%s)", asset, period, list(msg.keys())[:8]
            )
        return candles

    async def get_history(
        self, asset: str, period: int, count: int = 120
    ) -> list[dict[str, Any]]:
        """
        Fetch historical candles for ``asset`` at ``period`` seconds.

        Why this is simpler than it used to be
        --------------------------------------
        The previous implementation tried 7 different ``get_candles`` argument
        shapes in a 3-pass retry loop. On a low-latency local PC that worked,
        but on a high-latency VPS link to Quotex (Singapore → Brazil) every
        call races inside pyquotex on the SAME asset-level event:

            # excerpt from pyquotex/stable_api.py::get_candles
            self.api.candles.candles_data = None
            await self.api.event_registry.clear_event(f'candles_ready_{asset}')
            await self.start_candles_stream(asset, period)
            await self.api.get_candles(asset, index, end_from_time, offset, period)
            await self.api.event_registry.wait_event(f'candles_ready_{asset}', ...)

        When attempts 1 and 2 fire in quick succession, attempt 2's
        ``clear_event`` wipes the very event attempt 1 is waiting on, and
        attempt 2's ``candles_data = None`` discards attempt 1's response
        the instant it arrives. Both attempts then time out empty — which
        is exactly the VPS bug ("199 candle fetch hocche na").

        The fix has two parts:

        1. Match the reference repo (qxlivechart) call exactly — kwargs,
           float timestamp, single shape. That repo proves this one call
           is sufficient against the current pyquotex.
        2. Serialize calls per-asset with an asyncio lock so we NEVER
           issue overlapping ``get_candles`` for the same symbol, no
           matter how many subscribers there are.

        The 5 s TTL cache below also collapses bursts (asset switch, tab
        reload) into a single upstream request.
        """
        client = self._require()
        cache_key = (asset, int(period), int(count))
        now = time.time()

        # --- TTL cache ------------------------------------------------------
        cache = getattr(self, "_history_cache", None)
        if cache is None:
            cache = {}
            self._history_cache = cache  # type: ignore[attr-defined]
        entry = cache.get(cache_key)
        if entry is not None and now - entry[0] < 5.0:
            return entry[1]

        # --- Per-asset serialization lock -----------------------------------
        # Use the SAME asyncio.Lock as fetch_forming_candle_rest() and
        # fetch_closed_bucket_authoritative() — see ``__init__`` for the
        # full explanation of why this is keyed only by asset and not by
        # (asset, period).
        lock = self._asset_history_locks.get(asset)
        if lock is None:
            lock = asyncio.Lock()
            self._asset_history_locks[asset] = lock

        async with lock:
            # Re-check the cache after acquiring the lock — a sibling call
            # may have populated it while we were waiting.
            entry = cache.get(cache_key)
            if entry is not None and time.time() - entry[0] < 5.0:
                return entry[1]

            target = int(count)

            # Direct history/load: return every candle Quotex sends, untouched.
            direct = await self.history_load(
                asset, int(period), time.time(), int(period) * target
            )
            if direct:
                log.info(
                    "history (direct history/load) -> %d candles for %s/%s",
                    len(direct), asset, period,
                )
                cache[cache_key] = (time.time(), direct)
                return direct

            # -------------------------------------------------------------
            # =================================================================
            # PRIMARY PATH (NEW): ``get_candles`` with offset = N*period.
            #
            # ROOT-CAUSE NOTE — the 1-7 "dash candles right before the live
            # candle" bug:
            # ----------------------------------------------------------------
            # Our previous primary path used ``get_historical_candles`` (the
            # 5-worker deep-history fetcher in
            # ``pyquotex/_api/history.py``). That method parses raw
            # ``history/load`` WS frames via ``_parse_historical_candles`` and
            # MERGES them as-is — meaning every bucket the broker happens to
            # still be finalising at the live edge is surfaced verbatim, often
            # as ``open == high == low == close`` "dash" candles. Even after
            # we added ``_drop_trailing_partial_candles`` as a defensive
            # trim, the broker sometimes returns those partials at
            # non-bucket-aligned timestamps or with valid-looking high != low
            # but still mid-formation, so the trim alone could not catch every
            # case. Users on the chart-to-signal page kept seeing 2-7 bad
            # candles wedged between the (clean) 190+ historical candles and
            # the (live, correct) forming candle.
            #
            # The reference repository (qxlivechart) never has this issue
            # because it uses ``CLIENT.get_candles(end_from_time=now,
            # offset=199*period, period=period)`` instead. The key detail —
            # buried in ``pyquotex/_api/history.py:get_candles`` —
            # is that ``get_candles`` runs the broker's stream through
            # ``prepare_candles`` → ``calculate_candles(history, period)``,
            # and ``calculate_candles`` ENDS WITH ``return candles[:-1]``.
            # In other words, **pyquotex itself drops the trailing
            # unfinalised candle for us** on this path. No partial /
            # dash bar can leak out.
            #
            # So we now mirror the reference: ``get_candles`` as the primary
            # path, ``get_historical_candles`` only as a fallback for the
            # rare case where ``get_candles`` returns too few rows (e.g. a
            # slow VPS where the broker's first frame is truncated).
            # =================================================================
            primary_fn = getattr(client, "get_candles", None)
            if callable(primary_fn):
                try:
                    # offset is a number-of-SECONDS window, not a count;
                    # request a comfortable headroom over ``target`` so a
                    # possibly-truncated first frame still gives us enough
                    # closed buckets to fill the chart.
                    #
                    # ROOT-CAUSE FIX — the "1-7 dash candles right before
                    # the live candle" bug, fully grounded in pyquotex
                    # source (see github.com/cleitonleonel/pyquotex):
                    #
                    #   pyquotex/_api/history.py::get_candles
                    #     → self.prepare_candles(asset, period, history)
                    #
                    #   pyquotex/_api/history.py::prepare_candles
                    #     candles_data = calculate_candles(history, period)
                    #     # ↑ ends with ``return candles[:-1]`` — drops the
                    #     #   still-forming bucket. Good.
                    #     candles_v2_data = process_candles_v2(
                    #         self.api.candle_v2_data, asset, candles_data
                    #     )
                    #     return merge_candles(candles_v2_data)
                    #
                    #   pyquotex/utils/processor.py::process_candles_v2
                    #     candles = history.get(asset, {}) \
                    #                       .get("candles", [])[1:]
                    #     candles += data
                    #     return candles
                    #
                    # i.e. ``prepare_candles`` PREPENDS pyquotex's
                    # in-memory ``candle_v2_data[asset]`` cache (populated
                    # continuously by the long-running WS stream as
                    # ``candle/v2/data`` frames arrive) to the trimmed
                    # historical list, then dedupes by ``time``. Any
                    # candle with a ``time`` newer than the trimmed
                    # historical tail — i.e. the still-forming bucket AND
                    # the most-recently-arrived partial buckets sitting
                    # in the v2 cache with very few ticks — leaks
                    # straight through. Those low-tick partials surface
                    # as ``open ≈ high ≈ low ≈ close`` "dash" bars
                    # wedged between the (clean) historical buckets and
                    # the (clean) live candle.
                    #
                    # The reference repo (qxlivechart) never sees this
                    # because each chart open uses a freshly-warm
                    # session whose v2 cache is empty. Our backend
                    # multiplexes ALL subscribers onto one persistent
                    # pyquotex session, so ``candle_v2_data`` accrues
                    # forming-bucket partials for every asset we've
                    # ever subscribed to.
                    #
                    # FIX: clear ``candle_v2_data[asset]`` immediately
                    # before invoking ``get_candles``. Then
                    # ``process_candles_v2`` returns just the trimmed
                    # historical list (``data``), so the only candles
                    # ever leaving pyquotex are the ones
                    # ``calculate_candles[:-1]`` has finalised. The WS
                    # stream then immediately repopulates the cache
                    # with fresh frames, so live ticks keep working.
                    try:
                        api = getattr(client, "api", None)
                        v2 = getattr(api, "candle_v2_data", None) if api else None
                        if isinstance(v2, dict):
                            v2[asset] = {"candles": []}
                    except Exception:  # noqa: BLE001
                        # Cache clear is best-effort — pyquotex internals
                        # can change shape between releases. The
                        # downstream ``_drop_trailing_partial_candles``
                        # call remains as a second-level safety net.
                        pass

                    # =========================================================
                    # DEFINITIVE ROOT-CAUSE FIX for the "1-7 dash candles"
                    # bug on the trailing edge of history.
                    #
                    # PROBLEM:
                    # The broker's REST aggregator takes 5-7 minutes to
                    # finalize candle data. When we request history up to
                    # "now", the trailing 1-7 candles are NOT finalized yet
                    # - they contain only 1-5 ticks and appear as "dash"
                    # lines (open == high == low == close).
                    #
                    # pyquotex's calculate_candles[:-1] only drops 1 bucket,
                    # so 2-7 partial/dash candles still leak through.
                    #
                    # FIX:
                    # Set end_from_time to 7 BUCKETS (minutes) in the PAST.
                    # This tells the broker "give me only candles that are
                    # definitely finalized". The WebSocket tick stream will
                    # build the recent 7 candles fresh from live ticks -
                    # those will have proper OHLC data.
                    #
                    # Result:
                    # - REST returns ~192 properly finalized candles
                    # - WS tick stream builds the recent 7 candles live
                    # - NO dash candles appear on the chart
                    # =========================================================
                    _now = time.time()
                    # Go back 15 bucket periods to ensure we only get
                    # fully finalized candles from the broker.
                    # Some low-liquidity markets take up to 10-15 minutes
                    # to finalize their candle data on the broker side.
                    _buckets_back = 15
                    _current_bucket = (int(_now) // int(period)) * int(period)
                    _last_finalized_end = _current_bucket - (_buckets_back * int(period))

                    res = await _maybe_await(
                        primary_fn(
                            asset=asset,
                            end_from_time=_last_finalized_end,
                            offset=int(period) * (target + 15),
                            period=int(period),
                        )
                    )
                    log.info(
                        "get_candles for %s: end_from_time=%d (%d buckets back), "
                        "offset=%d seconds",
                        asset, _last_finalized_end, _buckets_back,
                        int(period) * (target + 15)
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "primary get_candles raised for %s/%s: %s — "
                        "falling back to get_historical_candles",
                        asset, period, exc,
                    )
                    res = None

                primary_candles = (
                    _extract_candle_list(res) if res is not None else []
                )
                if primary_candles:
                    # =====================================================
                    # EXACT REFERENCE BEHAVIOUR — qxlivechart engine.py
                    # ``process_candle_data`` (line 299).
                    #
                    # The reference's logic — proven to never show the
                    # 1-7 dash candles — is:
                    #
                    #   if not raw[0].get("open"):
                    #       return process_candles(raw, period)
                    #   else:
                    #       # normalize types + bucket-align, AS-IS
                    #
                    # That's it. No defensive trimming, no per-item
                    # aggregation, no flat-candle drops. The trick is
                    # that:
                    #
                    #   * ``process_candles`` (pyquotex's own helper
                    #     from ``pyquotex/utils/processor.py``) ends
                    #     with ``return candles[:-1]`` — pyquotex
                    #     itself drops the still-forming bucket.
                    #
                    #   * On the pre-aggregated branch, every item
                    #     returned by ``get_candles`` has already been
                    #     run through pyquotex's internal
                    #     ``calculate_candles(history, period)`` which
                    #     ALSO ends with ``[:-1]`` — so the still-
                    #     forming bucket is already gone before we see
                    #     it. No additional trimming needed.
                    #
                    # Our previous code wrapped this with
                    # ``_aggregate_ticks_to_candles`` +
                    # ``_drop_trailing_partial_candles``, which on rare
                    # broker responses (mixed proper-OHLC + trailing
                    # single-tick items) could BOTH (a) preserve
                    # tick-shaped items with ``high == open == close``
                    # and ``low`` slightly lower (so the flat-trim
                    # missed them) AND (b) bucket-align timestamps the
                    # broker had ALREADY aligned. Result: the 2-7
                    # dash bars the user kept reporting.
                    # =====================================================
                    first = primary_candles[0] if primary_candles else None
                    use_aggregator = (
                        isinstance(first, dict)
                        and first.get("open") is None
                    ) or not isinstance(first, dict)

                    if use_aggregator:
                        # Try pyquotex's own ``process_candles`` first
                        # (mirrors reference). If unavailable for any
                        # reason, fall back to our local aggregator —
                        # both end with the same ``[:-1]`` semantics.
                        normalized_full = None
                        try:
                            from pyquotex.utils.processor import (  # type: ignore
                                process_candles as _pq_process_candles,
                            )
                            raw_aggregated = _pq_process_candles(
                                primary_candles, int(period)
                            )
                            # pyquotex emits {start_time, end_time,
                            # open, high, low, close, ticks}; rename
                            # ``start_time`` → ``time`` and KEEP
                            # ``ticks`` for the adaptive trim.
                            normalized_full = []
                            for c in raw_aggregated or []:
                                if not isinstance(c, dict):
                                    continue
                                try:
                                    t = int(
                                        c.get("start_time", c.get("time", 0))
                                    )
                                    entry = {
                                        "time": (t // int(period)) * int(period),
                                        "open": float(c["open"]),
                                        "high": float(c["high"]),
                                        "low": float(c["low"]),
                                        "close": float(c["close"]),
                                    }
                                    if "ticks" in c:
                                        entry["ticks"] = int(c["ticks"])
                                    normalized_full.append(entry)
                                except (KeyError, ValueError, TypeError):
                                    continue
                        except Exception as exc:  # noqa: BLE001
                            log.warning(
                                "pyquotex.process_candles unavailable "
                                "(%s) — using local aggregator",
                                exc,
                            )
                            aggregated = _aggregate_ticks_to_candles(
                                primary_candles, int(period)
                            )
                            normalized_full = aggregated or []
                            # Local aggregator does NOT do ``[:-1]``,
                            # so apply the same trim here to match
                            # pyquotex's semantics.
                            if normalized_full:
                                normalized_full = sorted(
                                    normalized_full, key=lambda c: c["time"]
                                )[:-1]
                        # Apply the adaptive trim (will use ``ticks``
                        # field when present, falls back to flat
                        # detection otherwise), then strip ``ticks``
                        # before returning to keep downstream OHLC
                        # schema clean.
                        if normalized_full:
                            normalized_full = sorted(
                                normalized_full, key=lambda c: c["time"]
                            )
                            normalized_full = _drop_trailing_partial_candles(
                                normalized_full, int(period)
                            )
                            for f in normalized_full:
                                f.pop("ticks", None)
                    else:
                        # Pre-aggregated OHLC bars from pyquotex's
                        # ``calculate_candles[:-1]``. pyquotex drops
                        # ONLY the very last (still-forming) bucket
                        # for us — but its raw WS history feed often
                        # contains single-tick buckets in the trailing
                        # 2-7 slots (broker REST aggregator hasn't
                        # finalised those yet), and those surface here
                        # as ``open == high == low == close`` dash
                        # bars. THAT is the user-reported "1-7 dash
                        # candles right before the running candle"
                        # symptom — NOT candle_store, NOT pyquotex
                        # version: it's a trailing-partial-bucket
                        # issue in the raw broker stream that pyquotex
                        # only fixes for the very last bucket via
                        # ``[:-1]``.
                        #
                        # ``_drop_trailing_partial_candles`` strips
                        # them so the buffer ends on a real OHLC bar.
                        # The live WS tick stream + boot-time
                        # running-candle gate then rebuild the next
                        # bucket from scratch.
                        formatted: list[dict[str, Any]] = []
                        for c in primary_candles:
                            if not isinstance(c, dict):
                                continue
                            if not all(
                                k in c
                                for k in ("time", "open", "high", "low", "close")
                            ):
                                continue
                            try:
                                ct = int(float(c["time"]))
                                aligned = (ct // int(period)) * int(period)
                                # Preserve the ``ticks`` field that
                                # pyquotex's ``calculate_candles``
                                # attaches — it's the per-bucket tick
                                # count and ``_drop_trailing_partial_candles``
                                # uses it for the adaptive low-liquidity
                                # trim that catches the 2-7 dash bars on
                                # niche OTC pairs.
                                entry = {
                                    "time": aligned,
                                    "open": float(c["open"]),
                                    "high": float(c["high"]),
                                    "low": float(c["low"]),
                                    "close": float(c["close"]),
                                }
                                if "ticks" in c:
                                    try:
                                        entry["ticks"] = int(c["ticks"])
                                    except (ValueError, TypeError):
                                        pass
                                formatted.append(entry)
                            except (ValueError, KeyError, TypeError):
                                continue
                        formatted.sort(key=lambda x: x["time"])
                        formatted = _drop_trailing_partial_candles(
                            formatted, int(period)
                        )
                        # Strip the ``ticks`` field before returning so
                        # downstream consumers (chart frontend, candle
                        # store) see a clean OHLC schema. The trim above
                        # was the only place that needed it.
                        for f in formatted:
                            f.pop("ticks", None)
                        normalized_full = formatted

                    # FINAL SAFETY: Remove "dash" and "near-flat" candles.
                    # These are partial/unfinalised candles that slipped through.
                    #
                    # A candle is considered bad if:
                    # 1. Exact dash: open == high == low == close
                    # 2. Near-flat: (high - low) is extremely small compared
                    #    to the price (less than 0.00001% of price)
                    #
                    # This catches candles with only 1-5 ticks where
                    # high and low differ by a tiny amount like 0.00001
                    def _is_dash_or_near_flat(c: dict) -> bool:
                        o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
                        # Exact dash
                        if o == h == l == cl:
                            return True
                        # Near-flat: range < 0.00001% of price
                        price_range = h - l
                        avg_price = (h + l) / 2 if (h + l) > 0 else 1
                        if avg_price > 0 and (price_range / avg_price) < 0.0000001:
                            return True
                        return False
                    
                    normalized_full = [
                        c for c in (normalized_full or [])
                        if not _is_dash_or_near_flat(c)
                    ]
                    
                    normalized = (normalized_full or [])[-target:]
                    if len(normalized) >= max(20, target // 4):
                        log.info(
                            "history (primary get_candles, "
                            "reference-style) -> %d candles for %s/%s",
                            len(normalized), asset, period,
                        )
                        cache[cache_key] = (time.time(), normalized)
                        return normalized
                    log.info(
                        "primary get_candles returned only %d normalized "
                        "candles for %s/%s (target=%d) — trying "
                        "get_historical_candles",
                        len(normalized), asset, period, target,
                    )

            # =================================================================
            # SECONDARY PATH: ``get_historical_candles``.
            #
            # Only reached when ``get_candles`` returned too few rows. The
            # method uses 5 parallel workers and per-request indexes
            # (``candles_ready_{asset}_{index}``), which is more robust on
            # slow VPS links — but its output is NOT trimmed by pyquotex,
            # so we MUST run ``_drop_trailing_partial_candles`` ourselves
            # before returning.
            # =================================================================
            historical_fn = getattr(client, "get_historical_candles", None)
            if not callable(historical_fn):
                # Some pyquotex forks still expose the deprecated alias.
                historical_fn = getattr(client, "get_candles_deep", None)

            if callable(historical_fn):
                # Generous headroom (target + 20) so we still end up with
                # ``target`` candles AFTER ``_drop_trailing_partial_candles``
                # trims the dash/partial buckets at the live edge.
                amount_of_seconds = int(period) * (target + 20)
                try:
                    res = await _maybe_await(
                        historical_fn(
                            asset=asset,
                            amount_of_seconds=amount_of_seconds,
                            period=int(period),
                        )
                    )
                except TypeError:
                    # Older pyquotex signature (positional only).
                    try:
                        res = await _maybe_await(
                            historical_fn(asset, amount_of_seconds, int(period))
                        )
                    except Exception as exc:  # noqa: BLE001
                        log.warning(
                            "get_historical_candles (positional) raised "
                            "for %s/%s: %s — falling back",
                            asset, period, exc,
                        )
                        res = None
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "get_historical_candles raised for %s/%s: %s — "
                        "falling back to walk-back",
                        asset, period, exc,
                    )
                    res = None

                candles = _extract_candle_list(res) if res is not None else []
                if candles:
                    # Pyquotex's REST endpoint returns RAW TICKS, not
                    # OHLC bars (see _aggregate_ticks_to_candles docstring
                    # for the full explanation). Aggregate first; if the
                    # data was already OHLC the helper returns None and
                    # we fall through to the legacy normalize path.
                    aggregated = _aggregate_ticks_to_candles(
                        candles, int(period)
                    )
                    if aggregated is not None:
                        normalized_full = sorted(
                            aggregated, key=lambda c: c["time"]
                        )
                        # Drop trailing dash / forming-bucket candles
                        # BEFORE slicing to ``target``. See
                        # ``_drop_trailing_partial_candles`` for the
                        # full rationale.
                        normalized_full = _drop_trailing_partial_candles(
                            normalized_full, int(period)
                        )
                        normalized = normalized_full[-target:]
                        log.info(
                            "history (get_historical_candles, "
                            "tick-aggregated) -> %d candles from %d "
                            "ticks for %s/%s (trimmed partials)",
                            len(normalized), len(candles), asset, period,
                        )
                    else:
                        normalized_full = sorted(
                            (
                                n
                                for n in (
                                    _normalize_candle(c) for c in candles
                                )
                                if n
                            ),
                            key=lambda c: c["time"],
                        )
                        normalized_full = _drop_trailing_partial_candles(
                            normalized_full, int(period)
                        )
                        normalized = normalized_full[-target:]
                        log.info(
                            "history (get_historical_candles, "
                            "pre-aggregated) -> %d candles for %s/%s "
                            "(trimmed partials)",
                            len(normalized), asset, period,
                        )
                    if len(normalized) >= max(20, target // 4):
                        cache[cache_key] = (time.time(), normalized)
                        return normalized
                log.info(
                    "get_historical_candles returned %d candles for %s/%s "
                    "(target=%d) — running walk-back fallback",
                    len(candles), asset, period, target,
                )

            # -------------------------------------------------------------
            # FALLBACK PATH: walk-back over small ``get_candles`` chunks.
            #
            # Reached when (a) the installed pyquotex fork doesn't expose
            # ``get_historical_candles`` / ``get_candles_deep``, or
            # (b) the official call returned a suspiciously small batch.
            # Smaller per-call windows (60 candles) reliably fit a single
            # WS frame even on slow VPS links, and accumulating across
            # walk-back iterations always converges to the full window.
            fn = getattr(client, "get_candles", None)
            if not callable(fn):
                log.warning("client has no get_candles method")
                return []

            CHUNK = 60
            MAX_CHUNKS = 10
            PER_CHUNK_BACKOFF_S = 0.4

            accumulated: dict[int, dict] = {}
            end_from_time = float(time.time())

            for chunk_idx in range(1, MAX_CHUNKS + 1):
                chunk_offset = CHUNK * int(period)

                chunk_candles: list[dict] = []
                for _chunk_attempt in (1, 2):
                    try:
                        res = await _maybe_await(
                            fn(
                                asset=asset,
                                end_from_time=end_from_time,
                                offset=chunk_offset,
                                period=int(period),
                            )
                        )
                    except Exception as exc:  # noqa: BLE001
                        log.warning(
                            "fallback get_candles raised for %s/%s: %s",
                            asset, period, exc,
                        )
                        res = None
                    chunk_candles = (
                        _extract_candle_list(res) if res is not None else []
                    )
                    if chunk_candles:
                        break
                    await asyncio.sleep(0.6)

                if not chunk_candles:
                    break

                # Same tick-vs-OHLC detection as the primary path —
                # ``get_candles`` is even more likely to yield raw ticks
                # than ``get_historical_candles``. Aggregating here
                # guarantees the walk-back fallback also produces real
                # OHLC bars instead of one flat candle per tick.
                aggregated = _aggregate_ticks_to_candles(
                    chunk_candles, int(period)
                )
                normalized_chunk: list[dict[str, Any]]
                if aggregated is not None:
                    normalized_chunk = aggregated
                else:
                    normalized_chunk = [
                        n
                        for n in (
                            _normalize_candle(raw) for raw in chunk_candles
                        )
                        if n
                    ]

                new_oldest = end_from_time
                for norm in normalized_chunk:
                    t = int(norm["time"])
                    # Prefer the version with the widest high-low
                    # range when the same bucket appears in two
                    # overlapping chunks — protects against truncation
                    # at chunk boundaries.
                    prev = accumulated.get(t)
                    if prev is not None:
                        try:
                            prev_range = float(prev["high"]) - float(
                                prev["low"]
                            )
                            new_range = float(norm["high"]) - float(
                                norm["low"]
                            )
                        except (KeyError, TypeError, ValueError):
                            prev_range = -1.0
                            new_range = 0.0
                        if new_range > prev_range:
                            accumulated[t] = norm
                    else:
                        accumulated[t] = norm
                    if t < new_oldest:
                        new_oldest = t

                log.info(
                    "fallback chunk %d -> %d total for %s/%s",
                    chunk_idx, len(accumulated), asset, period,
                )

                if len(accumulated) >= target:
                    break
                if new_oldest >= end_from_time:
                    break
                end_from_time = new_oldest - int(period)
                await asyncio.sleep(PER_CHUNK_BACKOFF_S)

            if not accumulated:
                log.warning(
                    "no history available for %s/%s via either path",
                    asset, period,
                )
                return []

            normalized_full = sorted(
                accumulated.values(), key=lambda c: c["time"]
            )
            # Drop trailing dash / partial buckets here too — the walk-back
            # path is even MORE susceptible to single-tick trailing bars
            # than the primary ``get_historical_candles`` path.
            normalized_full = _drop_trailing_partial_candles(
                normalized_full, int(period)
            )
            normalized = normalized_full[-target:]
            log.info(
                "history (walk-back fallback) -> %d candles for %s/%s "
                "(trimmed partials)",
                len(normalized), asset, period,
            )
            cache[cache_key] = (time.time(), normalized)
            return normalized

    async def fetch_forming_candle_rest(
        self, asset: str, period: int
    ) -> dict[str, Any] | None:
        """
        Hit Quotex REST get_candles with end_time ~= now, push the close price
        into `_last_tick` so the main bucket state machine picks it up, then
        return the current forming candle built from the unified state.

        This ensures the REST fallback feeds the SAME state machine that the
        fast poller uses — so we never flip-flop between the two sources.
        """
        client = self.client
        if client is None:
            return None

        fn = getattr(client, "get_candles", None)
        if not callable(fn):
            return None

        # Share the per-asset lock with get_history / fetch_closed_bucket_*.
        # Without this the 1 s REST poller can fire ``candles_data = None``
        # right while the one-time history fetch is mid-wait, killing it.
        lock = self._asset_history_locks.get(asset)
        if lock is None:
            lock = asyncio.Lock()
            self._asset_history_locks[asset] = lock

        async with lock:
            # Single kwargs call — same shape as get_history.
            try:
                res = await _maybe_await(
                    fn(
                        asset=asset,
                        end_from_time=float(time.time()) + float(period),
                        offset=int(period) * 3,
                        period=int(period),
                    )
                )
            except Exception as exc:  # noqa: BLE001
                log.debug("fetch_forming get_candles failed for %s/%s: %s",
                          asset, period, exc)
                return None

            candles = _extract_candle_list(res)
            if not candles:
                log.debug("fetch_forming: no candles extracted for %s/%s (res type=%s)",
                          asset, period, type(res).__name__)
                return None

            norm = [c for c in (_normalize_candle(c) for c in candles) if c]
            if not norm:
                log.debug("fetch_forming: normalization yielded empty list for %s/%s",
                          asset, period)
                return None
            norm.sort(key=lambda c: c["time"])
            latest = norm[-1]

            # Feed the close price into the tick cache so _scrape_latest_price
            # (and therefore get_latest_candle) sees the up-to-date REST price.
            price = float(latest["close"])
            ts = float(latest.get("time") or time.time())
            prev = self._last_tick.get(asset)
            if prev is None or ts >= prev[0]:
                self._last_tick[asset] = (ts, price)

        # Outside the lock — get_latest_candle doesn't touch pyquotex
        # internals, just our own state machine.
        return await self.get_latest_candle(asset, period)

    # ---------------------------------------------------------- price scraping

    def _scrape_latest_price(self, asset: str) -> tuple[float, float] | None:
        """
        Return the freshest (timestamp_seconds, price) we can find for `asset`
        from ANY pyquotex internal source. Tries, in order:

          1. Hooked raw ws tick (most up-to-date).
          2. api.realtime_price[asset]        (dict OR list of entries)
          3. api.realtime_candles[asset][p]   (last candle's close)
          4. api.candles[asset]               (last closed candle's close)

        This is the engine that keeps the chart moving every 100ms even when
        individual sources lag or go quiet for a few seconds.
        """
        client = self.client
        if client is None:
            return None

        best_ts: float | None = None
        best_price: float | None = None

        def _consider(ts: float | None, price: float | None) -> None:
            nonlocal best_ts, best_price
            if price is None:
                return
            if ts is None:
                ts = time.time()
            if ts > 10_000_000_000:
                ts /= 1000.0
            if best_ts is None or ts >= best_ts:
                best_ts = float(ts)
                best_price = float(price)

        # 1. Hooked raw ws tick.
        hooked = self._last_tick.get(asset)
        if hooked is not None:
            _consider(hooked[0], hooked[1])

        api = getattr(client, "api", None)

        # 2. api.realtime_price — can be dict OR list OR dict-of-dicts.
        if api is not None:
            rp = getattr(api, "realtime_price", None)
            if isinstance(rp, dict):
                blob = rp.get(asset)
                if isinstance(blob, dict) and blob:
                    try:
                        last_key = max(blob.keys(), key=lambda k: float(k))
                        entry = blob[last_key]
                        price = entry.get("price") if isinstance(entry, dict) else entry
                        _consider(float(last_key), price)
                    except Exception:
                        pass
                elif isinstance(blob, list) and blob:
                    # Most forks store a list of {price, time} dicts.
                    last = blob[-1]
                    if isinstance(last, dict):
                        _consider(last.get("time") or last.get("timestamp"), last.get("price"))
                    elif isinstance(last, (int, float)):
                        _consider(None, float(last))

        # 3. api.realtime_candles[asset][period] — use latest candle's close.
        if api is not None:
            rc = getattr(api, "realtime_candles", None)
            if isinstance(rc, dict):
                blob = rc.get(asset)
                if isinstance(blob, dict):
                    # Walk every period bucket and grab the newest close.
                    for sub in blob.values():
                        if not isinstance(sub, dict) or not sub:
                            continue
                        try:
                            last_key = max(sub.keys(), key=lambda k: float(k))
                            entry = sub[last_key]
                            price = entry.get("close") if isinstance(entry, dict) else None
                            _consider(float(last_key), price)
                        except Exception:
                            continue
                elif isinstance(blob, list) and blob:
                    last = blob[-1]
                    if isinstance(last, dict):
                        _consider(
                            last.get("time") or last.get("from"),
                            last.get("close") or last.get("c"),
                        )

        # 4. api.candles — last completed candle.
        if api is not None:
            c = getattr(api, "candles", None)
            if isinstance(c, dict):
                blob = c.get(asset)
                if isinstance(blob, list) and blob:
                    last = blob[-1]
                    if isinstance(last, dict):
                        _consider(
                            last.get("time") or last.get("from"),
                            last.get("close") or last.get("c"),
                        )

        if best_price is None:
            return None
        return (best_ts or time.time(), best_price)

    def _get_server_forming_ohlc(
        self, asset: str, period: int, bucket: int
    ) -> dict[str, float] | None:
        """
        Read the FULL OHLC of the currently forming candle from pyquotex's
        internal `api.realtime_candles[asset][period]` dict. The server pushes
        per-tick OHLC updates into this dict, so the high/low are already
        accumulated correctly even when `get_realtime_price` returns sparse
        ticks (which is what was causing flat "dash" candles for low-activity
        OTC pairs).

        Returns None if no server OHLC is available for the CURRENT bucket.
        We only accept an entry whose timestamp matches `bucket` — stale
        entries from a previous bucket must NOT be returned (that would freeze
        the forming bar at the old close).
        """
        client = self.client
        if client is None:
            return None
        api = getattr(client, "api", None)
        if api is None:
            return None
        rc = getattr(api, "realtime_candles", None)
        if not isinstance(rc, dict):
            return None
        blob = rc.get(asset)
        if not isinstance(blob, dict):
            return None

        # The period key may be an int, str, or the blob may be a flat
        # timestamp-keyed dict (single-period fork). Handle all variants.
        sub = blob.get(period)
        if sub is None:
            sub = blob.get(str(period))
        if sub is None and all(_is_number_like(k) for k in blob.keys()):
            sub = blob  # already a ts->candle dict for this asset

        if not isinstance(sub, dict) or not sub:
            return None

        # Find the entry whose bucket timestamp matches the CURRENT bucket.
        # Prefer exact match; if not found, use the highest ts <= bucket + period.
        candidates: list[tuple[int, Any]] = []
        for k, v in sub.items():
            try:
                ki = int(float(k))
            except Exception:
                continue
            if ki > 10_000_000_000:  # ms -> s
                ki //= 1000
            candidates.append((ki, v))
        if not candidates:
            return None
        candidates.sort(key=lambda kv: kv[0])

        # Exact bucket match only — anything older is a closed candle and must
        # not be used to paint the forming bar (that's how we end up with
        # repeated flat dashes at yesterday's close).
        match = next((v for k, v in candidates if k == bucket), None)
        if match is None:
            # The very newest entry might be our bucket in a slightly shifted
            # form (e.g., server clock = local + 1s). Accept if within period/2.
            newest_k, newest_v = candidates[-1]
            if abs(newest_k - bucket) <= max(1, period // 2):
                match = newest_v
        if match is None or not isinstance(match, dict):
            return None

        try:
            o = float(match.get("open", match.get("o")))
            h = float(match.get("high", match.get("h")))
            l = float(match.get("low", match.get("l")))
            c = float(match.get("close", match.get("c")))
        except (TypeError, ValueError):
            return None

        return {"open": o, "high": h, "low": l, "close": c}

    def _archive_closed_bucket(
        self, key: tuple[str, int], state: dict[str, Any]
    ) -> None:
        """
        Push the just-closed bucket's final OHLC onto the pending queue so
        the polling loop in main.py can broadcast it one last time before
        the new forming bucket replaces it.

        Two-stage commit
        ----------------
        Stage 1 (this method, synchronous): the WS-built final OHLC is pushed
        onto the queue immediately so the chart never has to wait �� the closed
        bar's wick that we *did* observe lands on the frontend within the next
        polling tick (~100 ms).

        Stage 2 (scheduled async, see :meth:`_refetch_closed_bucket`): a
        background task fires ~500 ms later and asks pyquotex's REST
        ``get_candles`` for the broker-authoritative OHLC of this exact
        bucket. The authoritative candle is then enqueued under the same
        bucket time, so the next drain emits it as a same-time update and
        lightweight-charts atomically replaces the WS-built bar.

        Net effect: the user sees an immediate closed bar (no flicker, no
        gap), and any micro-discrepancy with the broker's authoritative
        record is silently corrected sub-second later. Wrong wicks from
        missed live ticks are eliminated because the broker's own final
        OHLC always wins the tiebreak.

        We defensively normalise high/low against open/close so a malformed
        state (e.g. a single-tick bucket where high accidentally ended up
        below close due to floating-point rounding) still renders as a
        valid candle on the frontend.
        """
        try:
            o = float(state["open"])
            h = float(state["high"])
            lo = float(state["low"])
            c = float(state["close"])
        except (KeyError, TypeError, ValueError):
            return
        # Guarantee high >= max(o, c) and low <= min(o, c) — lightweight-charts
        # silently drops candles that violate this invariant.
        h = max(h, o, c)
        lo = min(lo, o, c)
        bucket_time = int(state["bucket"])
        candle = {
            "time": bucket_time,
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "volume": 0.0,
            # Marks this candle as the *final* WS-aggregated OHLC for a
            # just-closed bucket. ``CandleStore.push()`` honors this flag
            # and bypasses both the wall-clock and locked-bucket guards
            # so the closed candle gets its full accumulated high/low
            # stored. Without the flag the guards (which exist to drop
            # *stale* ticks) also blocked this legitimate finalization —
            # the recent closed bucket then stayed locked with stale
            # mid-poll OHLC and rendered as a flat "line" on the
            # frontend chart ("ager 1-7 ta candle line dekhay" bug).
            "final": True,
        }
        q = self._closed_buckets_pending.setdefault(key, [])
        # If the same bucket is already queued (rare but possible when both
        # tick-driven and wall-clock advance fire in the same poll), update
        # in place rather than enqueueing twice.
        replaced = False
        for i in range(len(q) - 1, -1, -1):
            if int(q[i].get("time", -1)) == candle["time"]:
                # Don't clobber an authoritative candle with WS data —
                # the broker's REST result is strictly more trustworthy.
                if not q[i].get("authoritative"):
                    q[i] = candle
                replaced = True
                break
        if not replaced:
            q.append(candle)
            # Cap the queue so a stalled subscriber can't pile up unbounded
            # closed buckets (e.g. if the consumer crashed between polls).
            if len(q) > 32:
                del q[: len(q) - 32]

        # Stage 2: schedule the broker-authoritative refetch. We only do
        # this from within an active event loop — when the state machine
        # is being driven from a thread without a loop (rare, but possible
        # during pytest or sync utility calls), we silently skip and the
        # WS-built candle remains the final word, which is the same
        # behaviour as before this patch.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        # Dedup pending refetches per bucket: if a refetch is already in
        # flight for this exact key+bucket, don't schedule a second one.
        in_flight = self._authoritative_in_flight.setdefault(key, set())
        if bucket_time in in_flight:
            return
        in_flight.add(bucket_time)
        loop.create_task(self._refetch_closed_bucket(key, bucket_time))

    def request_authoritative_refetch(
        self, asset: str, period: int, bucket_time: int
    ) -> bool:
        """
        Public hook to ask for broker-authoritative REST OHLC for a
        specific *already-closed* bucket. Used by ``main.py`` after the
        periodic history refresh inserts synthetic flat placeholders for
        trailing buckets that REST has not yet materialised — those
        synthetics need to be replaced with real OHLC fast (within a few
        seconds), not 60 s later on the next history refresh.

        Returns ``True`` if a refetch was scheduled, ``False`` if one is
        already in flight for this exact bucket or no event loop is
        available (e.g. called from sync test code).

        The scheduled refetch reuses :meth:`_refetch_closed_bucket`, so
        the resulting candle is enqueued onto ``_closed_buckets_pending``
        with ``"authoritative": True``. The polling loop in main.py will
        drain it and push it into :class:`CandleStore` — the new push()
        path recognises ``authoritative=True`` and overwrites the
        synthetic placeholder even though the slot was previously
        locked.
        """
        key = (asset, int(period))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        in_flight = self._authoritative_in_flight.setdefault(key, set())
        if bucket_time in in_flight:
            return False
        in_flight.add(bucket_time)
        loop.create_task(self._refetch_closed_bucket(key, bucket_time))
        return True

    async def _refetch_closed_bucket(
        self, key: tuple[str, int], bucket_time: int
    ) -> None:
        """
        Wait briefly for broker-side aggregation to finalise, then ask
        pyquotex's REST ``get_candles`` for the authoritative OHLC of the
        just-closed bucket and enqueue it under the same bucket time so
        the polling loop emits it as a same-time update.

        Why the 500 ms delay
        --------------------
        Quotex's REST aggregator typically needs ~100–300 ms after a
        bucket boundary to publish that bucket's final OHLC. Fetching too
        eagerly returns either nothing for the requested bucket or the
        same partial OHLC the WS already gave us (defeats the point).
        500 ms is the empirical sweet spot — late enough to be reliable,
        early enough to feel instantaneous in the chart.
        """
        asset, period = key
        try:
            await asyncio.sleep(0.5)
            candle = await self.fetch_closed_bucket_authoritative(
                asset, period, bucket_time
            )
            if candle is None:
                return
            try:
                ao = float(candle["open"])
                ah = float(candle["high"])
                al = float(candle["low"])
                ac = float(candle["close"])
            except (KeyError, TypeError, ValueError):
                return
            ah = max(ah, ao, ac)
            al = min(al, ao, ac)
            # Quotex's REST aggregator sometimes echoes a single tick
            # as all four OHLC values (o == h == l == c) for buckets it
            # hasn't fully materialised yet. Publishing that as
            # ``authoritative: True`` would overwrite the WS-aggregated
            # OHLC (which DOES have real range from per-tick high/low
            # tracking) via the locked-bypass path in CandleStore.push,
            # turning the bar into a flat horizontal line on the chart.
            # We refuse the refetch result in that case — the WS data
            # already in the store is the better source and stays put.
            if ao == ah == al == ac:
                log.debug(
                    "authoritative refetch for %s/%s bucket=%d returned "
                    "flat OHLC (%.6f) — discarding to preserve "
                    "WS-aggregated range",
                    asset, period, bucket_time, ao,
                )
                return
            auth = {
                "time": bucket_time,
                "open": ao,
                "high": ah,
                "low": al,
                "close": ac,
                "volume": float(candle.get("volume", 0.0) or 0.0),
                "authoritative": True,
            }
            q = self._closed_buckets_pending.setdefault(key, [])
            # If the WS-built candle is still in the queue (drain hasn't
            # run yet), replace it in place. Otherwise append so the next
            # drain picks up the authoritative version and emits it as a
            # same-time update — lightweight-charts will replace the bar.
            for i in range(len(q) - 1, -1, -1):
                if int(q[i].get("time", -1)) == bucket_time:
                    q[i] = auth
                    break
            else:
                q.append(auth)
                if len(q) > 32:
                    del q[: len(q) - 32]
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "authoritative refetch failed for %s/%s bucket=%d: %s",
                asset,
                period,
                bucket_time,
                exc,
            )
        finally:
            in_flight = self._authoritative_in_flight.get(key)
            if in_flight is not None:
                in_flight.discard(bucket_time)

    async def fetch_closed_bucket_authoritative(
        self, asset: str, period: int, bucket_time: int
    ) -> dict[str, Any] | None:
        """
        Ask pyquotex's REST ``get_candles`` for the broker-authoritative
        OHLC of a single just-closed bucket.

        We request a small window centred on ``bucket_time`` and pick the
        candle whose timestamp matches exactly (or, failing that, the
        closest one within ``period / 2`` — Quotex's clock is sometimes
        off by a fractional second from the local wall-clock).

        Returns ``None`` if the broker hasn't published this bucket yet
        (caller is expected to retry on the next bucket close — but in
        practice the 500 ms delay in :meth:`_refetch_closed_bucket` makes
        misses negligible) or if pyquotex itself isn't connected.
        """
        client = self.client
        if client is None:
            return None
        direct = await self.history_load(
            asset, int(period), bucket_time + int(period) * 2, int(period) * 5
        )
        for c in direct:
            if int(c["time"]) == bucket_time:
                return c
        # Overshoot the bucket end by 2 periods so the broker definitely
        # includes our target candle in the response. The offset window
        # is intentionally small (5 buckets) — a single closed bucket is
        # all we need, and a tight window keeps broker load minimal.
        end_time = bucket_time + int(period) * 2
        offset = int(period) * 5
        # Single kwargs call (proven to work — see get_history docstring
        # for why multi-shape concurrent retries break things on VPS).
        fn = getattr(client, "get_candles", None)
        if not callable(fn):
            return None

        # Share the per-asset lock so this doesn't wipe an in-flight
        # get_history() or fetch_forming_candle_rest() call.
        lock = self._asset_history_locks.get(asset)
        if lock is None:
            lock = asyncio.Lock()
            self._asset_history_locks[asset] = lock

        async with lock:
            try:
                res = await _maybe_await(
                    fn(
                        asset=asset,
                        end_from_time=float(end_time),
                        offset=offset,
                        period=int(period),
                    )
                )
            except Exception as exc:  # noqa: BLE001
                log.debug("authoritative get_candles failed for %s/%s: %s",
                          asset, period, exc)
                return None
            candles = _extract_candle_list(res)
            if not candles:
                return None
        # Pyquotex returns raw ticks here too — same as get_history.
        # Aggregate them into proper OHLC bars before scanning for the
        # target bucket; otherwise the bucket we pick is a single
        # tick's price echoed as o==h==l==c (a flat line on the
        # chart). See ``_aggregate_ticks_to_candles`` for the full
        # explanation.
        aggregated = _aggregate_ticks_to_candles(candles, int(period))
        if aggregated is not None:
            norm = aggregated
        else:
            norm = [c for c in (_normalize_candle(c) for c in candles) if c]
        if not norm:
            return None
        # Exact match wins — broker timestamps are second-precise.
        for c in norm:
            if int(c["time"]) == bucket_time:
                return c
        # Tolerant match for the rare case where Quotex's bucket is
        # off by ≤ period/2 from our calculation.
        norm.sort(key=lambda c: abs(int(c["time"]) - bucket_time))
        if abs(int(norm[0]["time"]) - bucket_time) <= max(1, period // 2):
            return norm[0]
        return None

    def pull_pending_closed(
        self, asset: str, period: int
    ) -> list[dict[str, Any]]:
        """
        Drain and return the FIFO of closed-bucket candles for (asset, period).

        Called by the polling loop in main.py once per tick — every entry
        is broadcast as a regular ``{"type":"candle"}`` message so the
        chart's last-rendered bar gets its final wick before the new
        forming bar appends.
        """
        key = (asset, int(period))
        q = self._closed_buckets_pending.get(key)
        if not q:
            return []
        # Hand out the list, replace with empty so subsequent calls don't
        # re-emit the same candles. Sort defensively in case archive order
        # got out of sync (it shouldn't, but cheap insurance).
        out = sorted(q, key=lambda c: int(c.get("time", 0)))
        self._closed_buckets_pending[key] = []
        return out

    def _apply_tick_to_state(
        self, key: tuple[str, int], period: int, ts: float, price: float
    ) -> None:
        """
        Apply a single (timestamp, price) tick to the bucket state machine —
        same pure in-memory OHLC accumulation the reference project uses.

        - If we're still in the same bucket: extend high/low, update close.
        - If this tick belongs to a newer bucket: archive the just-closed
          bucket (so its final wick gets one last broadcast), then open a
          fresh candle with price as O=H=L=C.

        The tick timestamp drives the bucket decision (not wall-clock) so that
        ticks arriving slightly late don't accidentally roll into the next
        bucket and discard their high/low information.
        """
        period = int(period)
        bucket = int(ts) - (int(ts) % period)
        state = self._bucket_state.get(key)

        if state is None or bucket > state["bucket"]:
            # Bucket rollover via a tick — archive the previous bucket's
            # full OHLC so the polling loop can broadcast it one final
            # time. Without this, the wick we accumulated over the last
            # period of ticks would be silently overwritten below and the
            # frontend would render the closed bar with whatever OHLC it
            # received in the previous poll (i.e. mid-bucket, missing the
            # final wick).
            if state is not None and bucket > state["bucket"]:
                self._archive_closed_bucket(key, state)
            # New bucket — open at the first tick's price.
            state = {
                "bucket": bucket,
                "open": float(price),
                "high": float(price),
                "low": float(price),
                "close": float(price),
                "last_ts": float(ts),
            }
        elif bucket == state["bucket"]:
            # Same bucket — accumulate high/low, update close.
            if price > state["high"]:
                state["high"] = float(price)
            if price < state["low"]:
                state["low"] = float(price)
            state["close"] = float(price)
            state["last_ts"] = float(ts)
        else:
            # Stale tick (bucket < current) — ignore.
            return

        self._bucket_state[key] = state
        self._forming_cache[key] = state

    async def get_latest_candle(
        self, asset: str, period: int
    ) -> dict[str, Any] | None:
        """
        Return the *currently forming* candle for (asset, period) with live OHLC.

        Uses a deterministic wall-clock-driven bucket state machine:
          - Every call, compute the current time bucket.
          - Fetch the freshest price from pyquotex's public `get_realtime_price`
            API (the same method the working reference project uses) and then
            fall back to scraping internal dicts only if that fails.
          - If the bucket rolled over, start a new forming candle (open = last price).
          - Otherwise update close, and extend high/low with the new price.

        This guarantees smooth second-by-second chart updates even when the
        underlying ws tick stream is quiet or the pyquotex hook failed.
        """
        client = self._require()
        period = int(period)
        key = (asset, period)

        # --- Apply EVERY new tick from pyquotex to the bucket state ---------
        # `await client.get_realtime_price(asset)` returns the list of
        # accumulated ticks since the last call: [{"time": <ts>, "price": ...}, ...].
        # The reference project's `update_candle()` applies each tick to the
        # OHLC state so high/low accumulate properly over the bucket. Our
        # previous version only kept `data[-1]` — discarding the intermediate
        # ticks that carry the true high/low information, which is exactly why
        # closed candles were showing up as flat horizontal dashes.
        ticks_applied = 0
        fn = getattr(client, "get_realtime_price", None)
        if callable(fn):
            try:
                data = await _maybe_await(fn(asset))
                if data and isinstance(data, list):
                    for entry in data:
                        if not isinstance(entry, dict):
                            continue
                        ep = entry.get("price")
                        if ep is None:
                            ep = entry.get("close")
                        if ep is None:
                            continue
                        et = entry.get("time") or entry.get("timestamp")
                        et_val = float(et) if et is not None else time.time()
                        if et_val > 10_000_000_000:
                            et_val /= 1000.0
                        price_f = float(ep)
                        # Keep newest seen tick for _scrape_latest_price fallback.
                        prev = self._last_tick.get(asset)
                        if prev is None or et_val >= prev[0]:
                            self._last_tick[asset] = (et_val, price_f)
                        # Apply this tick to the bucket state machine.
                        self._apply_tick_to_state(key, period, et_val, price_f)
                        ticks_applied += 1
                    if ticks_applied:
                        self.last_tick_time = time.time()
            except Exception as exc:  # noqa: BLE001
                log.debug("get_realtime_price(%s) failed: %s", asset, exc)

        # If nothing arrived via the public API this poll, try scraping any
        # internal dict pyquotex exposes. Apply whatever we find as a single
        # tick — still lets the state machine breathe on quiet pairs.
        if ticks_applied == 0:
            scraped = self._scrape_latest_price(asset)
            if scraped is not None:
                self._apply_tick_to_state(key, period, scraped[0], scraped[1])

        now_ts = time.time()
        bucket = int(now_ts) - (int(now_ts) % period)
        state = self._bucket_state.get(key)

        # Guarantee the bucket advances on wall-clock time even when no tick
        # arrived — otherwise the chart would freeze on a stale bucket. We
        # open the new bucket flat at previous close for continuity, which is
        # ok because future ticks in this new bucket will extend high/low.
        if state is not None and bucket > state["bucket"]:
            # Archive the just-closed bucket's full OHLC FIRST so the
            # polling loop can broadcast it one last time with all
            # accumulated wick info. Without this, the wick we collected
            # over the last period of ticks gets silently dropped on
            # bucket rollover — which is precisely the "wick dekhacche
            # na" symptom on closed candles.
            self._archive_closed_bucket(key, state)
            price = float(state["close"])
            state = {
                "bucket": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "last_ts": now_ts,
            }
            self._bucket_state[key] = state

        if state is None:
            # Still no price anywhere — fall back to server-built OHLC if any.
            server = self._get_server_forming_ohlc(asset, period, bucket)
            if server is not None:
                state = {
                    "bucket": bucket,
                    "open": server["open"],
                    "high": server["high"],
                    "low": server["low"],
                    "close": server["close"],
                    "last_ts": now_ts,
                }
                self._bucket_state[key] = state
            else:
                return None

        # --- Merge with server-built OHLC when available ---------------------
        # pyquotex's internal `api.realtime_candles` carries proper server OHLC
        # updated on every tick. On low-activity OTC pairs get_realtime_price
        # may emit only 1 tick per bucket, but the server OHLC already has
        # accumulated high/low for that same bucket. Use it as a reinforcement.
        server = self._get_server_forming_ohlc(asset, period, state["bucket"])
        if server is not None:
            # When our local state is still flat (only one tick accumulated)
            # trust the server's open so the candle doesn't appear as a dash.
            if state["high"] == state["low"] == state["close"] == state["open"]:
                state["open"] = server["open"]
            state["high"] = max(state["high"], server["high"])
            state["low"] = min(state["low"], server["low"])
            state["close"] = server["close"]
            self._bucket_state[key] = state
        self._forming_cache[key] = state

        return {
            "time": state["bucket"],
            "open": float(state["open"]),
            "high": float(state["high"]),
            "low": float(state["low"]),
            "close": float(state["close"]),
            "volume": 0.0,
        }


# --------------------------------------------------------------------------- #
# Candle extraction helpers
# --------------------------------------------------------------------------- #


def _extract_candle_list(res: Any) -> list[Any]:
    """
    pyquotex fork variations:
      - list of dicts
      - dict with "data"/"candles" key
      - dict keyed by timestamp -> candle
    """
    if res is None:
        return []
    if isinstance(res, list):
        return res
    if isinstance(res, dict):
        for k in ("data", "candles", "result"):
            v = res.get(k)
            if isinstance(v, list):
                return v
        # timestamp-keyed dict
        if res and all(_is_number_like(k) for k in res.keys()):
            return [
                ({**v, "time": v.get("time") or int(float(k))} if isinstance(v, dict) else v)
                for k, v in res.items()
            ]
    return []


def _latest_from_blob(data: Any) -> dict[str, Any] | None:
    if not data:
        return None
    if isinstance(data, dict):
        try:
            key = max(data.keys(), key=lambda k: float(k))
        except Exception:
            key = list(data.keys())[-1]
        return _normalize_candle(data[key], default_time=int(float(key)))
    if isinstance(data, list):
        return _normalize_candle(data[-1])
    return None


def _is_number_like(k: Any) -> bool:
    try:
        float(k)
        return True
    except Exception:
        return False
