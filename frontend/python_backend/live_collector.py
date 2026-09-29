"""All-markets live tick collector (same pattern as crt-chk ``ticks.py``).

Subscribes EVERY open Quotex market on the single pyquotex websocket, drains
``client.api.realtime_price`` tick buffers and builds candles for each
period from the first aligned boundary. Browsers only read these candles —
no broker request is ever made on a client's behalf.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import re
import time
from typing import Any, Awaitable, Callable

from pyquotex.utils.processor import calculate_candles

log = logging.getLogger("live-collector")

Key = tuple[str, int]
KeyCb = Callable[[Key, Any], Awaitable[None]]
MAX_CANDLES = 200
HISTORY_COUNT = 199
V2_TIMEOUT_S = 8.0
BACKFILL_ATTEMPTS = 3


def _spacing(candles: list[dict[str, Any]]) -> int:
    """Most common gap between consecutive candle times (0 if unknown)."""
    gaps: dict[int, int] = {}
    for a, b in zip(candles, candles[1:]):
        g = int(b["time"]) - int(a["time"])
        gaps[g] = gaps.get(g, 0) + 1
    return max(gaps, key=gaps.get) if gaps else 0


async def _maybe_await(res: Any) -> Any:
    return await res if inspect.isawaitable(res) else res


_CATEGORY_ALIASES = {
    "currency": "currencies", "currencies": "currencies", "forex": "currencies",
    "cryptocurrency": "crypto", "crypto": "crypto",
    "commodity": "commodities", "commodities": "commodities",
    "stock": "stocks", "stocks": "stocks",
    "index": "indices", "indices": "indices",
}
_CRYPTO_BASES = {
    "BTC", "ETH", "ADA", "APT", "ARB", "ATO", "AVA", "AVAX", "AXS", "BCH", "BNB", "BON",
    "DAS", "DOG", "DOGE", "DOT", "ETC", "FLO", "GAL", "HMS", "LIN", "LINK", "LTC", "MEL",
    "SHIB", "SOL", "TIA", "TON", "TRU", "TRX", "WIF", "XRP", "ZEC", "MATIC", "TRUMP",
    "BEE", "NOT", "SUI", "NEAR", "FIL", "UNI", "XLM", "ICP", "INJ", "OP", "PEPE", "SEI",
}
_FIAT = set(
    "USD EUR GBP JPY CHF AUD CAD NZD BRL MXN ARS PKR PHP INR BDT IDR TRY ZAR EGP NGN DZD "
    "COP CLP PEN SGD HKD CNY CNH KRW THB MYR VND RUB UAH SAR AED QAR KWD BHD OMR JOD LBP "
    "YER SYP IRR ILS KES TND MAD CZK PLN HUF SEK NOK DKK RON BGN ISK KZT UZS LKR NPR TWD".split()
)
_COMMODITY_PREFIXES = ("XAU", "XAG", "XPT", "XPD", "XNG", "UKBRENT", "USCRUDE", "BRENT", "WTI", "NATGAS")
_INDEX_SYMBOLS = {
    "DJIUSD", "NDXUSD", "SPXUSD", "F40EUR", "FTSGBP", "HSIHKD", "IBXEUR", "JPXJPY",
    "CHIA50", "STXEUR", "D30EUR", "DAXEUR", "AUS200", "E35EUR", "E50EUR",
}


_COMMODITY_NAME = re.compile(r"\b(gold|silver|brent|crude|oil|natural gas|platinum|palladium|copper)\b")
_CRYPTO_NAMES = {
    "bitcoin", "ethereum", "binance coin", "ripple", "litecoin", "bitcoin cash", "cardano",
    "dogecoin", "polkadot", "solana", "tron", "chainlink", "avalanche", "cosmos",
    "ethereum classic", "zcash", "dash", "shiba inu", "polygon", "axie infinity", "toncoin",
    "aptos", "arbitrum", "bonk", "floki", "gala", "hamster kombat", "melania meme",
    "celestia", "truefi", "dogwifhat", "trump", "pepe",
}


def market_category(code: str, raw_type: Any, name: str = "") -> str:
    """Quotex market group: currencies / crypto / commodities / stocks / indices.

    Symbol/name checks win over the broker's type field, which is not
    reliable for OTC crypto / commodities.
    """
    base = str(code).upper().replace("_OTC", "")
    nm = str(name or "").lower().replace("(otc)", "").strip()
    if base.startswith(_COMMODITY_PREFIXES) or _COMMODITY_NAME.search(nm):
        return "commodities"
    if base in _CRYPTO_BASES or (base.endswith("USD") and base[:-3] in _CRYPTO_BASES) or nm in _CRYPTO_NAMES:
        return "crypto"
    if base in _INDEX_SYMBOLS:
        return "indices"
    if len(base) == 6 and base[:3] in _FIAT and base[3:] in _FIAT:
        return "currencies"
    if isinstance(raw_type, str) and raw_type.lower() in _CATEGORY_ALIASES:
        return _CATEGORY_ALIASES[raw_type.lower()]
    return "stocks"


def open_markets(instruments: Any) -> list[dict[str, Any]]:
    """Open markets with a payout, from pyquotex's raw instrument rows."""
    out: list[dict[str, Any]] = []
    for i in instruments or []:
        try:
            code = i[1]
            name = str(i[2]).replace("\n", "")
            is_open = bool(i[14])
            payout = i[-9] if isinstance(i[-9], (int, float)) else i[5]
            raw_type = i[3]
        except (IndexError, TypeError):
            continue
        if not code or not is_open or not isinstance(payout, (int, float)) or payout <= 0:
            continue
        out.append({
            "symbol": code,
            "name": name,
            "payout": int(payout),
            "is_open": True,
            "type": "otc" if str(code).endswith("_otc") else "real",
            "category": market_category(code, raw_type, name),
        })
    out.sort(key=lambda m: (-m["payout"], m["symbol"]))
    return out


class LiveCollector:
    def __init__(
        self,
        session: Any,
        store: Any,
        periods: list[int],
        align_s: int,
        on_candle: KeyCb,
        on_history: KeyCb,
        on_markets: Callable[[], Awaitable[None]],
        backfill: bool = True,
        idle_limit_s: float = 90,
        resub_s: float = 300,
        refresh_s: float = 60,
    ) -> None:
        self.session = session
        self.store = store
        self.periods = periods
        self.align_s = align_s
        self.on_candle = on_candle
        self.on_history = on_history
        self.on_markets = on_markets
        self.backfill_enabled = backfill
        self.idle_limit_s = idle_limit_s
        self.resub_s = resub_s
        self.refresh_s = refresh_s
        self.markets: dict[str, dict[str, Any]] = {}
        self.started_at: float | None = None
        self.last_tick = time.time()
        self._forming: dict[Key, dict[str, Any]] = {}
        self._backfilled: set[Key] = set()
        self._backfill_task: asyncio.Task | None = None
        # One market at a time on the socket: pyquotex only keeps the
        # history/list/v2 reply of ``api.current_asset``.
        self._ws_lock = asyncio.Lock()
        self._last_index = 0

    # ------------------------------------------------------------ public
    @property
    def started(self) -> bool:
        return self.started_at is not None and time.time() >= self.started_at

    def is_live(self, key: Key) -> bool:
        return self.started and key[0] in self.markets and key[1] in self.periods

    def assets(self) -> list[dict[str, Any]]:
        return list(self.markets.values()) if self.started else []

    # ------------------------------------------------------------ helpers
    @property
    def _client(self) -> Any:
        return getattr(self.session, "client", None)

    async def _refresh_markets(self) -> set[str]:
        client = self._client
        instruments = await _maybe_await(client.get_instruments())
        fresh = {m["symbol"]: m for m in open_markets(instruments)}
        if not fresh and self.markets:
            # pyquotex occasionally overwrites api.instruments with an
            # unrelated frame — keep the last good list.
            return set()
        added = set(fresh) - set(self.markets)
        for code in set(self.markets) - set(fresh):
            for p in self.periods:
                self._forming.pop((code, p), None)
        self.markets = fresh
        return added

    async def _subscribe(self, codes: list[str]) -> None:
        client = self._client
        async with self._ws_lock:
            for code in codes:
                try:
                    await _maybe_await(client.start_candles_stream(code, 60))
                except Exception as exc:  # noqa: BLE001
                    log.debug("subscribe %s failed: %s", code, exc)
                await asyncio.sleep(0.04)

    def _next_boundary(self) -> float:
        now = time.time()
        if self.align_s <= 0:
            return now
        return float((int(now) // self.align_s + 1) * self.align_s)

    def _apply_tick(self, code: str, ts: float, price: float, out: dict) -> None:
        for p in self.periods:
            key = (code, p)
            b = (int(ts) // p) * p
            cur = self._forming.get(key)
            if cur is not None and b < cur["time"]:
                continue
            if cur is None or b > cur["time"]:
                if cur is not None:
                    out[(key, cur["time"])] = dict(cur)
                    # Forward-fill silent buckets with flat candles.
                    gap_t = cur["time"] + p
                    while gap_t < b and b - gap_t <= MAX_CANDLES * p:
                        c = cur["close"]
                        flat = {"time": gap_t, "open": c, "high": c, "low": c, "close": c, "volume": 0.0}
                        out[(key, gap_t)] = flat
                        gap_t += p
                cur = {"time": b, "open": price, "high": price, "low": price, "close": price, "volume": 0.0}
                self._forming[key] = cur
            else:
                cur["high"] = max(cur["high"], price)
                cur["low"] = min(cur["low"], price)
                cur["close"] = price
            cur["volume"] += 1
            out[(key, b)] = dict(cur)

    def _drain(self, discard: bool) -> dict:
        """Pop every buffered tick; returns {(key, time): candle} in time order."""
        api = getattr(self._client, "api", None)
        out: dict = {}
        if api is None:
            return out
        buffers = getattr(api, "realtime_price", None) or {}
        for code in list(buffers.keys()):
            buf = buffers.get(code)
            if not buf:
                continue
            n = len(buf)
            items = buf[:n]
            del buf[:n]
            self.last_tick = time.time()
            if discard or code not in self.markets:
                continue
            for t in items:
                try:
                    ts = float(t["time"])
                    price = float(t["price"])
                except (KeyError, TypeError, ValueError):
                    continue
                if self.started_at is None or ts < self.started_at:
                    continue
                self._apply_tick(code, ts, price, out)
        return out

    async def _emit(self, changed: dict) -> None:
        for (key, _t), candle in changed.items():
            self.store.push(key, candle, key[1])
            await self.on_candle(key, candle)

    def _v2_candles(self, api: Any, code: str, p: int, ticks: Any) -> list[dict[str, Any]] | None:
        """pyquotex prepare_candles (crt-chk): tick-built candles + broker v2 candles.

        Returns None when the v2 reply belongs to another timeframe.
        """
        v2 = (api.candle_v2_data.get(code) or {}).get("candles") or []
        # Quotex sends v2 candles newest-first, so compare on time-sorted rows.
        v2 = sorted(v2, key=lambda c: float(c.get("time", 0) or 0))
        if v2 and _spacing(v2) not in (0, p):
            return None
        book: dict[int, dict[str, Any]] = {}
        try:
            for c in calculate_candles(ticks, p):
                book[int(c["time"])] = c
        except Exception:  # noqa: BLE001
            pass
        # Broker OHLC wins over tick-built buckets (running candle is cut later).
        for c in v2:
            book[(int(c["time"]) // p) * p] = c
        # history/load reply (if the broker sent one) only fills older gaps.
        for c in self._history_load_reply(api, p):
            book.setdefault(int(c["time"]), c)
        out = []
        for t in sorted(book):
            c = book[t]
            try:
                o, h, lo, cl = (float(c[k]) for k in ("open", "high", "low", "close"))
            except (KeyError, TypeError, ValueError):
                continue
            out.append({"time": t, "open": o, "high": max(h, o, cl), "low": min(lo, o, cl),
                        "close": cl, "volume": 0.0})
        return out

    def _history_load_reply(self, api: Any, p: int) -> list[dict[str, Any]]:
        store = getattr(api, "history_load_data", None) or {}
        msg = store.pop(self._last_index, None)
        rows = (msg or {}).get("data") or (msg or {}).get("candles") or []
        out = []
        for r in rows if isinstance(rows, list) else []:
            if isinstance(r, (list, tuple)) and len(r) >= 5:
                r = {"time": r[0], "open": r[1], "close": r[2], "high": r[3], "low": r[4]}
            if isinstance(r, dict) and r.get("open") is not None:
                out.append(dict(r, time=(int(float(r["time"])) // p) * p))
        return out

    async def _backfill_one(self, key: Key) -> bool:
        """crt-chk formation: subscribe this asset, send history/load, read its history/list/v2."""
        code, p = key
        client = self._client
        api = getattr(client, "api", None)
        if code not in self.markets:
            return True
        if api is None:
            return False
        hist: list[dict[str, Any]] = []
        async with self._ws_lock:
            try:
                api.candles.candles_data = None
                await _maybe_await(client.start_candles_stream(code, p))
                self._last_index = max(int(time.time() * 100), self._last_index + 1)
                end = int(self.started_at or time.time())
                await _maybe_await(api.get_candles(code, self._last_index, end, HISTORY_COUNT * p + p, p))
                deadline = time.time() + V2_TIMEOUT_S
                while time.time() < deadline:
                    ticks = api.candles.candles_data
                    if ticks is not None:
                        got = self._v2_candles(api, code, p, ticks)
                        if got is not None:
                            hist = got
                            break
                        api.candles.candles_data = None  # late reply for another timeframe
                    await asyncio.sleep(0.1)
            except Exception as exc:  # noqa: BLE001
                log.warning("backfill %s/%s failed: %r", code, p, exc)
        if not hist:
            log.warning("backfill %s/%ss: no history/list/v2 reply in %.0fs", code, p, V2_TIMEOUT_S)
        live = self.store.get(key)
        cut = int(self.started_at or time.time())
        if live:
            cut = min(cut, int(live[0]["time"]))
        older = [c for c in hist if c["time"] < cut][-HISTORY_COUNT:]
        if not older:
            return False
        self._backfilled.add(key)
        log.info("history %s/%ss: %d candles (history/list/v2)", code, p, len(older))
        self.store.set_history(key, (older + live)[-MAX_CANDLES * 2:], p)
        await self.on_history(key, self.store.get(key))
        return True

    async def _backfill(self) -> None:
        """Walk every market one by one (crt-chk style), retrying failures."""
        while self.started_at is not None and time.time() < self.started_at:
            await asyncio.sleep(0.5)
        for attempt in range(1, BACKFILL_ATTEMPTS + 1):
            pending = [
                (code, p) for code in list(self.markets) for p in self.periods
                if (code, p) not in self._backfilled
            ]
            if not pending:
                break
            failed = []
            for k in pending:
                if not await self._backfill_one(k):
                    failed.append(k)
                await self._emit(self._drain(discard=False))
            log.info(
                "backfill round %d: %d/%d ok, %d failed",
                attempt, len(pending) - len(failed), len(pending), len(failed),
            )
            if failed:
                await asyncio.sleep(2)
        # Put every market back on the 60s stream after the per-period requests.
        await self._subscribe(list(self.markets))
        total = len(self.markets) * len(self.periods)
        log.info("backfill finished: %d/%d streams have history", len(self._backfilled), total)

    def _start_backfill(self) -> None:
        if not self.backfill_enabled:
            return
        if self._backfill_task is None or self._backfill_task.done():
            self._backfill_task = asyncio.create_task(self._backfill())

    # ------------------------------------------------------------ main loop
    async def run(self) -> None:
        while True:
            try:
                await self._refresh_markets()
                await self._subscribe(list(self.markets))
                last_sub = last_refresh = time.time()
                first_start = self.started_at is None
                if first_start:
                    self.started_at = self._next_boundary()
                log.info(
                    "%d markets subscribed — collecting from %s",
                    len(self.markets),
                    time.strftime("%H:%M:%S", time.localtime(self.started_at)),
                )
                while time.time() < self.started_at:
                    self._drain(discard=True)
                    await asyncio.sleep(0.2)
                await self.on_markets()
                self._start_backfill()
                self.last_tick = time.time()
                while True:
                    await self._emit(self._drain(discard=False))
                    now = time.time()
                    if now - self.last_tick > self.idle_limit_s:
                        raise ConnectionError(f"no ticks for {self.idle_limit_s:.0f}s")
                    if now - last_refresh > self.refresh_s:
                        last_refresh = now
                        before = set(self.markets)
                        added = await self._refresh_markets()
                        if added:
                            await self._subscribe(sorted(added))
                        # Also retries streams whose history failed earlier.
                        self._start_backfill()
                        if set(self.markets) != before:
                            await self.on_markets()
                    if now - last_sub > self.resub_s:
                        await self._subscribe(list(self.markets))
                        last_sub = time.time()
                    await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("collector error: %s — reconnecting in 10s", exc)
                await asyncio.sleep(10)
                try:
                    await self.session.reconnect()
                except Exception as rexc:  # noqa: BLE001
                    log.warning("reconnect failed: %s", rexc)
