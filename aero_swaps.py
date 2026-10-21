#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Aerodrome Swaps Tracker (Base) — USDC/LMTS Slipstream pool

Берёт все Swap-ивенты пула за заданное окно времени и показывает,
кто покупал / продавал LMTS, сколько и по какой цене.

ЗАПУСК
  python aero_swaps.py --start "2026-10-20 18:00" --end "2026-10-20 21:00" --tz Asia/Tbilisi
  python aero_swaps.py --last-hours 3                     # последние 3 часа
  (или через env: SWAP_START / SWAP_END / SWAP_TZ — так удобно из GitHub Actions)

  Окно = [start, end): начало включительно, конец не включительно.
  --end пустой → до текущего момента.

КАК ОПРЕДЕЛЯЕТСЯ ТРЕЙДЕР
  По умолчанию трейдер = tx.from (кошелёк, подписавший транзакцию).
  Если recipient или sender свапа есть в лейблах (например, наш бот-контракт),
  берётся он. Порядок проверки: tx.from → recipient → sender.

НАПРАВЛЕНИЕ (как в Uniswap V3 / Slipstream, с точки зрения пула)
  amount > 0 — токен пришёл в пул, amount < 0 — ушёл из пула.
  LMTS ушёл из пула → трейдер КУПИЛ LMTS.
  LMTS пришёл в пул → трейдер ПРОДАЛ LMTS.
  Цена исполнения = |USDC| / |LMTS| (с учётом комиссии пула).

ЛЕЙБЛЫ
  Лист "labels" (имя меняется через LABELS_SHEET). Колонки ищутся по заголовку:
    address  (address / wallet / кошелек / адрес)
    label    (label / лейбл / метка / name)
    group    (group / type / группа / тип) — необязательная
  group: ours / retail / retail_bundle / любое своё значение.
  Если group пустая — определяется автоматически:
    кошелёк из OUR_WALLETS → ours
    лейбл содержит "ритейл связка" → retail_bundle
    лейбл из списка RETAIL_LABELS → retail
    другой лейбл → labeled_other
    нет лейбла → unlabeled

ВЫГРУЗКА
  Google Sheets (перезаписываются при каждом запуске):
    swaps_txs        — все свапы в хронологическом порядке
    swaps_by_wallet  — итоги по каждому кошельку
    swaps_summary    — параметры окна + итоги по группам (наши / ритейл / связка / ...)
  CSV: out/<pool>/swaps/
"""

import argparse
import csv
import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext
from zoneinfo import ZoneInfo

from web3 import Web3
from eth_abi import decode as abi_decode

getcontext().prec = 80

# =========================================================
# CONFIG
# =========================================================
CHAIN_ID = 8453
POOL = Web3.to_checksum_address("0xbe4C36B9542610dF83Ca690C8b5BC53BbbC5d542")
POOL_CREATED_BLOCK = 43139450  # нижняя граница для поиска блоков

USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
LMTS = "0x9eadbe35f3ee3bf3e28180070c429298a1b02f93"

# Фолбэк, если лейбла нет в листе. Ключи — строго lowercase.
OUR_WALLETS = {
    "0x5f0aea872b7d6dbcc181338f80048b130e443e3b": "our_pool_wallet",
    "0x44a3f0354f4c10eb9cd93e522b5e3210d126f054": "team_mm2",
    "0x8ebe6ad4f5bd2f471e0eb828c5918996dd2cd756": "buyb_cnbase",
    "0x42f4b7ec748e1c858471fb9fb6e147fe66fdba5e": "our_new_5e",
}

# Лейблы, которые считаются ритейлом (сравнение без учёта регистра)
RETAIL_LABELS = {s.lower() for s in [
    "Retail",
    "claimed IU",
    "claimed Treasury",
    "f46",
    "Retail Get Drop 27 May",
    "Retail MEXC",
    "retail: 4f6 contract send",
    "whale",
    "top PnL",
    "Wallchain Sent",
    "Wallchain Sent claimed IU",
]}
RETAIL_BUNDLE_MARKER = "ритейл связка"

GROUP_ALIASES = {
    "ours": "ours", "our": "ours", "наш": "ours", "наши": "ours", "team": "ours",
    "retail": "retail", "ритейл": "retail",
    "retail_bundle": "retail_bundle", "bundle": "retail_bundle",
    "ритейл связка": "retail_bundle", "связка": "retail_bundle",
}
GROUP_ORDER = ["ours", "retail", "retail_bundle", "labeled_other", "unlabeled"]

SPREADSHEET_ID = os.environ.get("SWAPS_SPREADSHEET_ID") or os.environ.get("AERO_SPREADSHEET_ID", "")
LABELS_SPREADSHEET_ID = os.environ.get("LABELS_SPREADSHEET_ID") or SPREADSHEET_ID
LABELS_SHEET = os.environ.get("LABELS_SHEET", "labels")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS", "")

SHEET_TXS = "swaps_txs"
SHEET_WALLETS = "swaps_by_wallet"
SHEET_SUMMARY = "swaps_summary"

POOL_ID = POOL.lower()
STATE_DIR = os.path.join("state", POOL_ID, "swaps")
TX_CACHE_FILE = os.path.join(STATE_DIR, "tx_from_cache.json")
OUT_DIR = os.path.join("out", POOL_ID, "swaps")

CHUNK_SIZE = 2000
SLEEP_BETWEEN_CHUNKS = 0.1
RETRIES = 5
RETRY_SLEEP = 2.0
TIMEOUT = 30
CONFIRMATIONS_BUFFER = 5

BUY = "КУПИЛ"
SELL = "ПРОДАЛ"

D0 = Decimal(0)
Q96 = Decimal(2) ** 96


def _topic(sig: str) -> str:
    h = Web3.keccak(text=sig).hex()
    return h if h.startswith("0x") else "0x" + h


# Slipstream CLPool Swap — та же сигнатура, что у Uniswap V3
SWAP_TOPIC = _topic("Swap(address,address,int256,int256,uint160,uint128,int24)")

w3 = None  # инициализируется в main()

# =========================================================
# ABIs
# =========================================================
POOL_ABI = [
    {"name": "token0", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "token1", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
]
ERC20_ABI = [
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint8"}]},
    {"name": "symbol", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "string"}]},
]


# =========================================================
# HELPERS
# =========================================================
def init_w3():
    global w3
    rpc = os.environ.get("BASE_RPC_URL")
    if not rpc:
        raise SystemExit("BASE_RPC_URL не задан")
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": TIMEOUT}))
    if not w3.is_connected():
        raise RuntimeError("RPC not connected — check BASE_RPC_URL secret")


def with_retry(fn, what: str):
    last = None
    for attempt in range(RETRIES):
        try:
            return fn()
        except Exception as e:
            last = e
            wait = RETRY_SLEEP * (attempt + 1)
            print(f"  [warn] {what} failed ({e}), retry in {wait:.1f}s", flush=True)
            time.sleep(wait)
    raise last


def _to_bytes(data) -> bytes:
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if hasattr(data, "hex"):
        return bytes(data)
    s = str(data)
    if s[:2].lower() == "0x":
        s = s[2:]
    return bytes.fromhex(s)


def hex0x(data) -> str:
    h = _to_bytes(data).hex().lower()
    return "0x" + h


def topic_to_address(topic) -> str:
    return "0x" + _to_bytes(topic).hex()[-40:].lower()


def human(raw: int, decimals: int) -> Decimal:
    return Decimal(raw) / (Decimal(10) ** decimals)


def fmt(x, places: int = 6) -> str:
    if x is None:
        return ""
    if isinstance(x, Decimal):
        return str(x.quantize(Decimal(10) ** -places))
    return str(x)


def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, sort_keys=True)


# =========================================================
# TIME → BLOCKS
# =========================================================
_ts_cache = {}


def block_ts(n: int) -> int:
    if n not in _ts_cache:
        _ts_cache[n] = int(with_retry(lambda: w3.eth.get_block(n)["timestamp"], f"get_block {n}"))
    return _ts_cache[n]


def first_block_at_or_after(ts: int, lo: int, hi: int) -> int:
    """Наименьший блок в [lo, hi] с timestamp >= ts; hi+1, если такого нет."""
    if block_ts(hi) < ts:
        return hi + 1
    while lo < hi:
        mid = (lo + hi) // 2
        if block_ts(mid) >= ts:
            hi = mid
        else:
            lo = mid + 1
    return lo


def resolve_block_window(start_ts: int, end_ts: int):
    latest = int(w3.eth.block_number)
    safe = latest - CONFIRMATIONS_BUFFER
    start_block = first_block_at_or_after(start_ts, POOL_CREATED_BLOCK, safe)
    end_block = min(first_block_at_or_after(end_ts, POOL_CREATED_BLOCK, safe) - 1, safe)
    return start_block, end_block, safe


def parse_local(s: str, tz: ZoneInfo) -> datetime:
    s = s.strip().replace("T", " ")
    for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, f).replace(tzinfo=tz)
        except ValueError:
            pass
    raise SystemExit(f"Не могу разобрать дату '{s}'. Формат: 2026-10-20 18:00")


# =========================================================
# RPC: LOGS / TX
# =========================================================
RANGE_ERRORS = ("more than", "block range", "limit exceeded", "too many", "response size",
                "range is too large", "range should", "up to a", "10 block", "exceed")


class RangeTooWide(Exception):
    """Нода отказалась отдавать логи за такой диапазон — надо дробить."""


def _error_body(e: Exception) -> str:
    resp = getattr(e, "response", None)
    text = getattr(resp, "text", "") or ""
    return text.strip().replace("\n", " ")[:300]


def _get_logs_once(a: int, b: int) -> list:
    params = {"fromBlock": hex(a), "toBlock": hex(b), "address": POOL, "topics": [SWAP_TOPIC]}
    last = None
    for attempt in range(RETRIES):
        try:
            return w3.eth.get_logs(params)
        except Exception as e:
            body = _error_body(e)
            status = getattr(getattr(e, "response", None), "status_code", None)
            msg = (str(e) + " " + body).lower()
            # 400/413 и ошибки про диапазон — детерминированные, ретраить бессмысленно
            if status in (400, 413) or any(s in msg for s in RANGE_ERRORS):
                raise RangeTooWide(body or str(e)) from e
            last = e
            wait = RETRY_SLEEP * (attempt + 1)
            print(f"  [warn] get_logs {a}->{b} failed ({e} {body}), retry in {wait:.1f}s", flush=True)
            time.sleep(wait)
    raise last


def fetch_swap_logs(start_block: int, end_block: int) -> list:
    """Тянет логи окнами, на отказ ноды уменьшает окно вдвое и запоминает рабочий размер."""
    logs = []
    span = CHUNK_SIZE
    cur = start_block
    total = end_block - start_block + 1
    reported = set()
    calls = 0

    while cur <= end_block:
        b = min(cur + span - 1, end_block)
        try:
            chunk = _get_logs_once(cur, b)
        except RangeTooWide as e:
            if span == 1:
                raise RuntimeError(f"Нода отказывает даже для одного блока {cur}: {e}")
            reason = str(e)
            if reason not in reported:
                print(f"  [rpc] нода отказала на {span} блоков: {reason}", flush=True)
                reported.add(reason)
            m = re.search(r"up to a ([\d,]+)[ -]block", reason, re.I)
            hinted = int(m.group(1).replace(",", "")) if m else 0
            span = hinted if 0 < hinted < span else max(1, span // 2)
            print(f"  [rpc] уменьшаю окно до {span} блоков", flush=True)
            continue

        logs.extend(chunk)
        calls += 1
        cur = b + 1
        if calls % 25 == 0 or cur > end_block:
            done = cur - start_block
            print(f"  {done:,}/{total:,} блоков ({done / total * 100:.0f}%) | окно {span} | swaps: {len(logs)}",
                  flush=True)
        time.sleep(SLEEP_BETWEEN_CHUNKS)

    return logs


def get_tx_from(txh: str, cache: dict) -> str:
    if txh not in cache:
        tx = with_retry(lambda: w3.eth.get_transaction(txh), f"get_transaction {txh[:12]}")
        cache[txh] = str(tx["from"]).lower()
    return cache[txh]


# =========================================================
# POOL INFO
# =========================================================
def load_pool_info() -> dict:
    pool_c = w3.eth.contract(address=POOL, abi=POOL_ABI)
    t0 = str(with_retry(lambda: pool_c.functions.token0().call(), "token0")).lower()
    t1 = str(with_retry(lambda: pool_c.functions.token1().call(), "token1")).lower()

    if {t0, t1} != {USDC, LMTS}:
        raise RuntimeError(f"Неожиданные токены пула: {t0} / {t1}")

    def dec(addr):
        c = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=ERC20_ABI)
        return int(with_retry(lambda: c.functions.decimals().call(), f"decimals {addr[:8]}"))

    return {
        "usdc_is_token0": t0 == USDC,
        "dec0": dec(t0),
        "dec1": dec(t1),
        "dec_usdc": dec(USDC),
        "dec_lmts": dec(LMTS),
    }


def sqrt_to_usd_per_lmts(sqrt_price_x96: int, pool: dict) -> Decimal:
    ratio = (Decimal(sqrt_price_x96) / Q96) ** 2  # token1 per token0, raw
    p1_per_0 = ratio * (Decimal(10) ** (pool["dec0"] - pool["dec1"]))
    if pool["usdc_is_token0"]:
        return Decimal(1) / p1_per_0 if p1_per_0 else D0  # LMTS per USDC → USD per LMTS
    return p1_per_0


# =========================================================
# LABELS
# =========================================================
def classify(addr: str, label: str, group_raw: str) -> str:
    g = (group_raw or "").strip().lower()
    if g:
        return GROUP_ALIASES.get(g, g)
    if addr in OUR_WALLETS:
        return "ours"
    lab = (label or "").strip().lower()
    if RETAIL_BUNDLE_MARKER in lab:
        return "retail_bundle"
    if lab in RETAIL_LABELS:
        return "retail"
    if lab:
        return "labeled_other"
    return "unlabeled"


def load_labels(gc) -> dict:
    labels = {a: {"label": n, "group": "ours"} for a, n in OUR_WALLETS.items()}

    if gc is None or not LABELS_SPREADSHEET_ID:
        print("  [labels] таблица не подключена — только OUR_WALLETS")
        return labels

    try:
        values = gc.open_by_key(LABELS_SPREADSHEET_ID).worksheet(LABELS_SHEET).get_all_values()
    except Exception as e:
        print(f"  [labels] не удалось прочитать лист '{LABELS_SHEET}': {e} — только OUR_WALLETS")
        return labels

    if not values:
        return labels

    header = [h.strip().lower() for h in values[0]]

    def col(*names):
        for n in names:
            if n in header:
                return header.index(n)
        return None

    ia = col("address", "wallet", "кошелек", "кошелёк", "адрес")
    il = col("label", "лейбл", "метка", "name")
    ig = col("group", "type", "группа", "тип", "category")

    if ia is None:
        print(f"  [labels] в листе '{LABELS_SHEET}' нет колонки address — только OUR_WALLETS")
        return labels

    def cell(row, i):
        return row[i].strip() if i is not None and i < len(row) else ""

    n = 0
    for row in values[1:]:
        addr = cell(row, ia).lower()
        if not (addr.startswith("0x") and len(addr) == 42):
            continue
        label = cell(row, il) or labels.get(addr, {}).get("label", "")
        labels[addr] = {"label": label, "group": classify(addr, label, cell(row, ig))}
        n += 1

    print(f"  [labels] загружено {n} адресов из '{LABELS_SHEET}' (+ OUR_WALLETS)")
    return labels


def resolve_trader(tx_from: str, recipient: str, sender: str, labels: dict):
    for cand in (tx_from, recipient, sender):
        if cand in labels:
            return cand, labels[cand]
    return tx_from, {"label": "", "group": "unlabeled"}


# =========================================================
# COLLECT SWAPS
# =========================================================
def collect_swaps(start_block: int, end_block: int, pool: dict, labels: dict, tz: ZoneInfo) -> list:
    logs = fetch_swap_logs(start_block, end_block)

    logs.sort(key=lambda lg: (int(lg["blockNumber"]), int(lg["logIndex"])))
    print(f"  swap logs: {len(logs)} — подтягиваю tx.from и время...", flush=True)

    tx_cache = load_json(TX_CACHE_FILE, {})
    rows = []

    for i, lg in enumerate(logs, 1):
        if len(lg["topics"]) < 3:
            continue
        txh = hex0x(lg["transactionHash"])
        blk = int(lg["blockNumber"])
        idx = int(lg["logIndex"])
        sender = topic_to_address(lg["topics"][1])
        recipient = topic_to_address(lg["topics"][2])
        a0, a1, sqrtp, _liq, _tick = abi_decode(
            ["int256", "int256", "uint160", "uint128", "int24"], _to_bytes(lg["data"])
        )

        usdc_raw, lmts_raw = (a0, a1) if pool["usdc_is_token0"] else (a1, a0)
        if lmts_raw == 0 or usdc_raw == 0:
            continue

        side = BUY if lmts_raw < 0 else SELL
        lmts_amt = human(abs(lmts_raw), pool["dec_lmts"])
        usdc_amt = human(abs(usdc_raw), pool["dec_usdc"])

        tx_from = get_tx_from(txh, tx_cache)
        trader, info = resolve_trader(tx_from, recipient, sender, labels)

        dt_utc = datetime.fromtimestamp(block_ts(blk), timezone.utc)

        rows.append({
            "time_local": dt_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S"),
            "time_utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "side": side,
            "trader": trader,
            "label": info["label"],
            "group": info["group"],
            "lmts": lmts_amt,
            "usdc": usdc_amt,
            "price_usd_per_lmts": usdc_amt / lmts_amt,
            "pool_price_after": sqrt_to_usd_per_lmts(sqrtp, pool),
            "block": blk,
            "log_index": idx,
            "tx_hash": txh,
            "tx_from": tx_from,
            "sender": sender,
            "recipient": recipient,
            "basescan": f"https://basescan.org/tx/{txh}",
        })

        if i % 50 == 0:
            save_json(TX_CACHE_FILE, tx_cache)
            print(f"  processed {i}/{len(logs)}", flush=True)

    save_json(TX_CACHE_FILE, tx_cache)
    return rows


# =========================================================
# AGGREGATION
# =========================================================
def new_agg() -> dict:
    return {
        "swaps": 0, "buys": 0, "sells": 0,
        "lmts_bought": D0, "usdc_spent": D0,
        "lmts_sold": D0, "usdc_received": D0,
        "min_price": None, "max_price": None,
        "first": "", "last": "",
    }


def add_to(agg: dict, r: dict):
    agg["swaps"] += 1
    if r["side"] == BUY:
        agg["buys"] += 1
        agg["lmts_bought"] += r["lmts"]
        agg["usdc_spent"] += r["usdc"]
    else:
        agg["sells"] += 1
        agg["lmts_sold"] += r["lmts"]
        agg["usdc_received"] += r["usdc"]
    p = r["price_usd_per_lmts"]
    agg["min_price"] = p if agg["min_price"] is None else min(agg["min_price"], p)
    agg["max_price"] = p if agg["max_price"] is None else max(agg["max_price"], p)
    if not agg["first"]:
        agg["first"] = r["time_local"]
    agg["last"] = r["time_local"]


METRIC_COLS = [
    "swaps",
    "buys", "lmts_bought", "usdc_spent", "avg_buy_price",
    "sells", "lmts_sold", "usdc_received", "avg_sell_price",
    "net_lmts", "net_usdc",
    "volume_usdc", "vwap", "min_price", "max_price",
    "first_trade", "last_trade",
]


def metrics(agg: dict) -> dict:
    vol_lmts = agg["lmts_bought"] + agg["lmts_sold"]
    vol_usdc = agg["usdc_spent"] + agg["usdc_received"]
    return {
        "swaps": agg["swaps"],
        "buys": agg["buys"],
        "lmts_bought": agg["lmts_bought"],
        "usdc_spent": agg["usdc_spent"],
        "avg_buy_price": agg["usdc_spent"] / agg["lmts_bought"] if agg["lmts_bought"] else None,
        "sells": agg["sells"],
        "lmts_sold": agg["lmts_sold"],
        "usdc_received": agg["usdc_received"],
        "avg_sell_price": agg["usdc_received"] / agg["lmts_sold"] if agg["lmts_sold"] else None,
        "net_lmts": agg["lmts_bought"] - agg["lmts_sold"],
        "net_usdc": agg["usdc_received"] - agg["usdc_spent"],
        "volume_usdc": vol_usdc,
        "vwap": vol_usdc / vol_lmts if vol_lmts else None,
        "min_price": agg["min_price"],
        "max_price": agg["max_price"],
        "first_trade": agg["first"],
        "last_trade": agg["last"],
    }


def build_summaries(rows: list, window: dict):
    by_wallet = {}
    groups = defaultdict(new_agg)

    for r in rows:
        key = r["trader"]
        if key not in by_wallet:
            by_wallet[key] = {"label": r["label"], "group": r["group"], "agg": new_agg()}
        add_to(by_wallet[key]["agg"], r)

        add_to(groups[r["group"]], r)
        if r["group"] in ("retail", "retail_bundle"):
            add_to(groups["retail_total"], r)
        if r["group"] != "ours":
            add_to(groups["not_ours_total"], r)
        add_to(groups["ALL"], r)

    # --- по кошелькам: сначала наши, потом по объёму
    wallet_header = ["trader", "label", "group"] + METRIC_COLS
    wallet_rows = []
    for addr, w in by_wallet.items():
        m = metrics(w["agg"])
        wallet_rows.append([addr, w["label"], w["group"]] + [m[c] for c in METRIC_COLS])
    vol_i = wallet_header.index("volume_usdc")
    wallet_rows.sort(key=lambda r: (r[2] != "ours", -r[vol_i]))

    # --- по группам
    extra = sorted(g for g in groups if g not in GROUP_ORDER + ["retail_total", "not_ours_total", "ALL"])
    order = [g for g in GROUP_ORDER if g in groups] + extra + \
            [g for g in ("retail_total", "not_ours_total", "ALL") if g in groups]
    group_header = ["group", "wallets"] + METRIC_COLS
    wallets_per_group = defaultdict(set)
    for r in rows:
        wallets_per_group[r["group"]].add(r["trader"])
        if r["group"] in ("retail", "retail_bundle"):
            wallets_per_group["retail_total"].add(r["trader"])
        if r["group"] != "ours":
            wallets_per_group["not_ours_total"].add(r["trader"])
        wallets_per_group["ALL"].add(r["trader"])
    group_rows = []
    for g in order:
        m = metrics(groups[g])
        group_rows.append([g, len(wallets_per_group[g])] + [m[c] for c in METRIC_COLS])

    # --- параметры окна
    allm = metrics(groups["ALL"]) if rows else {}
    meta = [
        ["pool", str(POOL)],
        ["window_start_local", window["start_local"]],
        ["window_end_local", window["end_local"]],
        ["timezone", window["tz"]],
        ["window_start_utc", window["start_utc"]],
        ["window_end_utc", window["end_utc"]],
        ["start_block", window["start_block"]],
        ["end_block", window["end_block"]],
        ["swaps", len(rows)],
        ["unique_traders", len(by_wallet)],
        ["first_price", rows[0]["price_usd_per_lmts"] if rows else ""],
        ["last_price", rows[-1]["price_usd_per_lmts"] if rows else ""],
        ["pool_price_at_end", rows[-1]["pool_price_after"] if rows else ""],
        ["min_price", allm.get("min_price", "")],
        ["max_price", allm.get("max_price", "")],
        ["vwap", allm.get("vwap", "")],
        ["generated_at_utc", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")],
    ]

    return wallet_header, wallet_rows, group_header, group_rows, meta


TX_COLS = [
    "time_local", "time_utc", "side", "trader", "label", "group",
    "lmts", "usdc", "price_usd_per_lmts", "pool_price_after",
    "block", "log_index", "tx_hash", "tx_from", "sender", "recipient", "basescan",
]


# =========================================================
# CSV
# =========================================================
def write_csv(path: str, header: list, rows: list):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow([fmt(v, 8) if isinstance(v, Decimal) else ("" if v is None else v) for v in r])
    print(f"  → {path} ({len(rows)} rows)")


# =========================================================
# GOOGLE SHEETS
# =========================================================
def open_gspread():
    if not GOOGLE_CREDENTIALS_JSON:
        print("  [sheets] GOOGLE_CREDENTIALS не задан — работаю без таблицы")
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("  [sheets] gspread не установлен — работаю без таблицы")
        return None
    try:
        creds = Credentials.from_service_account_info(
            json.loads(GOOGLE_CREDENTIALS_JSON),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"],
        )
        return gspread.authorize(creds)
    except Exception as e:
        print(f"  [sheets] auth failed: {e}")
        return None


def to_cell(v):
    if isinstance(v, Decimal):
        return float(v)  # число, а не строка — не зависит от локали таблицы
    if v is None:
        return ""
    return v


def write_sheet(sh, title: str, data: list):
    import gspread
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=100, cols=20)
    width = max((len(r) for r in data), default=1)
    ws.clear()
    ws.resize(rows=max(len(data) + 5, 50), cols=max(width, 5))
    ws.update(range_name="A1", values=[[to_cell(c) for c in r] for r in data],
              value_input_option="USER_ENTERED")
    try:
        ws.freeze(rows=1)
    except Exception:
        pass
    print(f"  [sheets] '{title}' updated ({len(data) - 1} rows)")


def export_to_sheets(gc, tx_rows, wallet_header, wallet_rows, group_header, group_rows, meta):
    if gc is None or not SPREADSHEET_ID:
        print("  [sheets] SWAPS_SPREADSHEET_ID / AERO_SPREADSHEET_ID не задан — пропускаю")
        return
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        print(f"  [sheets] open failed: {e}")
        return

    write_sheet(sh, SHEET_TXS, [TX_COLS] + [[r[c] for c in TX_COLS] for r in tx_rows])
    write_sheet(sh, SHEET_WALLETS, [wallet_header] + wallet_rows)
    write_sheet(sh, SHEET_SUMMARY, [["metric", "value"]] + meta + [[]] + [group_header] + group_rows)


# =========================================================
# MAIN
# =========================================================
def parse_args():
    env = lambda k: os.environ.get(k) or None
    p = argparse.ArgumentParser(description="Aerodrome USDC/LMTS swaps report for a time window")
    p.add_argument("--start", default=env("SWAP_START"), help='начало, напр. "2026-10-20 18:00"')
    p.add_argument("--end", default=env("SWAP_END"), help="конец (не включительно); пусто = сейчас")
    p.add_argument("--tz", default=env("SWAP_TZ") or "UTC", help="часовой пояс, напр. Asia/Tbilisi, Europe/Lisbon")
    p.add_argument("--last-hours", type=float, default=float(env("SWAP_LAST_HOURS") or 0),
                   help="вместо --start: последние N часов")
    return p.parse_args()


def main():
    args = parse_args()
    tz = ZoneInfo(args.tz)
    now_local = datetime.now(timezone.utc).astimezone(tz)

    end_local = parse_local(args.end, tz) if args.end else now_local
    if args.start:
        start_local = parse_local(args.start, tz)
    elif args.last_hours:
        start_local = end_local - timedelta(hours=args.last_hours)
    else:
        raise SystemExit("Задай --start (или --last-hours)")
    if end_local <= start_local:
        raise SystemExit("Конец окна должен быть позже начала")

    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)

    init_w3()
    gc = open_gspread()

    print("=" * 60)
    print("LABELS")
    print("=" * 60)
    labels = load_labels(gc)

    print()
    print("=" * 60)
    print("WINDOW")
    print("=" * 60)
    pool = load_pool_info()
    start_ts = int(start_local.timestamp())
    end_ts = int(end_local.timestamp())
    start_block, end_block, safe = resolve_block_window(start_ts, end_ts)

    window = {
        "start_local": start_local.strftime("%Y-%m-%d %H:%M:%S"),
        "end_local": end_local.strftime("%Y-%m-%d %H:%M:%S"),
        "tz": args.tz,
        "start_utc": start_local.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "end_utc": end_local.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "start_block": start_block,
        "end_block": end_block,
    }
    print(f"  {window['start_local']} → {window['end_local']} ({args.tz})")
    print(f"  UTC: {window['start_utc']} → {window['end_utc']}")
    print(f"  blocks: {start_block} → {end_block} (safe head {safe})")

    print()
    print("=" * 60)
    print("COLLECTING SWAPS")
    print("=" * 60)
    if start_block > end_block:
        print("  В окне нет блоков")
        tx_rows = []
    else:
        tx_rows = collect_swaps(start_block, end_block, pool, labels, tz)
    print(f"  swaps in window: {len(tx_rows)}")

    wallet_header, wallet_rows, group_header, group_rows, meta = build_summaries(tx_rows, window)

    print()
    print("=" * 60)
    print("EXPORTS")
    print("=" * 60)
    write_csv(os.path.join(OUT_DIR, "swaps_txs.csv"), TX_COLS, [[r[c] for c in TX_COLS] for r in tx_rows])
    write_csv(os.path.join(OUT_DIR, "swaps_by_wallet.csv"), wallet_header, wallet_rows)
    write_csv(os.path.join(OUT_DIR, "swaps_by_group.csv"), group_header, group_rows)
    write_csv(os.path.join(OUT_DIR, "swaps_meta.csv"), ["metric", "value"], meta)
    export_to_sheets(gc, tx_rows, wallet_header, wallet_rows, group_header, group_rows, meta)

    print()
    print("=== BY GROUP ===")
    gi = {c: i for i, c in enumerate(group_header)}
    for r in group_rows:
        print(f"  {r[0]:<16} buys={r[gi['buys']]:<4} bought={fmt(r[gi['lmts_bought']], 2):>14} LMTS "
              f"@ {fmt(r[gi['avg_buy_price']], 6) or '-':<10} | sells={r[gi['sells']]:<4} "
              f"sold={fmt(r[gi['lmts_sold']], 2):>14} LMTS @ {fmt(r[gi['avg_sell_price']], 6) or '-'}")
    print("\nDONE")


if __name__ == "__main__":
    main()
