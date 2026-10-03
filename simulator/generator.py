"""Deterministic commerce world + event-stream generator.

Design
------
* **Seeded and chunk-stable.** Every random draw comes from ``np.random.default_rng([seed, stream,
  chunk_start])`` with a fixed ``CHUNK_ORDERS``; event ids are ``uuid5`` of a stable business name.
  The same config therefore yields byte-identical events no matter how the stream is consumed.
* **Event-time ordered output.** Orders are laid out on a timeline drawn from the real hour-of-day /
  weekday profile (inverse-CDF, stratified). Each chunk generates the *full* lifecycle of its
  orders; events in the future are carried over and released when the timeline reaches them.
* **Columnar.** Each emitted batch is ``dict[topic, DataFrame]`` sorted by ``event_timestamp``
  (int64 microseconds, UTC) — cheap to COPY into Postgres or serialise to Avro.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd

from contracts import events as ev
from simulator.reference import Reference, load

CHUNK_ORDERS = 25_000
MICROS = 1_000_000
HOUR = 3600 * MICROS
DAY = 24 * HOUR

COUNTRY_CITIES: dict[str, list[tuple[str, str | None]]] = {
    "United Kingdom": [
        ("London", None),
        ("Manchester", None),
        ("Birmingham", None),
        ("Leeds", None),
        ("Glasgow", None),
        ("Bristol", None),
        ("Liverpool", None),
        ("Edinburgh", None),
    ],
    "Germany": [("Berlin", None), ("Munich", None), ("Hamburg", None), ("Cologne", None)],
    "France": [("Paris", None), ("Lyon", None), ("Marseille", None)],
    "Spain": [("Madrid", None), ("Barcelona", None), ("Valencia", None)],
    "Belgium": [("Brussels", None), ("Antwerp", None)],
    "Portugal": [("Lisbon", None), ("Porto", None)],
    "Netherlands": [("Amsterdam", None), ("Rotterdam", None), ("Utrecht", None)],
    "Switzerland": [("Zurich", None), ("Geneva", None)],
    "Sweden": [("Stockholm", None), ("Gothenburg", None)],
    "Italy": [("Rome", None), ("Milan", None), ("Turin", None)],
    "Australia": [("Sydney", None), ("Melbourne", None), ("Brisbane", None)],
    "Finland": [("Helsinki", None), ("Espoo", None)],
    "United States": [
        ("Phoenix", "AZ"),
        ("Tempe", "AZ"),
        ("San Jose", "CA"),
        ("Los Angeles", "CA"),
        ("Seattle", "WA"),
        ("Austin", "TX"),
        ("Chicago", "IL"),
        ("New York", "NY"),
        ("Boston", "MA"),
        ("Denver", "CO"),
    ],
}
EURO = {"Germany", "France", "Spain", "Belgium", "Portugal", "Netherlands", "Italy", "Finland"}
FIRST_NAMES = [
    "Olivia",
    "Liam",
    "Amelia",
    "Noah",
    "Isla",
    "Oliver",
    "Ava",
    "George",
    "Mia",
    "Arthur",
    "Sofia",
    "Leo",
    "Emma",
    "Lucas",
    "Hannah",
    "Mateo",
    "Chloe",
    "Ethan",
    "Priya",
    "Arjun",
    "Yuki",
    "Chen",
    "Fatima",
    "Omar",
    "Ines",
    "Luca",
    "Freya",
    "Hugo",
    "Elena",
    "Jonas",
]
LAST_NAMES = [
    "Smith",
    "Jones",
    "Taylor",
    "Brown",
    "Williams",
    "Wilson",
    "Johnson",
    "Davies",
    "Patel",
    "Garcia",
    "Muller",
    "Schmidt",
    "Martin",
    "Bernard",
    "Rossi",
    "Silva",
    "Jansen",
    "Nilsson",
    "Virtanen",
    "Kumar",
    "Nguyen",
    "Kim",
    "Lopez",
    "Dubois",
    "Fischer",
    "Novak",
    "Khan",
]
CARRIERS = ["ROYAL_MAIL", "DHL", "UPS", "FEDEX", "DPD"]


def currency_for(country: str) -> str:
    if country == "United Kingdom":
        return "GBP"
    return "EUR" if country in EURO else "USD"


@dataclass(frozen=True)
class SimulationConfig:
    seed: int = 42
    orders: int = 10_000
    customers: int | None = None
    stores: int = 30
    products: int = 2_000
    start: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    days: float = 31.0
    customer_move_rate: float = 0.06
    product_change_rate: float = 0.15
    p_cancel_before_payment: float = 0.03
    p_payment_fail: float = 0.08
    p_retry_after_fail: float = 0.6
    p_cancel_after_paid: float = 0.02
    lifecycle_time_scale: float = 1.0  # <1 compresses ship/deliver delays (real-time demos)
    channel_probs: tuple[float, float, float] = (0.55, 0.30, 0.15)
    truncate_at_end: bool = True  # drop lifecycle events that would happen after the window

    @property
    def n_customers(self) -> int:
        return self.customers if self.customers is not None else max(100, self.orders // 4)

    @property
    def start_us(self) -> int:
        return int(self.start.timestamp() * MICROS)

    @property
    def end_us(self) -> int:
        return self.start_us + int(self.days * DAY)

    def to_json(self) -> dict:
        d = asdict(self)
        d["start"] = self.start.isoformat()
        return d


def us_to_datetime(us: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=int(us))


@dataclass
class World:
    """Static entities plus their pre-generated change events."""

    stores: pd.DataFrame
    customers: pd.DataFrame  # final state is not stored here; see customer_events
    customer_signup_us: np.ndarray  # sorted
    customer_country_idx: np.ndarray
    countries: list[str]
    products: pd.DataFrame  # initial state
    product_price_gbp: np.ndarray  # initial unit price (minor) per product index
    product_change_us: np.ndarray  # time of the single price change (int64 max = never)
    product_new_price_gbp: np.ndarray
    product_weights: np.ndarray
    stores_by_country: dict[int, np.ndarray] = field(default_factory=dict)


class EventIds:
    """Deterministic uuid5 ids from stable business names."""

    def __init__(self, seed: int) -> None:
        self.ns = uuid.uuid5(uuid.NAMESPACE_URL, f"https://atlaslake/seed/{seed}")

    def many(self, names: list[str]) -> list[str]:
        ns = self.ns
        return [str(uuid.uuid5(ns, n)) for n in names]


def _largest_remainder(weights: np.ndarray, total: int, minimum: int = 1) -> np.ndarray:
    n = len(weights)
    base = np.full(n, minimum)
    rest = total - base.sum()
    if rest <= 0:
        return base
    raw = weights / weights.sum() * rest
    alloc = np.floor(raw).astype(int)
    remainder = rest - alloc.sum()
    order = np.argsort(-(raw - alloc), kind="stable")
    alloc[order[:remainder]] += 1
    return base + alloc


class CommerceSimulator:
    def __init__(self, cfg: SimulationConfig, reference: Reference | None = None) -> None:
        self.cfg = cfg
        self.ref = reference or load()
        self.ids = EventIds(cfg.seed)
        self.world = self._build_world()

    # ------------------------------------------------------------------ world
    def _build_world(self) -> World:
        cfg, ref = self.cfg, self.ref
        rng = np.random.default_rng([cfg.seed, 0])

        countries = [("United States" if c == "Other" else c) for c in ref.countries[0]]
        country_p = np.asarray(ref.countries[1])
        # merge duplicate "United States" entries
        uniq: dict[str, float] = {}
        for c, p in zip(countries, country_p, strict=True):
            uniq[c] = uniq.get(c, 0.0) + float(p)
        countries = list(uniq)
        country_p = np.array(list(uniq.values()))
        country_p = country_p / country_p.sum()

        # stores: at least one per country, rest by customer weight
        per_country = _largest_remainder(country_p, max(cfg.stores, len(countries)))
        rows, store_country = [], []
        sid = 0
        for ci, count in enumerate(per_country):
            cities = COUNTRY_CITIES.get(countries[ci], COUNTRY_CITIES["United States"])
            for j in range(count):
                sid += 1
                city = cities[j % len(cities)][0]
                rows.append(
                    {
                        "store_id": f"s{sid:04d}",
                        "name": f"AtlasMart {city}{'' if j < len(cities) else f' {j // len(cities) + 1}'}",
                        "city": city,
                        "country": countries[ci],
                        "currency": currency_for(countries[ci]),
                        "opened_at_us": cfg.start_us - 3 * 365 * DAY + int(rng.integers(0, 365)) * DAY,
                    }
                )
                store_country.append(ci)
        stores = pd.DataFrame(rows)
        store_country_arr = np.array(store_country)
        stores_by_country = {ci: np.flatnonzero(store_country_arr == ci) for ci in range(len(countries))}

        # customers: 70% pre-existing, 30% sign up during the window; ids in signup order
        n = cfg.n_customers
        pre = rng.random(n) < 0.7
        signup = np.where(
            pre,
            cfg.start_us - (rng.random(n) * 365 * DAY).astype(np.int64) - 1,
            cfg.start_us + (rng.random(n) * (cfg.end_us - cfg.start_us)).astype(np.int64),
        )
        signup.sort()
        country_idx = rng.choice(len(countries), size=n, p=country_p)
        customers = pd.DataFrame(
            {
                "customer_id": [f"c{i + 1:08d}" for i in range(n)],
                "country_idx": country_idx,
                "signup_us": signup,
            }
        )

        # products from the real catalogue (popularity-weighted)
        cat = ref.products.head(cfg.products).reset_index(drop=True)
        n_p = len(cat)
        products = pd.DataFrame(
            {
                "product_id": [f"p{i + 1:05d}" for i in range(n_p)],
                "sku": cat["stock_code"].astype(str),
                "name": cat["name"],
                "category": cat["category"],
                "unit_price_minor": cat["unit_price_minor"].astype(np.int64),
                "created_us": cfg.start_us - 400 * DAY,
            }
        )
        changes = rng.random(n_p) < cfg.product_change_rate
        change_us = np.where(
            changes,
            cfg.start_us + (rng.random(n_p) * (cfg.end_us - cfg.start_us)).astype(np.int64),
            np.iinfo(np.int64).max,
        )
        new_price = np.maximum(1, np.round(cat["unit_price_minor"].to_numpy() * rng.uniform(0.85, 1.2, n_p)))
        weights = cat["weight"].to_numpy(dtype=float)

        return World(
            stores=stores,
            customers=customers,
            customer_signup_us=signup,
            customer_country_idx=country_idx,
            countries=countries,
            products=products,
            product_price_gbp=products["unit_price_minor"].to_numpy(),
            product_change_us=change_us,
            product_new_price_gbp=new_price.astype(np.int64),
            product_weights=weights / weights.sum(),
            stores_by_country=stores_by_country,
        )

    # --------------------------------------------------------- entity events
    def customer_events(self) -> pd.DataFrame:
        """CUSTOMER_CREATED for everyone + CUSTOMER_UPDATED (city moves) inside the window."""
        cfg, w = self.cfg, self.world
        rng = np.random.default_rng([cfg.seed, 3])
        n = len(w.customers)
        first = rng.choice(FIRST_NAMES, n)
        last = rng.choice(LAST_NAMES, n)
        segment = rng.choice(ev.CUSTOMER_SEGMENTS, n, p=[0.85, 0.12, 0.03])
        phone = [f"+1-555-{x:07d}" for x in rng.integers(0, 10_000_000, n)]
        countries = [w.countries[i] for i in w.customer_country_idx]
        city_choice = rng.random(n)
        cities, states = [], []
        for c, u in zip(countries, city_choice, strict=True):
            options = COUNTRY_CITIES.get(c, COUNTRY_CITIES["United States"])
            city, state = options[int(u * len(options))]
            cities.append(city)
            states.append(state)
        ids = w.customers["customer_id"].tolist()
        emails = [
            f"{f.lower()}.{lname.lower()}.{cid[1:]}@example.com"
            for f, lname, cid in zip(first, last, ids, strict=True)
        ]
        created = pd.DataFrame(
            {
                "event_type": "CUSTOMER_CREATED",
                "event_timestamp": w.customer_signup_us,
                "customer_id": ids,
                "email": emails,
                "phone": phone,
                "first_name": first,
                "last_name": last,
                "city": cities,
                "state": states,
                "country": countries,
                "segment": segment,
                "signup_timestamp": w.customer_signup_us,
            }
        )
        movers = np.flatnonzero(rng.random(n) < cfg.customer_move_rate)
        moved = created.iloc[movers].copy()
        lo = np.maximum(w.customer_signup_us[movers] + HOUR, cfg.start_us)
        span = np.maximum(cfg.end_us - lo, 1)
        moved["event_timestamp"] = lo + (rng.random(len(movers)) * span).astype(np.int64)
        moved = moved[moved["event_timestamp"] < cfg.end_us]
        new_cities, new_states = [], []
        for c, old, u in zip(moved["country"], moved["city"], rng.random(len(moved)), strict=True):
            options = [o for o in COUNTRY_CITIES.get(c, COUNTRY_CITIES["United States"]) if o[0] != old]
            city, state = options[int(u * len(options))]
            new_cities.append(city)
            new_states.append(state)
        moved["city"] = new_cities
        moved["state"] = new_states
        moved["event_type"] = "CUSTOMER_UPDATED"
        out = pd.concat([created, moved], ignore_index=True)
        names = [
            f"customer:{cid}:{et}:{ts}"
            for cid, et, ts in zip(out["customer_id"], out["event_type"], out["event_timestamp"], strict=True)
        ]
        out.insert(0, "event_id", self.ids.many(names))
        return out.sort_values(["event_timestamp", "event_id"], kind="stable").reset_index(drop=True)

    def product_events(self) -> pd.DataFrame:
        w = self.world
        p = w.products
        created = pd.DataFrame(
            {
                "event_type": "PRODUCT_CREATED",
                "event_timestamp": p["created_us"].to_numpy(),
                "product_id": p["product_id"],
                "sku": p["sku"],
                "name": p["name"],
                "category": p["category"],
                "unit_price_minor": p["unit_price_minor"].to_numpy(),
                "currency": "GBP",
                "active": True,
            }
        )
        changed = np.flatnonzero(w.product_change_us < np.iinfo(np.int64).max)
        upd = created.iloc[changed].copy()
        upd["event_type"] = "PRODUCT_UPDATED"
        upd["event_timestamp"] = w.product_change_us[changed]
        upd["unit_price_minor"] = w.product_new_price_gbp[changed]
        out = pd.concat([created, upd], ignore_index=True)
        names = [
            f"product:{pid}:{et}:{ts}"
            for pid, et, ts in zip(out["product_id"], out["event_type"], out["event_timestamp"], strict=True)
        ]
        out.insert(0, "event_id", self.ids.many(names))
        return out.sort_values(["event_timestamp", "event_id"], kind="stable").reset_index(drop=True)

    # ------------------------------------------------------------ timeline
    def order_times(self) -> np.ndarray:
        """Stratified inverse-CDF sampling of order creation times from the real hourly profile."""
        cfg, ref = self.cfg, self.ref
        rng = np.random.default_rng([cfg.seed, 1])
        hours = int(np.ceil(cfg.days * 24))
        hour_starts = cfg.start_us + np.arange(hours, dtype=np.int64) * HOUR
        dts = pd.to_datetime(hour_starts, unit="us", utc=True)
        weights = ref.hour_of_day[dts.hour.to_numpy()] * ref.weekday[dts.weekday.to_numpy()]
        weights[-1] *= (cfg.days * 24) - (hours - 1)  # partial last hour
        cdf = np.concatenate([[0.0], np.cumsum(weights)])
        cdf /= cdf[-1]
        q = (np.arange(cfg.orders) + rng.random(cfg.orders)) / cfg.orders
        bucket = np.clip(np.searchsorted(cdf, q, side="right") - 1, 0, hours - 1)
        frac = (q - cdf[bucket]) / np.maximum(cdf[bucket + 1] - cdf[bucket], 1e-12)
        times = hour_starts[bucket] + (frac * HOUR).astype(np.int64)
        return np.minimum(np.maximum.accumulate(times), cfg.end_us - 1)

    # ------------------------------------------------------- order lifecycle
    def price_at(self, product_idx: np.ndarray, t_us: np.ndarray) -> np.ndarray:
        w = self.world
        return np.where(
            t_us >= w.product_change_us[product_idx],
            w.product_new_price_gbp[product_idx],
            w.product_price_gbp[product_idx],
        )

    def generate_orders(
        self, first_index: int, created_us: np.ndarray, stream: int = 2
    ) -> dict[str, pd.DataFrame]:
        """Full lifecycle for orders ``first_index .. first_index+len(created_us)-1``."""
        cfg, ref, w = self.cfg, self.ref, self.world
        rng = np.random.default_rng([cfg.seed, stream, first_index])
        n = len(created_us)
        scale = cfg.lifecycle_time_scale
        order_ids = np.array([f"o{first_index + i:010d}" for i in range(n)], dtype=object)

        # who / where / how
        eligible = np.maximum(np.searchsorted(w.customer_signup_us, created_us, side="right"), 1)
        cust_idx = np.minimum((eligible * rng.random(n) ** 1.3).astype(np.int64), eligible - 1)
        cust_country = w.customer_country_idx[cust_idx]
        store_idx = np.empty(n, dtype=np.int64)
        u = rng.random(n)
        for ci, stores in w.stores_by_country.items():
            mask = cust_country == ci
            store_idx[mask] = stores[(u[mask] * len(stores)).astype(np.int64)]
        currency = w.stores["currency"].to_numpy()[store_idx]
        fx = np.vectorize(ev.FX_FROM_GBP.get)(currency).astype(float)
        channel = rng.choice(ev.SALES_CHANNELS, n, p=cfg.channel_probs)

        # basket
        k = rng.choice(ref.basket_lines[0], n, p=ref.basket_lines[1]).astype(np.int64)
        total_lines = int(k.sum())
        line_order = np.repeat(np.arange(n), k)
        offsets = np.concatenate([[0], np.cumsum(k)[:-1]])
        line_no = np.arange(total_lines) - np.repeat(offsets, k) + 1
        prod_idx = rng.choice(len(w.product_weights), total_lines, p=w.product_weights)
        qty = rng.choice(ref.line_quantity[0], total_lines, p=ref.line_quantity[1]).astype(np.int64)
        line_t = created_us[line_order]
        unit_price = np.round(self.price_at(prod_idx, line_t) * fx[line_order]).astype(np.int64)
        line_amount = qty * unit_price
        amount = np.add.reduceat(line_amount, offsets) if n else np.array([], dtype=np.int64)

        # lifecycle
        exp = rng.exponential
        never = np.iinfo(np.int64).max
        cancel_pre = rng.random(n) < cfg.p_cancel_before_payment
        pay1_start = created_us + (1 + exp(90, n)).astype(np.int64) * MICROS
        pay1_end = pay1_start + (1 + exp(20, n)).astype(np.int64) * MICROS
        fail1 = (rng.random(n) < cfg.p_payment_fail) & ~cancel_pre
        retry = fail1 & (rng.random(n) < cfg.p_retry_after_fail)
        pay2_start = pay1_end + (30 + exp(600, n)).astype(np.int64) * MICROS
        pay2_end = pay2_start + (1 + exp(20, n)).astype(np.int64) * MICROS
        fail2 = retry & (rng.random(n) < cfg.p_payment_fail)
        paid = ~cancel_pre & (~fail1 | (retry & ~fail2))
        paid_at = np.where(fail1, pay2_end, pay1_end) + MICROS
        cancel_post = paid & (rng.random(n) < cfg.p_cancel_after_paid)
        cancel_at = np.full(n, never, dtype=np.int64)
        cancel_reason = np.full(n, None, dtype=object)
        cancel_at[cancel_pre] = (
            created_us[cancel_pre] + (60 + exp(1800, int(cancel_pre.sum()))).astype(np.int64) * MICROS
        )
        cancel_reason[cancel_pre] = "CUSTOMER_REQUEST"
        pay_failed = ~cancel_pre & ~paid
        fail_end = np.where(retry, pay2_end, pay1_end)
        cancel_at[pay_failed] = fail_end[pay_failed] + 5 * MICROS
        cancel_reason[pay_failed] = "PAYMENT_FAILED"
        cancel_at[cancel_post] = (
            paid_at[cancel_post]
            + ((600 + exp(2 * 3600, int(cancel_post.sum()))) * scale).astype(np.int64) * MICROS
        )
        cancel_reason[cancel_post] = "CUSTOMER_REQUEST"
        ships = paid & ~cancel_post
        ship_at = paid_at + (rng.lognormal(np.log(18 * 3600), 0.6, n) * scale).astype(np.int64) * MICROS
        deliver_at = ship_at + (rng.lognormal(np.log(2.5 * 86400), 0.4, n) * scale).astype(np.int64) * MICROS
        cancelled = cancel_pre | pay_failed | cancel_post
        carrier = rng.choice(CARRIERS, n)
        tracking = rng.integers(10**11, 10**12, n)
        method1 = rng.choice(ev.PAYMENT_METHODS, n, p=[0.6, 0.2, 0.15, 0.05])
        method2 = np.where(rng.random(n) < 0.5, method1, rng.choice(ev.PAYMENT_METHODS, n))
        failure_reasons = rng.choice(
            ["INSUFFICIENT_FUNDS", "CARD_DECLINED", "FRAUD_SUSPECTED", "TIMEOUT"], n, p=[0.4, 0.35, 0.1, 0.15]
        )

        cust_ids = w.customers["customer_id"].to_numpy()[cust_idx]
        store_ids = w.stores["store_id"].to_numpy()[store_idx]
        prod_ids = w.products["product_id"].to_numpy()[prod_idx]

        # ---- commerce.orders
        items = [
            [
                {
                    "line_number": int(line_no[j]),
                    "product_id": prod_ids[j],
                    "quantity": int(qty[j]),
                    "unit_price_minor": int(unit_price[j]),
                }
                for j in range(offsets[i], offsets[i] + k[i])
            ]
            for i in range(n)
        ]
        base = {
            "order_id": order_ids,
            "customer_id": cust_ids,
            "store_id": store_ids,
            "amount_minor": amount,
            "currency": currency,
            "sales_channel": channel,
        }
        frames = [
            pd.DataFrame(
                {
                    **base,
                    "event_type": "ORDER_CREATED",
                    "event_timestamp": created_us,
                    "status": "CREATED",
                    "items": items,
                }
            )
        ]
        frames.append(
            pd.DataFrame(
                {**base, "event_type": "ORDER_UPDATED", "event_timestamp": paid_at, "status": "PAID"}
            )[paid]
        )
        frames.append(
            pd.DataFrame(
                {
                    **base,
                    "event_type": "ORDER_CANCELLED",
                    "event_timestamp": cancel_at,
                    "status": "CANCELLED",
                    "cancel_reason": cancel_reason,
                }
            )[cancelled]
        )
        frames.append(
            pd.DataFrame(
                {
                    **base,
                    "event_type": "ORDER_SHIPPED",
                    "event_timestamp": ship_at,
                    "status": "SHIPPED",
                    "carrier": carrier,
                    "tracking_number": [f"TRK{x}" for x in tracking],
                }
            )[ships]
        )
        frames.append(
            pd.DataFrame(
                {
                    **base,
                    "event_type": "ORDER_DELIVERED",
                    "event_timestamp": deliver_at,
                    "status": "DELIVERED",
                }
            )[ships]
        )
        orders = pd.concat(frames, ignore_index=True)

        # ---- commerce.payments
        pbase = {"order_id": order_ids, "amount_minor": amount, "currency": currency}
        att1 = ~cancel_pre
        pay_frames = [
            pd.DataFrame(
                {
                    **pbase,
                    "payment_id": [f"pay{o[1:]}_1" for o in order_ids],
                    "attempt": 1,
                    "method": method1,
                    "event_type": "PAYMENT_STARTED",
                    "status": "STARTED",
                    "event_timestamp": pay1_start,
                }
            )[att1],
            pd.DataFrame(
                {
                    **pbase,
                    "payment_id": [f"pay{o[1:]}_1" for o in order_ids],
                    "attempt": 1,
                    "method": method1,
                    "event_type": np.where(fail1, "PAYMENT_FAILED", "PAYMENT_COMPLETED"),
                    "status": np.where(fail1, "FAILED", "COMPLETED"),
                    "failure_reason": np.where(fail1, failure_reasons, None),  # type: ignore[call-overload]
                    "event_timestamp": pay1_end,
                }
            )[att1],
            pd.DataFrame(
                {
                    **pbase,
                    "payment_id": [f"pay{o[1:]}_2" for o in order_ids],
                    "attempt": 2,
                    "method": method2,
                    "event_type": "PAYMENT_STARTED",
                    "status": "STARTED",
                    "event_timestamp": pay2_start,
                }
            )[retry],
            pd.DataFrame(
                {
                    **pbase,
                    "payment_id": [f"pay{o[1:]}_2" for o in order_ids],
                    "attempt": 2,
                    "method": method2,
                    "event_type": np.where(fail2, "PAYMENT_FAILED", "PAYMENT_COMPLETED"),
                    "status": np.where(fail2, "FAILED", "COMPLETED"),
                    "failure_reason": np.where(fail2, failure_reasons, None),  # type: ignore[call-overload]
                    "event_timestamp": pay2_end,
                }
            )[retry],
        ]
        payments = pd.concat(pay_frames, ignore_index=True)

        # ---- commerce.inventory (reservation / release / shipment per line; restock added at emit)
        lo = line_order
        inv_key = np.char.add(np.char.add(store_ids[lo].astype(str), ":"), prod_ids.astype(str))
        ibase = {
            "store_id": store_ids[lo],
            "product_id": prod_ids,
            "inventory_key": inv_key,
            "order_id": order_ids[lo],
            "line_number": line_no,
        }
        inv_frames = [
            pd.DataFrame(
                {
                    **ibase,
                    "event_type": "INVENTORY_RESERVED",
                    "reason": "RESERVATION",
                    "on_hand_delta": 0,
                    "reserved_delta": qty,
                    "event_timestamp": created_us[lo] + line_no * 1000,
                }
            ),
            pd.DataFrame(
                {
                    **ibase,
                    "event_type": "INVENTORY_UPDATED",
                    "reason": "RELEASE",
                    "on_hand_delta": 0,
                    "reserved_delta": -qty,
                    "event_timestamp": cancel_at[lo] + line_no * 1000,
                }
            )[cancelled[lo]],
            pd.DataFrame(
                {
                    **ibase,
                    "event_type": "INVENTORY_UPDATED",
                    "reason": "SHIPMENT",
                    "on_hand_delta": -qty,
                    "reserved_delta": -qty,
                    "event_timestamp": ship_at[lo] + line_no * 1000,
                }
            )[ships[lo]],
        ]
        inventory = pd.concat(inv_frames, ignore_index=True)

        out = {ev.TOPIC_ORDERS: orders, ev.TOPIC_PAYMENTS: payments, ev.TOPIC_INVENTORY: inventory}
        for topic, df in out.items():
            if topic == ev.TOPIC_ORDERS:
                names = [f"{o}:{t}" for o, t in zip(df["order_id"], df["event_type"], strict=True)]
            elif topic == ev.TOPIC_PAYMENTS:
                names = [f"{p}:{t}" for p, t in zip(df["payment_id"], df["event_type"], strict=True)]
            else:
                names = [
                    f"{o}:{ln}:{r}"
                    for o, ln, r in zip(df["order_id"], df["line_number"], df["reason"], strict=True)
                ]
            df.insert(0, "event_id", self.ids.many(names))
        return out

    # --------------------------------------------------------------- stream
    def stream(self) -> Iterator[dict[str, pd.DataFrame]]:
        """Time-ordered batches covering the whole simulation window (historical/bulk mode)."""
        cfg = self.cfg
        times = self.order_times()
        pending: dict[str, list[pd.DataFrame]] = {t: [] for t in ev.TOPICS}
        pending[ev.TOPIC_CUSTOMERS].append(self.customer_events())
        pending[ev.TOPIC_PRODUCTS].append(self.product_events())
        tracker = InventoryTracker(self.ids)
        n_chunks = max(1, -(-cfg.orders // CHUNK_ORDERS))
        for c in range(n_chunks):
            lo, hi = c * CHUNK_ORDERS, min((c + 1) * CHUNK_ORDERS, cfg.orders)
            if hi > lo:
                for topic, df in self.generate_orders(lo, times[lo:hi]).items():
                    pending[topic].append(df)
            horizon = int(times[hi]) if hi < cfg.orders else cfg.end_us
            batch = {}
            for topic in ev.TOPICS:
                if not pending[topic]:
                    batch[topic] = pd.DataFrame()
                    continue
                df = pd.concat(pending[topic], ignore_index=True)
                if cfg.truncate_at_end:
                    df = df[df["event_timestamp"] < cfg.end_us]
                ready = df["event_timestamp"] < horizon
                batch[topic] = df[ready]
                pending[topic] = [df[~ready]] if (~ready).any() else []
            batch[ev.TOPIC_INVENTORY] = tracker.add_restocks(batch[ev.TOPIC_INVENTORY])
            yield {t: sort_events(d) for t, d in batch.items()}


def sort_events(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.sort_values(["event_timestamp", "event_id"], kind="stable").reset_index(drop=True)


class InventoryTracker:
    """Inserts RESTOCK events so available stock (on_hand - reserved) never runs out.

    Sequential by construction (stock is path-dependent), applied to time-ordered batches.
    """

    INITIAL_STOCK = 200
    REORDER_POINT = 25
    RESTOCK_QTY = 150

    def __init__(self, ids: EventIds, available: dict[str, int] | None = None, namespace: str = "") -> None:
        # ``namespace`` keeps restock event ids unique across independent runs (e.g. each live workload
        # run restarts its counters); the historical generator uses "" so its ids stay stable.
        self.namespace = namespace
        self.ids = ids
        self.available: dict[str, int] = dict(available or {})
        self.restocks: dict[str, int] = {}

    def add_restocks(self, inv: pd.DataFrame) -> pd.DataFrame:
        if inv.empty:
            return inv
        inv = inv.sort_values(["event_timestamp", "event_id"], kind="stable")
        keys = inv["inventory_key"].to_numpy()
        reasons = inv["reason"].to_numpy()
        qty = inv["reserved_delta"].to_numpy()
        ts = inv["event_timestamp"].to_numpy()
        new_rows: list[tuple[str, int, int]] = []  # key, ts, qty
        avail = self.available
        for i in range(len(keys)):
            key = keys[i]
            if key not in avail:
                avail[key] = self.INITIAL_STOCK
                new_rows.append((key, int(ts[i]) - 1, self.INITIAL_STOCK))
            r = reasons[i]
            if r == "RESERVATION":
                avail[key] -= int(qty[i])
                if avail[key] < self.REORDER_POINT:
                    avail[key] += self.RESTOCK_QTY
                    new_rows.append((key, int(ts[i]) + 1, self.RESTOCK_QTY))
            elif r == "RELEASE":
                avail[key] -= int(qty[i])  # qty is negative for release
        if not new_rows:
            return inv
        names = []
        for key, _, _ in new_rows:
            n = self.restocks.get(key, 0) + 1
            self.restocks[key] = n
            names.append(f"restock:{self.namespace}{key}:{n}")
        keys_arr = [r[0] for r in new_rows]
        restock = pd.DataFrame(
            {
                "event_id": self.ids.many(names),
                "store_id": [k.split(":")[0] for k in keys_arr],
                "product_id": [k.split(":")[1] for k in keys_arr],
                "inventory_key": keys_arr,
                "order_id": None,
                "line_number": 0,
                "event_type": "INVENTORY_UPDATED",
                "reason": "RESTOCK",
                "on_hand_delta": [r[2] for r in new_rows],
                "reserved_delta": 0,
                "event_timestamp": [r[1] for r in new_rows],
            }
        )
        return pd.concat([inv, restock], ignore_index=True)
