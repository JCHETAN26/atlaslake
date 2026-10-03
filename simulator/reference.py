"""Real-data reference layer for the commerce simulator.

Source: UCI Online Retail II (Chen, 2019; CC BY 4.0) — ~1.07M real transaction lines from a UK online
retailer, 2009-12 → 2011-12. https://archive.ics.uci.edu/dataset/502/online+retail+ii

``prepare()`` derives a compact, committed reference set from the raw file:

* ``products.csv``        — the most-sold stock codes with real descriptions, median real prices
                             and popularity weights (used to sample basket contents)
* ``distributions.json``  — empirical basket-size, line-quantity, hour-of-day, weekday and
                             customer-country distributions

The simulator then expands this real base deterministically (seeded) into customers, stores,
orders and full lifecycles. If the reference files are missing, ``load()`` falls back to a small
synthetic catalogue so tests never depend on the download.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

REFERENCE_DIR = Path(__file__).parent / "reference_data"
UCI_URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"

# Ordered keyword → category rules applied to the (upper-case) product description.
CATEGORY_RULES: list[tuple[str, str]] = [
    (r"CHRISTMAS|XMAS|ADVENT|SANTA|REINDEER", "Seasonal"),
    (r"\bBAG\b|BAGS|PURSE|TOTE|SHOPPER", "Bags"),
    (r"CANDLE|T-LIGHT|TEA LIGHT|LANTERN|LIGHT HOLDER", "Candles & Lighting"),
    (r"\bLIGHTS?\b|LAMP", "Candles & Lighting"),
    (
        r"\bMUG\b|\bCUP\b|PLATE|BOWL|TEAPOT|JUG|GLASS|CAKE|BAKING|KITCHEN|LUNCH BOX|NAPKIN|TRAY",
        "Kitchen & Dining",
    ),
    (
        r"\bCARD\b|CARDS|WRAP|PAPER|NOTEBOOK|PENCIL|PEN\b|STICKER|CHALK|STATIONERY|ENVELOPE",
        "Stationery & Gifts",
    ),
    (r"CHILDREN|CHILDS|TOY|DOLL|GAME|SPACEBOY|DINOSAUR|PARTY", "Kids & Party"),
    (r"FRAME|CLOCK|SIGN|MIRROR|CUSHION|DOORMAT|WALL|HOOK|STORAGE|BOX|DRAWER|BASKET|VASE", "Home Decor"),
    (r"GARDEN|PLANT|FLOWER|BIRD|WATERING", "Garden"),
    (r"NECKLACE|BRACELET|EARRINGS|RING|HAIR|JEWEL", "Jewellery & Accessories"),
]

US_CITIES: list[tuple[str, str]] = [
    ("Phoenix", "AZ"),
    ("Tempe", "AZ"),
    ("Tucson", "AZ"),
    ("San Jose", "CA"),
    ("Los Angeles", "CA"),
    ("San Diego", "CA"),
    ("San Francisco", "CA"),
    ("Seattle", "WA"),
    ("Portland", "OR"),
    ("Denver", "CO"),
    ("Austin", "TX"),
    ("Dallas", "TX"),
    ("Houston", "TX"),
    ("Chicago", "IL"),
    ("New York", "NY"),
    ("Boston", "MA"),
    ("Atlanta", "GA"),
    ("Miami", "FL"),
    ("Minneapolis", "MN"),
    ("Salt Lake City", "UT"),
]


@dataclass(frozen=True)
class Reference:
    products: pd.DataFrame  # stock_code, name, category, unit_price_minor, weight
    basket_lines: tuple[np.ndarray, np.ndarray]  # values, probabilities
    line_quantity: tuple[np.ndarray, np.ndarray]
    hour_of_day: np.ndarray  # 24 probabilities
    weekday: np.ndarray  # 7 probabilities (Mon=0)
    countries: tuple[list[str], np.ndarray]
    source: str


def categorize(description: str) -> str:
    text = description.upper()
    for pattern, category in CATEGORY_RULES:
        if re.search(pattern, text):
            return category
    return "Home & Living"


def _histogram(series: pd.Series, lo: int, hi: int, divisor: float = 1.0) -> dict[str, list[float]]:
    """Empirical histogram after rescaling by ``divisor`` (ceil) and clipping to [lo, hi]."""
    clipped = pd.Series(np.clip(np.ceil(series.to_numpy() / divisor), lo, hi).astype(int))
    counts = clipped.value_counts().sort_index()
    return {"values": [int(v) for v in counts.index], "probs": (counts / counts.sum()).round(8).tolist()}


def prepare(raw_path: Path, out_dir: Path = REFERENCE_DIR, n_products: int = 3000) -> dict[str, int]:
    """Derive the committed reference set from the raw UCI file (.xlsx or cached .parquet)."""
    if raw_path.suffix == ".parquet":
        df = pd.read_parquet(raw_path)
    else:
        sheets = pd.read_excel(raw_path, sheet_name=None, dtype={"Invoice": str, "StockCode": str})
        df = pd.concat(sheets.values(), ignore_index=True)
    df = df.rename(columns={"Customer ID": "CustomerID"})
    raw_rows = len(df)
    df = df[~df["Invoice"].astype(str).str.startswith("C")]
    df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]
    df = df[df["StockCode"].astype(str).str.match(r"^\d{5}[A-Z]?$")]
    df = df.dropna(subset=["Description"])
    clean_rows = len(df)

    grouped = df.groupby("StockCode")
    products = pd.DataFrame(
        {
            "name": grouped["Description"].agg(lambda s: s.mode().iat[0]).str.strip().str.title(),
            "median_price": grouped["Price"].median(),
            "weight": grouped.size(),
        }
    ).sort_values("weight", ascending=False)
    products = products[products["median_price"].between(0.1, 500)].head(n_products)
    products["unit_price_minor"] = (products["median_price"] * 100).round().astype(int)
    products["category"] = products["name"].map(categorize)
    products = products.reset_index().rename(columns={"StockCode": "stock_code"})
    products = products[["stock_code", "name", "category", "unit_price_minor", "weight"]]

    invoices = df.groupby("Invoice").agg(lines=("StockCode", "size"), ts=("InvoiceDate", "first"))
    customers = df.dropna(subset=["CustomerID"]).groupby("CustomerID")["Country"].first()
    hours = invoices["ts"].dt.hour.value_counts().reindex(range(24), fill_value=0)
    weekdays = invoices["ts"].dt.weekday.value_counts().reindex(range(7), fill_value=0)
    countries = customers.value_counts()
    top = countries.head(12)
    countries = pd.concat([top, pd.Series({"Other": countries.iloc[12:].sum()})])

    dists = {
        "source": "UCI Online Retail II (CC BY 4.0)",
        "raw_rows": int(raw_rows),
        "clean_rows": int(clean_rows),
        "invoices": len(invoices),
        "customers": len(customers),
        # The retailer is a wholesaler (median basket ~ 15 lines, quantities in dozens). We keep the
        # *shape* of each distribution but rescale it to retail-sized baskets before clipping.
        "basket_lines": _histogram(invoices["lines"], 1, 8, divisor=6),
        "line_quantity": _histogram(df["Quantity"], 1, 6, divisor=6),
        "hour_of_day": (hours / hours.sum()).round(8).tolist(),
        "weekday": (weekdays / weekdays.sum()).round(8).tolist(),
        "countries": {
            "values": list(countries.index),
            "probs": (countries / countries.sum()).round(8).tolist(),
        },
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    products.to_csv(out_dir / "products.csv", index=False)
    (out_dir / "distributions.json").write_text(json.dumps(dists, indent=2))
    return {"raw_rows": raw_rows, "clean_rows": clean_rows, "products": len(products)}


def _normalise(p: list[float] | np.ndarray) -> np.ndarray:
    arr = np.asarray(p, dtype=float)
    # Smooth zeros (e.g. hours the UK shop was closed) so a 24h synthetic business still trades.
    arr = arr + arr.max() * 0.02
    return arr / arr.sum()


def _synthetic() -> Reference:
    rng = np.random.default_rng(0)
    n = 500
    cats = [c for _, c in CATEGORY_RULES]
    products = pd.DataFrame(
        {
            "stock_code": [f"{10000 + i}" for i in range(n)],
            "name": [f"Synthetic Product {i}" for i in range(n)],
            "category": [cats[i % len(cats)] for i in range(n)],
            "unit_price_minor": rng.integers(100, 5000, n),
            "weight": rng.zipf(1.5, n).clip(1, 1000),
        }
    )
    return Reference(
        products=products,
        basket_lines=(np.arange(1, 9), _normalise([30, 22, 15, 10, 8, 6, 5, 4])),
        line_quantity=(np.arange(1, 7), _normalise([50, 20, 10, 8, 6, 6])),
        hour_of_day=_normalise(np.ones(24)),
        weekday=_normalise(np.ones(7)),
        countries=(["United States"], np.array([1.0])),
        source="synthetic-fallback",
    )


def load(directory: Path = REFERENCE_DIR) -> Reference:
    products_path, dists_path = directory / "products.csv", directory / "distributions.json"
    if not (products_path.exists() and dists_path.exists()):
        return _synthetic()
    products = pd.read_csv(products_path, dtype={"stock_code": str})
    d = json.loads(dists_path.read_text())
    return Reference(
        products=products,
        basket_lines=(np.array(d["basket_lines"]["values"]), _normalise(d["basket_lines"]["probs"])),
        line_quantity=(np.array(d["line_quantity"]["values"]), _normalise(d["line_quantity"]["probs"])),
        hour_of_day=_normalise(d["hour_of_day"]),
        weekday=_normalise(d["weekday"]),
        countries=(d["countries"]["values"], _normalise(d["countries"]["probs"])),
        source=d["source"],
    )
