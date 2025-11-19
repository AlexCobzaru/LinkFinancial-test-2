#1. Generare dataset (tot Python, dar folosim Polars din start)

import polars as pl
import random
from datetime import datetime, timedelta

symbols = ["AAPL", "GOOG", "MSFT", "TSLA", "NVDA"]
sides = ["BUY", "SELL"]
base = datetime(2024, 8, 15, 9, 0, 0)

def create_data(rows = 500, sorted_column = "Timestamp"):
    timestamps = []
    symbols_list = []
    sides_list = []
    qty_list = []
    price_list = []

    for _ in range(rows):
        ts = base + timedelta(
        days=random.randint(0, 20),
        minutes=random.randint(0, 360))
        timestamps.append(ts.isoformat())

        symbols_list.append(random.choice(symbols))

        sides_list.append(random.choice(sides + ["INVALID"]))  # invalid

        qty_list.append(int(random.choice([0, -10, random.randint(1, 500)])))

        price_list.append(float(random.choice([0, -10.0, round(random.uniform(80, 500), 2)])))


    df = pl.DataFrame({
        "Timestamp": timestamps,
        "Symbol": symbols_list,
        "Side": sides_list,
        "Quantity": qty_list,
        "Price": price_list
    }).sort(sorted_column)

    df = df.with_row_index("ID")

    df.write_csv("trades.csv")