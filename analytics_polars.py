import polars as pl
import sqlite3

def get_summary():
    conn = sqlite3.connect("trades.db")

    df = pl.read_database("SELECT * FROM trades", connection=conn)

    # Clean + compute value
    df = df.filter(
        (pl.col("Side").is_in(["BUY","SELL"])) &
        (pl.col("Quantity") > 0) &
        (pl.col("Price") > 0)
    ).with_columns(
        (pl.col("Quantity") * pl.col("Price")).alias("Value"),
        pl.when(pl.col("Side")=="BUY").then(pl.col("Quantity")).otherwise(0).alias("BuyQty"),
        pl.when(pl.col("Side")=="SELL").then(pl.col("Quantity")).otherwise(0).alias("SellQty")
    )

    summary = (
        df.group_by("Symbol")
        .agg([
            pl.col("Quantity").sum().alias("TotalVolume"),
            pl.col("Value").sum().alias("TotalValue"),
            (pl.col("BuyQty").sum() - pl.col("SellQty").sum()).alias("NetPosition")
        ])
    )

    return summary.to_dicts()


def get_trend(symbol):
    conn = sqlite3.connect("trades.db")

    df = pl.read_database(f"SELECT * FROM trades WHERE Symbol='{symbol}'", connection=conn)

    df = df.filter(
        (pl.col("Quantity") > 0) &
        (pl.col("Price") > 0) &
        (pl.col("Side").is_in(["BUY","SELL"]))
    ).with_columns(
        pl.col("Timestamp").str.strptime(pl.Datetime).dt.date().alias("Date")
    )

    trend = (
        df.group_by("Date")
        .agg(pl.col("Quantity").sum().alias("Volume"))
        .sort("Date")
    )

    return trend.to_dicts()
