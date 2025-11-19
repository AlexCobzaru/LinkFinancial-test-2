import polars as pl
import sqlite3

# 2. Load CSV + Inspect Data în Polars

df = pl.read_csv("trades.csv", try_parse_dates=True)
df_numbers = df.select(pl.col(pl.Float64, pl.Int64)).drop("ID")

def create_connection():
    conn = sqlite3.connect("trades.db")
    df.to_pandas().to_sql("trades", conn, if_exists="replace", index=False)
    conn.close()

def task_1():
    print("Number of rows & columns generated:",df.shape)
    print("Sample of 10 rows:",df.sample(10))
    print("Count of unique stock symbols generated:",df.select(pl.col("Symbol").n_unique()).item())
    print("Summary of numeric fields (min, max, mean, etc.):",df_numbers.describe())


# ========== Task 2 — Data Cleaning în Polars ==========
# 1. Detectarea rândurilor invalide
issues = {
    "missing_any": df.filter(pl.any_horizontal(pl.col("*").is_null())),
    "invalid_side": df.filter(~pl.col("Side").is_in(["BUY", "SELL"])),
    "invalid_qty": df.filter(pl.col("Quantity") <= 0),
    "invalid_price": df.filter(pl.col("Price") <= 0),
    "invalid_timestamp": df.filter(pl.col("Timestamp").is_null())
}

error_cols = {
    "invalid_side": "Side",
    "invalid_qty": "Quantity",
    "invalid_price": "Price",
    "invalid_timestamp": "Timestamp",
    "missing_any": None,
}

def log_errors(error_cols = error_cols, issues = issues):
    for err, df_err in issues.items():
        print(f"\n=== {err} ===")
        
        if error_cols[err] is None:
            # pentru missing → afișezi toate coloanele
            print(df_err)
        else:
            # pentru invalid → afișezi doar ID + coloana relevantă
            print(df_err.select(["ID", error_cols[err]]))

# 2. Curățare dataset
clean_df = (
    df
    .filter(
        (pl.col("Side").is_in(["BUY", "SELL"])) &
        (pl.col("Quantity") > 0) &
        (pl.col("Price") > 0) &
        (pl.col("Timestamp").is_not_null())
    )
)

def log_clean():
    print(clean_df.head(len(clean_df)))

# ========== Task 3 — Financial Analytics în Polars ==========
# 1. Total volume per symbol
volume = clean_df.group_by("Symbol").agg(
    total_volume = pl.col("Quantity").sum()
)

# 2. Total traded value per symbol
value_df = clean_df.with_columns(
    (pl.col("Quantity") * pl.col("Price")).alias("Value")
)

value = value_df.group_by("Symbol").agg(
    total_value = pl.col("Value").sum()
)

# 3. Net position (BUY - SELL)
net_df = clean_df.with_columns([
    pl.when(pl.col("Side") == "BUY")
      .then(pl.col("Quantity"))
      .otherwise(0)
      .alias("BuyQty"),
    
    pl.when(pl.col("Side") == "SELL")
      .then(pl.col("Quantity"))
      .otherwise(0)
      .alias("SellQty")
])

net_position = (
    net_df
    .group_by("Symbol")
    .agg([
        pl.col("BuyQty").sum().alias("TotalBuy"),
        pl.col("SellQty").sum().alias("TotalSell")
    ])
    .with_columns(
        (pl.col("TotalBuy") - pl.col("TotalSell")).alias("NetPosition")
    )
)


# 4. Highest volume day
clean_df = clean_df.with_columns(
    pl.col("Timestamp").dt.date().alias("Date")
)

overall_day = (
    clean_df.group_by("Date")
    .agg(total_vol = pl.col("Quantity").sum())
    .sort("total_vol", descending=True)
    .head(1)
)

# 5. Highest volume day per symbol
per_symbol = (
    clean_df.group_by(["Symbol", "Date"])
    .agg(total_vol = pl.col("Quantity").sum())
)

highest_per_symbol = (
    per_symbol
    .sort(["Symbol", "total_vol"], descending=[False, True])
    .group_by("Symbol")
    .head(1)
)
