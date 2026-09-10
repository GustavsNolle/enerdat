"""Which bidding zone should a cost-optimal scheduler target?

Run this BEFORE building anything. The euro objective exploits asymmetry
between the cost of being long and the cost of being short; where imbalance
pricing is symmetric there is nothing for it to bite on, and the whole idea
collapses to median regression.

This project learned that the expensive way: BE was chosen on data-quality
grounds and turned out to be almost perfectly symmetric (tau 0.462), so the
objective was worth 0.2%. One query would have said so up front.

Measured Jun-Sep 2026:

    zone   c_long   c_short     tau   asym
    BE      19.75     22.99   0.462   1.16x  symmetric
    NL      36.68     32.71   0.529   1.12x  symmetric
    FR      28.64     34.91   0.451   1.22x  symmetric
    DK_1    31.43     32.70   0.490   1.04x  symmetric
    DK_2    29.23     31.22   0.484   1.07x  symmetric
    AT      23.65     28.99   0.449   1.23x  symmetric
    ES      29.75     21.10   0.585   1.41x  long punished
    PL     115.90    439.78   0.209   3.79x  SHORT punished

Over a full year PL is stronger still: c_long 54.38, c_short 397.35,
tau 0.120, 7.31x, and every one of thirteen months sits below tau 0.34.
"""
import warnings; warnings.filterwarnings("ignore")
import pandas as pd, numpy as np
from dotenv import dotenv_values
from entsoe import EntsoePandasClient

c = EntsoePandasClient(api_key=dotenv_values('.env')['ENTSOE_API_KEY'],
                       timeout=120, retry_count=1, retry_delay=0)
TZ = "Europe/Brussels"
s = pd.Timestamp("20260601", tz=TZ); e = pd.Timestamp("20260901", tz=TZ)

print(f"{'zone':8} {'c_long':>8} {'c_short':>8} {'tau':>7} {'asym':>7}  read")
for zone in ["BE", "NL", "FR", "DK_1", "DK_2", "PL", "ES", "AT"]:
    try:
        da = c.query_day_ahead_prices(zone, start=s, end=e)
        imb = c.query_imbalance_prices(zone, start=s, end=e)
    except Exception as ex:
        print(f"{zone:8} {'-':>8} {'-':>8} {'-':>7} {'-':>7}  {type(ex).__name__}")
        continue

    cols = [x for x in imb.columns if pd.api.types.is_numeric_dtype(imb[x])]
    if not cols:
        print(f"{zone:8} no numeric imbalance columns"); continue
    lo = imb[cols[0]]; sh = imb[cols[-1]]
    df = pd.DataFrame({"da": da, "lo": lo, "sh": sh}).dropna()
    if df.empty:
        print(f"{zone:8} no overlap"); continue

    c_long = (df.da - df.lo).clip(lower=0).mean()
    c_short = (df.sh - df.da).clip(lower=0).mean()
    tau = c_long / (c_long + c_short) if (c_long + c_short) else float("nan")
    asym = max(c_long, c_short) / max(min(c_long, c_short), 1e-9)
    read = "symmetric" if 0.42 <= tau <= 0.58 else ("SHORT punished" if tau < 0.42 else "LONG punished")
    print(f"{zone:8} {c_long:8.2f} {c_short:8.2f} {tau:7.3f} {asym:7.2f}x  {read}", flush=True)
