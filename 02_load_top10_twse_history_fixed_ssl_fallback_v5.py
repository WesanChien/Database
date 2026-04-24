from __future__ import annotations

import calendar
import math
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, List, Optional

import pandas as pd
import pyodbc
import requests
import urllib3
from requests.exceptions import SSLError


# =========================
# 你要改的設定
# =========================
SQL_SERVER = "localhost,14330"
SQL_DATABASE = "TAtest"
SQL_USERNAME = "wesan"
SQL_PASSWORD = "XXXX"

START_YEAR = 2022
START_MONTH = 1
END_YEAR = 2026
END_MONTH = 3

REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 1.2   # 避免打太快被擋
VERIFY_SSL = False     # 先嘗試正常驗證；若本機憑證鏈有問題，程式會自動 fallback 為 verify=False


# =========================
# 官方資料來源
# =========================
# 固定寫死的前 10 大上市股票（方便課堂示範，不做自動更新）
# 依照 2026/03 左右 TWSE 上市公司市值排行，排除 ETF 0050 後取前 10 檔股票。
FIXED_TOP10_STOCKS = [
    ("2330", "台積電"),
    ("2308", "台達電"),
    ("2317", "鴻海"),
    ("2454", "聯發科"),
    ("3711", "日月光投控"),
    ("2881", "富邦金"),
    ("2382", "廣達"),
    ("2412", "中華電"),
    ("2882", "國泰金"),
    ("2891", "中信金"),
]

# TWSE 月資料：一次抓單一股票單一月份
TWSE_STOCK_DAY_URLS = [
    "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY",
    "https://www.twse.com.tw/exchangeReport/STOCK_DAY",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    )
}


@dataclass
class Constituent:
    stock_code: str
    stock_name: str
    snapshot_date: date
    source: str


def normalize_code(code: str) -> str:
    return str(code).strip().replace(".TW", "")


def parse_roc_date(roc_text: str) -> date:
    # 例如 111/01/03 -> 2022-01-03
    roc_text = str(roc_text).strip()
    y, m, d = roc_text.split("/")
    return date(int(y) + 1911, int(m), int(d))


def clean_number(value) -> Optional[float]:
    if value is None:
        return None

    text = str(value).strip()
    if text in {"", "--", "---", "----", "X0.00", "除權息", "除息", "除權", "null", "None"}:
        return None

    text = text.replace(",", "").replace(" ", "")
    text = text.replace("▲", "").replace("△", "")
    text = text.replace("▼", "-").replace("▽", "-")
    text = text.replace("+", "")

    try:
        return float(text)
    except ValueError:
        return None


def clean_int(value) -> Optional[int]:
    num = clean_number(value)
    if num is None or math.isnan(num):
        return None
    return int(num)


def clean_float(value, digits: int = 2) -> Optional[float]:
    num = clean_number(value)
    if num is None or math.isnan(num):
        return None
    return round(float(num), digits)


def month_iter(start_year: int, start_month: int, end_year: int, end_month: int):
    y, m = start_year, start_month
    while (y < end_year) or (y == end_year and m <= end_month):
        yield y, m
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def try_get_json(session: requests.Session, url: str):
    resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
    resp.raise_for_status()
    return resp.json()


def try_get_text(session: requests.Session, url: str) -> str:
    resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


def fetch_fixed_top10_constituents() -> List[Constituent]:
    snapshot = date.today()
    return [
        Constituent(
            stock_code=code,
            stock_name=name,
            snapshot_date=snapshot,
            source="Fixed top10 stock list for teaching demo",
        )
        for code, name in FIXED_TOP10_STOCKS
    ]


def safe_get(session: requests.Session, url: str, *, params=None):
    try:
        return session.get(
            url,
            params=params,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            verify=VERIFY_SSL,
        )
    except SSLError as e:
        print(f"      SSL 驗證失敗，改用 verify=False 重試：{e}")
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return session.get(
            url,
            params=params,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            verify=False,
        )


def fetch_twse_month(session: requests.Session, stock_code: str, year: int, month: int) -> pd.DataFrame:
    yyyymmdd = f"{year}{month:02d}01"
    params = {
        "response": "json",
        "date": yyyymmdd,
        "stockNo": stock_code,
    }

    payload = None
    last_error = None

    for url in TWSE_STOCK_DAY_URLS:
        try:
            resp = safe_get(session, url, params=params)
            if resp.status_code == 404:
                last_error = f"404 from {url}"
                continue

            resp.raise_for_status()
            payload = resp.json()
            break
        except Exception as e:
            last_error = e
            continue

    if payload is None:
        raise RuntimeError(
            f"無法取得 {stock_code} {yyyymmdd} 的月資料，最後錯誤：{last_error}"
        )

    if payload.get("stat") != "OK":
        # 某些月份可能停牌、尚未上市、資料不存在
        return pd.DataFrame()

    data = payload.get("data", [])
    if not data:
        return pd.DataFrame()

    rows = []
    for row in data:
        # TWSE STOCK_DAY 常見欄位順序：
        # 日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數
        if len(row) < 9:
            continue

        rows.append(
            {
                "Date": parse_roc_date(row[0]),
                "StockCode": stock_code,
                "Capacity": clean_int(row[1]),
                "Volume": clean_float(row[2], 2),
                "Open": clean_float(row[3], 2),
                "High": clean_float(row[4], 2),
                "Low": clean_float(row[5], 2),
                "Close": clean_float(row[6], 2),
                "Change": clean_float(row[7], 2),
                "Transaction": clean_int(row[8]),
            }
        )

    return pd.DataFrame(rows)


def compute_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["StockCode", "Date"]).copy()
    for window in [5, 10, 20, 60, 120, 240]:
        df[f"MA{window}"] = (
            df.groupby("StockCode")["Close"]
            .transform(lambda s: s.rolling(window=window, min_periods=window).mean())
            .round(2)
        )
    return df


def connect_sql_server() -> pyodbc.Connection:
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def upsert_constituents(conn: pyodbc.Connection, constituents: List[Constituent]) -> None:
    sql = """
    MERGE dbo.TW50_Constituents_Current AS target
    USING (SELECT ? AS SnapshotDate, ? AS StockCode, ? AS StockName, ? AS Source) AS src
    ON target.SnapshotDate = src.SnapshotDate AND target.StockCode = src.StockCode
    WHEN MATCHED THEN
        UPDATE SET StockName = src.StockName, Source = src.Source
    WHEN NOT MATCHED THEN
        INSERT (SnapshotDate, StockCode, StockName, Source)
        VALUES (src.SnapshotDate, src.StockCode, src.StockName, src.Source);
    """

    cur = conn.cursor()
    for c in constituents:
        cur.execute(sql, c.snapshot_date, c.stock_code, c.stock_name, c.source)
    conn.commit()



def normalize_sql_float(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "null"}:
        return None

    try:
        return float(text)
    except Exception:
        return None


def normalize_sql_decimal(value, digits=2):
    value = normalize_sql_float(value)
    if value is None:
        return None
    return round(float(value), digits)


def normalize_sql_int(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    text = str(value).strip().replace(",", "")
    if text == "" or text.lower() in {"nan", "none", "null"}:
        return None

    try:
        return int(float(text))
    except Exception:
        return None

def upsert_daily_price(conn: pyodbc.Connection, df: pd.DataFrame) -> None:
    sql = """
    MERGE dbo.TW50_DailyPrice AS target
    USING (
        SELECT
            CAST(? AS DATE) AS [Date],
            CAST(? AS VARCHAR(10)) AS StockCode,
            CAST(? AS BIGINT) AS Capacity,
            CAST(? AS DECIMAL(20,2)) AS [Volume],
            CAST(? AS DECIMAL(10,2)) AS [Open],
            CAST(? AS DECIMAL(10,2)) AS High,
            CAST(? AS DECIMAL(10,2)) AS Low,
            CAST(? AS DECIMAL(10,2)) AS [Close],
            CAST(? AS DECIMAL(10,2)) AS [Change],
            CAST(? AS BIGINT) AS [Transaction],
            CAST(? AS DECIMAL(10,2)) AS MA5,
            CAST(? AS DECIMAL(10,2)) AS MA10,
            CAST(? AS DECIMAL(10,2)) AS MA20,
            CAST(? AS DECIMAL(10,2)) AS MA60,
            CAST(? AS DECIMAL(10,2)) AS MA120,
            CAST(? AS DECIMAL(10,2)) AS MA240
    ) AS src
    ON target.[Date] = src.[Date] AND target.StockCode = src.StockCode
    WHEN MATCHED THEN
        UPDATE SET
            Capacity = src.Capacity,
            [Volume] = src.[Volume],
            [Open] = src.[Open],
            High = src.High,
            Low = src.Low,
            [Close] = src.[Close],
            [Change] = src.[Change],
            [Transaction] = src.[Transaction],
            MA5 = src.MA5,
            MA10 = src.MA10,
            MA20 = src.MA20,
            MA60 = src.MA60,
            MA120 = src.MA120,
            MA240 = src.MA240
    WHEN NOT MATCHED THEN
        INSERT (
            [Date], StockCode, Capacity, [Volume], [Open], High, Low, [Close], [Change], [Transaction],
            MA5, MA10, MA20, MA60, MA120, MA240
        )
        VALUES (
            src.[Date], src.StockCode, src.Capacity, src.[Volume], src.[Open], src.High, src.Low, src.[Close], src.[Change], src.[Transaction],
            src.MA5, src.MA10, src.MA20, src.MA60, src.MA120, src.MA240
        );
    """

    cur = conn.cursor()
    cur.fast_executemany = False

    for row in df.itertuples(index=False):
        try:
            cur.execute(
                sql,
                row.Date,
                str(row.StockCode).strip(),
                normalize_sql_int(row.Capacity),
                normalize_sql_decimal(row.Volume),
                normalize_sql_decimal(row.Open),
                normalize_sql_decimal(row.High),
                normalize_sql_decimal(row.Low),
                normalize_sql_decimal(row.Close),
                normalize_sql_decimal(row.Change),
                normalize_sql_int(row.Transaction),
                normalize_sql_decimal(row.MA5),
                normalize_sql_decimal(row.MA10),
                normalize_sql_decimal(row.MA20),
                normalize_sql_decimal(row.MA60),
                normalize_sql_decimal(row.MA120),
                normalize_sql_decimal(row.MA240),
            )
        except Exception:
            print("寫入失敗資料列：")
            print({
                "Date": row.Date,
                "StockCode": row.StockCode,
                "Capacity": row.Capacity,
                "Volume": row.Volume,
                "Open": row.Open,
                "High": row.High,
                "Low": row.Low,
                "Close": row.Close,
                "Change": row.Change,
                "Transaction": row.Transaction,
                "MA5": row.MA5,
                "MA10": row.MA10,
                "MA20": row.MA20,
                "MA60": row.MA60,
                "MA120": row.MA120,
                "MA240": row.MA240,
            })
            raise

    conn.commit()


def main():
    if SQL_PASSWORD == "請填入你的SQL密碼":
        raise ValueError("請先把 SQL_PASSWORD 改成你的 SQL Server 密碼。")

    session = requests.Session()

    print("1) 讀取固定寫死的前 10 大股票清單...")
    constituents = fetch_fixed_top10_constituents()
    print(f"   成功載入 {len(constituents)} 檔股票")

    codes = [c.stock_code for c in constituents]
    print("   代號：", ",".join(codes))

    print("2) 下載歷史日資料...")
    all_frames = []
    for idx, code in enumerate(codes, start=1):
        print(f"   [{idx:02d}/{len(codes):02d}] {code}")
        one_stock_frames = []
        for y, m in month_iter(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
            df_month = fetch_twse_month(session, code, y, m)
            if not df_month.empty:
                one_stock_frames.append(df_month)
            time.sleep(SLEEP_SECONDS)

        if one_stock_frames:
            stock_df = pd.concat(one_stock_frames, ignore_index=True).drop_duplicates(
                subset=["Date", "StockCode"]
            )
            all_frames.append(stock_df)

    if not all_frames:
        raise RuntimeError("沒有抓到任何歷史資料。請檢查網路、股票代號清單、或 TWSE API 是否有回應。")

    df = pd.concat(all_frames, ignore_index=True)
    df = df.sort_values(["StockCode", "Date"]).reset_index(drop=True)

    print("3) 計算 MA5 / MA10 / MA20 / MA60 / MA120 / MA240 ...")
    df = compute_moving_averages(df)

    print("4) 寫入 SQL Server ...")
    conn = connect_sql_server()
    try:
        upsert_constituents(conn, constituents)
        upsert_daily_price(conn, df)
    finally:
        conn.close()

    print("完成")
    print(f"共寫入 / 更新 {len(constituents)} 檔成分股，{len(df)} 筆日資料。")


if __name__ == "__main__":
    main()
