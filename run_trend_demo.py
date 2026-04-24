import json
from datetime import datetime
import pyodbc

SERVER = "localhost,14330"
DATABASE = "TAtest"
USERNAME = "wesan"
PASSWORD = "wesan0213"

TABLE_NAME = "dbo.TW50_DailyPrice"
SP_NAME = "dbo.sp_CalculateTrend"
ALLOWED_MA = {"MA5", "MA10", "MA20", "MA60", "MA120", "MA240"}


def get_connection():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def prompt_user():
    stock_code = input("請輸入股票代號: ").strip()
    ma_type = input("請輸入均線類型（如 MA5、MA10、MA20、MA60、MA120、MA240）: ").strip().upper()
    lookback_days = int(input("請輸入從資料表日期往回看的天數（包含當天）: ").strip())
    decision_days = int(input("請輸入決定天數: ").strip())

    if not stock_code:
        raise ValueError("股票代號不可為空。")
    if ma_type not in ALLOWED_MA:
        raise ValueError("均線類型錯誤，請輸入 MA5 / MA10 / MA20 / MA60 / MA120 / MA240。")
    if lookback_days <= 0 or decision_days <= 0:
        raise ValueError("天數必須大於 0。")
    if decision_days > lookback_days:
        raise ValueError("決定天數不可大於往回看的天數。")

    return stock_code, ma_type, lookback_days, decision_days


def execute_sp(cursor, stock_code, ma_type, lookback_days, decision_days):
    cursor.execute(
        f"EXEC {SP_NAME} ?, ?, ?, ?",
        stock_code,
        ma_type,
        lookback_days,
        decision_days,
    )


def fetch_latest_window(cursor, stock_code, ma_type, lookback_days):
    sql = f"""
    SELECT TOP (?) 
        [Date],
        StockCode,
        [{ma_type}] AS MAValue,
        Trend
    FROM {TABLE_NAME}
    WHERE StockCode = ?
      AND [{ma_type}] IS NOT NULL
    ORDER BY [Date] DESC;
    """
    cursor.execute(sql, lookback_days, stock_code)
    rows = cursor.fetchall()
    rows = list(reversed(rows))
    return rows


def analyze_window(rows, stock_code, ma_type, lookback_days, decision_days):
    if not rows:
        return None

    up_days = 0
    down_days = 0
    no_change_days = 0

    for i in range(1, len(rows)):
        today_ma = rows[i].MAValue
        yesterday_ma = rows[i - 1].MAValue

        if today_ma is None or yesterday_ma is None:
            no_change_days += 1
        elif today_ma > yesterday_ma:
            up_days += 1
        elif today_ma < yesterday_ma:
            down_days += 1
        else:
            no_change_days += 1

    latest = rows[-1]
    start_date = rows[0].Date.strftime("%Y-%m-%d")
    selected_date = latest.Date.strftime("%Y-%m-%d")

    if up_days >= decision_days:
        inferred_trend = "上漲趨勢"
    elif down_days >= decision_days:
        inferred_trend = "下跌趨勢"
    else:
        inferred_trend = "盤整整理"

    result = {
        "StockCode": stock_code,
        "MovingAverage": ma_type,
        "SelectedDate": selected_date,
        "LookbackPeriod": lookback_days,
        "DecisionDays": decision_days,
        "StartDate": start_date,
        "TotalDays": len(rows),
        "UpDays": up_days,
        "DownDays": down_days,
        "NoChangeDays": no_change_days,
        "Trend": latest.Trend if latest.Trend else inferred_trend,
    }
    return result


def fetch_latest_10(cursor, stock_code, ma_type):
    sql = f"""
    SELECT TOP (10)
        [Date],
        StockCode,
        [{ma_type}] AS MAValue,
        Trend
    FROM {TABLE_NAME}
    WHERE StockCode = ?
      AND [{ma_type}] IS NOT NULL
    ORDER BY [Date] DESC;
    """
    cursor.execute(sql, stock_code)
    return cursor.fetchall()


def main():
    print("=" * 60)
    print("資料庫課程 - Trend 計算工具")
    print("=" * 60)

    try:
        stock_code, ma_type, lookback_days, decision_days = prompt_user()
    except Exception as e:
        print(f"\n輸入錯誤：{e}")
        input("\nPress any key to continue . . .")
        return

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        print("\n[執行中] 計算趨勢並更新資料庫...\n")
        execute_sp(cursor, stock_code, ma_type, lookback_days, decision_days)
        conn.commit()

        window_rows = fetch_latest_window(cursor, stock_code, ma_type, lookback_days)
        analysis = analyze_window(window_rows, stock_code, ma_type, lookback_days, decision_days)

        if analysis is None:
            print("查無可分析資料，請確認股票代碼與均線欄位是否有資料。")
            input("\nPress any key to continue . . .")
            return

        print("[計算摘要]")
        print(json.dumps(analysis, ensure_ascii=False, indent=4))
        print()

        latest_rows = fetch_latest_10(cursor, stock_code, ma_type)

        print("[趨勢更新後資料表結果 - 最近幾筆]:\n")
        for row in latest_rows:
            date_str = row.Date.strftime("%Y-%m-%d")
            ma_value = "NULL" if row.MAValue is None else f"{float(row.MAValue):.2f}"
            trend_text = row.Trend if row.Trend else "NULL"
            print(
                f"日期: {date_str}, 股票: {row.StockCode}, {ma_type}: {ma_value}, 趨勢: {trend_text}"
            )

    except Exception as e:
        print(f"\n執行失敗：{e}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

    input("\nPress any key to continue . . .")


if __name__ == "__main__":
    main()
