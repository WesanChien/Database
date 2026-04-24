import json
from decimal import Decimal
import pyodbc

SERVER = "localhost,14330"
DATABASE = "TAtest"
USERNAME = "wesan"
PASSWORD = "wesan0213"

SP_NAME = "dbo.sp_DetectGranvilleRule1And5"


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


def normalize_value(value):
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, Decimal):
        return float(value)
    return value


def fetch_results(cursor, stock_code):
    cursor.execute(f"EXEC {SP_NAME} ?", stock_code)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    results = []
    for row in rows:
        item = {}
        for idx, col in enumerate(columns):
            item[col] = normalize_value(row[idx])
        results.append(item)
    return results


def print_recent_signals(results):
    print("[最近 10 筆訊號]:")
    if not results:
        print("查無符合條件的突破 / 跌破訊號。")
        return

    for item in results[:10]:
        signal = item["buy_or_sell"]
        arrow = f"{item['PreviousTrend']} → {item['CurrentTrend']}"
        relation = ">" if item["Close"] > item["MA20"] else "<"
        print(
            f"日期: {item['SignalDate']}, "
            f"股票: {item['StockCode']}, "
            f"收盤: {item['Close']} {relation} MA20: {item['MA20']} | "
            f"趨勢: {arrow} | "
            f"訊號: {signal}"
        )


def main():
    stock_code = input("請輸入股票代碼: ").strip()

    if not stock_code:
        print("股票代碼不可為空。")
        input("Press any key to continue . . .")
        return

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        print("\n[執行中] 葛蘭碧突破 / 跌破法則偵測中...\n")

        results = fetch_results(cursor, stock_code)

        print("[分析結果]:\n")
        print(json.dumps(results[:10], ensure_ascii=False, indent=4))
        print()

        print_recent_signals(results)

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
