import os
import re
import time
import traceback
from datetime import datetime

import requests
import argparse


from config import prod_ids, interval_seconds

# ===== Firestore =====
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

LOG_FILE = "monitor_log.txt"
base_dir = os.path.dirname(os.path.abspath(__file__))
sa_path = os.path.join(base_dir, "serviceAccountKey.json")

# 新增：单次执行
def tick_once():
    for pid in prod_ids:
        record_data(pid)
# ---------------- Utils ----------------
def log_message(message):
    print(message)
    try:
        with open(LOG_FILE, mode='a', encoding='utf-8') as f:
            f.write(message + "\n")
    except Exception:
        # 避免日志写入异常影响主流程
        pass

def extract_member_name(sku_name):
    clean_name = sku_name.strip().replace('【', '').replace('】', '')
    match_eng = re.search(r'([A-Z]{2,})$', clean_name)
    if match_eng:
        return match_eng.group(1)
    match_cn = re.search(r'([\u4e00-\u9fa5]{2,4})$', clean_name)
    if match_cn:
        return match_cn.group(1)
    return "UNKNOWN"

# ---------------- Firestore init ----------------
_db = None
def init_firebase():
    """
    优先读取 GOOGLE_APPLICATION_CREDENTIALS 指向的 json 文件；
    若未设置，则默认使用 ./serviceAccountKey.json
    """
    global _db
    if _db is not None:
        return _db

    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "serviceAccountKey.json")
    cred = credentials.Certificate(sa_path)
    firebase_admin.initialize_app(cred)
    _db = firestore.client()
    return _db

def get_last_stocks(db, prod_id, member_name):
    """
    读取该 member 最近一次快照的 stocks；若没有则返回 None
    """
    try:
        ref = (db.collection("logs").document(str(prod_id))
                 .collection("members").document(member_name)
                 .collection("snapshots"))
        docs = ref.order_by("time_utc", direction=firestore.Query.DESCENDING).limit(1).stream()
        for d in docs:
            data = d.to_dict() or {}
            return int(data.get("stocks"))
    except Exception:
        # 没有索引/没有数据/其他异常都返回 None（首次写入）
        return None
    return None

# === 替换：获取最近快照（返回 doc_ref 和 data） ===
def get_last_snapshot(db, prod_id, member_name):
    """
    返回最近一条快照的 (doc_ref, data)；若没有则返回 (None, None)
    """
    try:
        ref = (db.collection("logs").document(str(prod_id))
                 .collection("members").document(member_name)
                 .collection("snapshots"))
        docs = ref.order_by("time_utc", direction=firestore.Query.DESCENDING).limit(1).stream()
        for d in docs:
            return d.reference, (d.to_dict() or {})
    except Exception:
        pass
    return None, None

# === 替换：写入逻辑，unit_sales=0 时仅更新旧快照的时间戳 ===
def write_snapshot_to_firestore(prod_id, now_local, month_sales, sold_num, member_stocks):
    """
    路径: logs/{prod_id}/members/{member_name}/snapshots/{auto_id}
    - 若 stocks 未变（unit_sales=0）：更新最近一条快照的 time / time_utc（不新建）
    - 若 stocks 变化：新建一条快照，并计算 unit_sales = last_stocks - stocks
    """
    db = init_firebase()
    batch = db.batch()

    for member_name, stocks in member_stocks.items():
        stocks = int(stocks)

        last_ref, last_data = get_last_snapshot(db, prod_id, member_name)
        last_stocks = int(last_data.get("stocks")) if last_data and "stocks" in last_data else None

        if last_stocks is not None and last_stocks == stocks:
            # ---- 库存没变：更新旧快照的时间戳（以及你想同步刷新的字段）----
            # 仅更新时间字段；如需顺带记录最新的 month_sales / sold_num，也可一并更新：
            update_fields = {
                "time": now_local,
                "time_utc": SERVER_TIMESTAMP,
                "month_sales": int(month_sales),
                "sold_num": int(sold_num),
                # stocks 不变，不改；unit_sales 也不改（仍为上次计算值或 None）
            }
            batch.update(last_ref, update_fields)
            print("Updated (no change):", f"logs/{prod_id}/members/{member_name}/snapshots/{last_ref.id}")
        else:
            # ---- 库存变化：新建快照，写入 unit_sales ----
            unit_sales = None
            if last_stocks is not None:
                unit_sales = last_stocks - stocks  # 正数=卖出

            doc = {
                "prod_id": str(prod_id),
                "member_name": member_name,
                "time": now_local,
                "time_utc": SERVER_TIMESTAMP,      # 服务端时间戳，便于排序
                "month_sales": int(month_sales),
                "sold_num": int(sold_num),
                "stocks": stocks,
                "unit_sales": int(unit_sales) if isinstance(unit_sales, int) else None,
                "source": "kmstation",
            }

            ref = (db.collection("logs").document(str(prod_id))
                     .collection("members").document(member_name)
                     .collection("snapshots").document())
            batch.set(ref, doc)
            print("Created (changed):", f"logs/{prod_id}/members/{member_name}/snapshots/{ref.id}")

    batch.commit()


# ---------------- Crawler ----------------
def record_data(prod_id):
    try:
        url = f"https://kms.kmstation.net/prod/prodInfo?prodId={prod_id}"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        now_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        month_sales = data.get("monthSales", 0)
        sold_num = data.get("soldNum", 0)
        sku_list = data.get("skuList", []) or []

        member_stocks = {}
        for sku in sku_list:
            sku_name = sku.get("skuName", "") or ""
            stocks = int(sku.get("stocks", 0) or 0)
            member_name = extract_member_name(sku_name)
            member_stocks[member_name] = member_stocks.get(member_name, 0) + stocks

        # 写入 Firestore
        write_snapshot_to_firestore(prod_id, now_local, month_sales, sold_num, member_stocks)

        log_message(f"[{now_local}] Firestore wrote {len(member_stocks)} members for prodId {prod_id}.")
    except Exception as e:
        now_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message(f"[{now_local}] Error when recording prodId {prod_id}: {e}")
        traceback.print_exc()
        
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run one tick and exit")
    args = parser.parse_args()

    if args.once:
        tick_once()           # 只跑一轮
    else:
        # 原来的常驻循环保留用于本地/VPS
        while True:
            tick_once()
            time.sleep(interval_seconds)

if __name__ == "__main__":
    main()