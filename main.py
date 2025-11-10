def write_snapshot_to_firestore(prod_id, now_local, month_sales, sold_num, member_stocks):
    """
    路径: logs/{prod_id}/members/{member_name}/snapshots/{auto_id}
    - 若 stocks 未变（unit_sales=0）：更新最近一条快照 time / time_utc（不新建）
    - 若 stocks 变化：新建快照，并计算 unit_sales = last_stocks - stocks
    - 同时：占位创建父文档（以及成员文档，可选），确保前端能列出该 prod_id
    """
    db = init_firebase()
    batch = db.batch()

    # ☆☆☆ 关键新增：占位创建父文档，保证 collection('logs') 能看到这个 prod_id
    parent_ref = db.collection("logs").document(str(prod_id))
    batch.set(parent_ref, {"_exists": True, "updated_at": SERVER_TIMESTAMP}, merge=True)

    for member_name, stocks in member_stocks.items():
        stocks = int(stocks)

        # （可选但推荐）占位创建成员文档，保证 collectionGroup('members') 能稳定反推 prod_id
        member_ref = parent_ref.collection("members").document(member_name)
        batch.set(member_ref, {"_exists": True, "updated_at": SERVER_TIMESTAMP}, merge=True)

        # 最近一条快照
        last_ref, last_data = get_last_snapshot(db, prod_id, member_name)
        last_stocks = int(last_data.get("stocks")) if last_data and "stocks" in last_data else None

        if last_stocks is not None and last_stocks == stocks:
            # 库存没变：只更新时间相关字段（不新建文档）
            update_fields = {
                "time": now_local,
                "time_utc": SERVER_TIMESTAMP,
                "month_sales": int(month_sales),
                "sold_num": int(sold_num),
            }
            batch.update(last_ref, update_fields)
            print("Updated (no change):", f"logs/{prod_id}/members/{member_name}/snapshots/{last_ref.id}")
        else:
            # 库存变化：新建一条快照
            unit_sales = None
            if last_stocks is not None:
                unit_sales = last_stocks - stocks  # 正数=卖出

            snap_ref = member_ref.collection("snapshots").document()
            batch.set(snap_ref, {
                "prod_id": str(prod_id),
                "member_name": member_name,
                "time": now_local,
                "time_utc": SERVER_TIMESTAMP,
                "month_sales": int(month_sales),
                "sold_num": int(sold_num),
                "stocks": stocks,
                "unit_sales": int(unit_sales) if isinstance(unit_sales, int) else None,
                "source": "kmstation",
            })
            print("Created (changed):", f"logs/{prod_id}/members/{member_name}/snapshots/{snap_ref.id}")

    batch.commit()
