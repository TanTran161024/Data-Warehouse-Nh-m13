import mysql.connector
import os
from mysql.connector import Error
from datetime import date
import logging
import config  # Import config file

# ===========================
# Cấu hình logger
# ===========================
LOG_FILE = config.get_log_file("load_datamart")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("LoadDataMartLogger")

# ================================================================
#              CẤU HÌNH KẾT NỐI MYSQL
# ================================================================
DB_CONFIG = config.DB_CONFIG_BASE

SQL_DATAMART_SCHEMA = config.DATAMART_SQL_SCHEMA_FILE

# ================================================================
#              HÀM THỰC THI FILE SQL ĐƠN GIẢN (không DELIMITER)
# ================================================================
def execute_sql_file(cursor, filepath, desc=""):
    if not os.path.exists(filepath):
        logger.error("Không tìm thấy file: %s", filepath)
        return False

    logger.info("Đang thực thi %s: %s ...", desc, filepath)

    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    stmts = [s.strip() for s in sql.split(';') if s.strip()]

    for stmt in stmts:
        try:
            cursor.execute(stmt)
        except Error as e:
            lower = str(e).lower()
            if "already exists" in lower or "duplicate" in lower:
                continue
            logger.error("Lỗi thực thi SQL: %s", e)
            logger.error("Câu lệnh lỗi (rút gọn): %s", stmt[:200])
            raise

    return True

# ================================================================
#            HÀM REFRESH DATAMART (DW → DataMart)
# ================================================================
def refresh_datamart(conn, cursor):
    logger.info("Truncating DataMart tables (refresh)...")

    tables = [
        "agg_price_by_make",
        "agg_count_by_location",
        "agg_views_by_day",
        "agg_price_bucket",
        "top_listings_by_views"
    ]

    for t in tables:
        cursor.execute(f"TRUNCATE TABLE bonbanh_datamart.{t}")

    logger.info("Populating agg_price_by_make ...")
    cursor.execute("""
        INSERT INTO bonbanh_datamart.agg_price_by_make
        (ten_xe, nam_san_xuat, listings_count, avg_price, min_price, max_price)
        SELECT
            COALESCE(dm.ten_xe, 'Unknown') AS ten_xe,
            COALESCE(dm.nam_san_xuat, 0) AS nam_san_xuat,
            COUNT(f.id) AS listings_count,
            AVG(f.gia_xe) AS avg_price,
            MIN(f.gia_xe) AS min_price,
            MAX(f.gia_xe) AS max_price
        FROM bonbanh_datawarehouse.fact_danh_sach_xe f
        LEFT JOIN bonbanh_datawarehouse.dim_mau_xe dm 
            ON f.mau_xe_sk = dm.surrogate_key
        GROUP BY COALESCE(dm.ten_xe, 'Unknown'), COALESCE(dm.nam_san_xuat, 0)
        ORDER BY listings_count DESC
    """)

    logger.info("Populating agg_count_by_location ...")
    cursor.execute("""
        INSERT INTO bonbanh_datamart.agg_count_by_location
        (noi_ban, listings_count, avg_price)
        SELECT
            COALESCE(dv.noi_ban, 'Unknown') AS noi_ban,
            COUNT(f.id) AS listings_count,
            AVG(f.gia_xe) AS avg_price
        FROM bonbanh_datawarehouse.fact_danh_sach_xe f
        LEFT JOIN bonbanh_datawarehouse.dim_vi_tri dv 
            ON f.vi_tri_sk = dv.surrogate_key
        GROUP BY COALESCE(dv.noi_ban, 'Unknown')
        ORDER BY listings_count DESC
    """)

    logger.info("Populating agg_views_by_day ...")
    cursor.execute("""
        INSERT INTO bonbanh_datamart.agg_views_by_day
        (ngay, total_listings, total_views, avg_views_per_listing)
        SELECT
            f.ngay_dang AS ngay,
            COUNT(f.id) AS total_listings,
            SUM(COALESCE(f.luot_xem,0)) AS total_views,
            CASE 
                WHEN COUNT(f.id)=0 THEN 0 
                ELSE SUM(COALESCE(f.luot_xem,0))/COUNT(f.id) 
            END AS avg_views_per_listing
        FROM bonbanh_datawarehouse.fact_danh_sach_xe f
        GROUP BY f.ngay_dang
        ORDER BY f.ngay_dang DESC
    """)

    logger.info("Populating agg_price_bucket ...")
    cursor.execute("""
        INSERT INTO bonbanh_datamart.agg_price_bucket
        (bucket_label, bucket_min, bucket_max, listings_count, avg_price)
        SELECT
            CASE
                WHEN f.gia_xe < 200000000 THEN '<200M'
                WHEN f.gia_xe BETWEEN 200000000 AND 500000000 THEN '200-500M'
                WHEN f.gia_xe BETWEEN 500000000 AND 1000000000 THEN '500M-1T'
                WHEN f.gia_xe >= 1000000000 THEN '>1T'
                ELSE 'Unknown'
            END AS bucket_label,
            CASE
                WHEN f.gia_xe < 200000000 THEN 0
                WHEN f.gia_xe BETWEEN 200000000 AND 500000000 THEN 200000000
                WHEN f.gia_xe BETWEEN 500000000 AND 1000000000 THEN 500000000
                WHEN f.gia_xe >= 1000000000 THEN 1000000000
                ELSE NULL
            END AS bucket_min,
            CASE
                WHEN f.gia_xe < 200000000 THEN 199999999
                WHEN f.gia_xe BETWEEN 200000000 AND 500000000 THEN 500000000
                WHEN f.gia_xe BETWEEN 500000000 AND 1000000000 THEN 1000000000
                WHEN f.gia_xe >= 1000000000 THEN NULL
                ELSE NULL
            END AS bucket_max,
            COUNT(*) AS listings_count,
            AVG(f.gia_xe) AS avg_price
        FROM bonbanh_datawarehouse.fact_danh_sach_xe f
        GROUP BY bucket_label
        ORDER BY listings_count DESC
    """)

    logger.info("Populating top_listings_by_views (top 100)...")
    cursor.execute("""
        INSERT INTO bonbanh_datamart.top_listings_by_views
        (source_fact_id, ten_xe, gia_xe, luot_xem, ngay_dang, noi_ban, link_xe)
        SELECT
            f.id AS source_fact_id,
            COALESCE(dm.ten_xe, '') AS ten_xe,
            f.gia_xe,
            COALESCE(f.luot_xem, 0) AS luot_xem,
            f.ngay_dang,
            COALESCE(dv.noi_ban, '') AS noi_ban,
            f.link_xe
        FROM bonbanh_datawarehouse.fact_danh_sach_xe f
        LEFT JOIN bonbanh_datawarehouse.dim_mau_xe dm 
            ON f.mau_xe_sk = dm.surrogate_key
        LEFT JOIN bonbanh_datawarehouse.dim_vi_tri dv 
            ON f.vi_tri_sk = dv.surrogate_key
        ORDER BY COALESCE(f.luot_xem,0) DESC
        LIMIT 100
    """)

    conn.commit()

# =================================================================
#                       HÀM MAIN
# =================================================================
def main():
    logger.info("BẮT ĐẦU LOAD DATA MART (DW -> DataMart)")

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logger.info("Kết nối MySQL thành công!")

        execute_sql_file(cursor, SQL_DATAMART_SCHEMA, "Tạo DataMart schema")
        conn.commit()
        logger.info("DataMart schema đã sẵn sàng.")

        refresh_datamart(conn, cursor)

        # Thống kê số bản ghi
        cursor.execute("SELECT COUNT(*) FROM bonbanh_datamart.agg_price_by_make")
        count1 = cursor.fetchone()[0]
        logger.info(f"   → agg_price_by_make: {count1:,} bản ghi")

        cursor.execute("SELECT COUNT(*) FROM bonbanh_datamart.agg_count_by_location")
        count2 = cursor.fetchone()[0]
        logger.info(f"   → agg_count_by_location: {count2:,} bản ghi")

        cursor.execute("SELECT COUNT(*) FROM bonbanh_datamart.top_listings_by_views")
        count3 = cursor.fetchone()[0]
        logger.info(f"   → top_listings_by_views: {count3:,} bản ghi")

        logger.info("HOÀN TẤT! DataMart đã được cập nhật.")

    except Error as e:
        logger.error("LỖI KẾT NỐI HOẶC THỰC THI: %s", e)
        if 'conn' in locals():
            conn.rollback()

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
        logger.info("Đóng kết nối MySQL.")

# =================================================================
if __name__ == "__main__":
    main()