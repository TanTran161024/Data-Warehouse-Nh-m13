import pandas as pd
import mysql.connector
import math
import os
from mysql.connector import Error
from datetime import datetime
import logging

# ===========================
# Cấu hình logger
# ===========================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
today_str_log = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = os.path.join(LOG_DIR, f"load_to_staging_{today_str_log}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("LoadStagingLogger")

# ===========================
# Cấu hình DB + file CSV
# ===========================
DB_CONFIG_NO_DB = {
    "host": "localhost",
    "user": "root",
    "password": "",         
    "charset": "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
    "autocommit": True
}

DB_CONFIG = {**DB_CONFIG_NO_DB, "database": "bonbanh_staging"}

today_str = datetime.now().strftime("%Y-%m-%d")
CSV_FILE = f"data/bonbanh_raw_{today_str}.csv"

if not os.path.exists(CSV_FILE):
    logger.error("Chưa có dữ liệu ngày hôm nay: %s", CSV_FILE)
    logger.info("Chạy: python get_data.py trước!")
    exit()

SQL_SCHEMA_FILE = "staging/bonbanh_staging.sql"
SQL_SP_FILE = "staging/transform.sql"

# ===========================
# Hàm tiện ích
# ===========================
def fix_nan(x):
    return "" if (x is None or (isinstance(x, float) and math.isnan(x))) else str(x).strip()

def db_exists(cursor, db_name="bonbanh_staging"):
    cursor.execute("SHOW DATABASES LIKE %s", (db_name,))
    return cursor.fetchone() is not None

def table_exists(cursor):
    cursor.execute("""
        SELECT 1 FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = 'bonbanh_staging' AND TABLE_NAME = 'xe_bonbanh'
    """)
    return cursor.fetchone() is not None

def procedure_exists(cursor):
    cursor.execute("""
        SELECT 1 FROM information_schema.ROUTINES 
        WHERE ROUTINE_SCHEMA = 'bonbanh_staging' 
          AND ROUTINE_NAME = 'sp_transform_row' 
          AND ROUTINE_TYPE = 'PROCEDURE'
    """)
    return cursor.fetchone() is not None

def execute_sql_file(cursor, filepath):
    if not os.path.exists(filepath):
        logger.error("Không tìm thấy file: %s", filepath)
        return False

    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    statements = []
    current_stmt = ""
    current_delim = ";"

    for line in sql.splitlines():
        line = line.strip()
        if line.upper().startswith("DELIMITER"):
            if current_stmt:
                statements.append((current_delim, current_stmt))
                current_stmt = ""
            current_delim = line.split()[-1]
        else:
            current_stmt += line + "\n"
            if line.endswith(current_delim):
                statements.append((current_delim, current_stmt.strip()))
                current_stmt = ""

    if current_stmt.strip():
        statements.append((current_delim, current_stmt.strip()))

    for delim, stmt in statements:
        for part in [s.strip() for s in stmt.split(delim) if s.strip()]:
            if part.upper().startswith("DELIMITER"):
                continue
            try:
                cursor.execute(part)
            except Error as e:
                if "already exists" not in str(e).lower():
                    logger.error("Lỗi SQL: %s", e)
    return True

# ===========================
# Khởi tạo DB
# ===========================
def init_database():
    logger.info("Bước 1: Kiểm tra và khởi tạo database...")

    conn = mysql.connector.connect(**DB_CONFIG_NO_DB)
    cursor = conn.cursor()

    if not db_exists(cursor):
        logger.info("→ Database 'bonbanh_staging' chưa tồn tại → đang tạo...")
        execute_sql_file(cursor, SQL_SCHEMA_FILE)
    else:
        logger.info("→ Database 'bonbanh_staging' đã tồn tại.")

    cursor.close()
    conn.close()

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    if not table_exists(cursor):
        logger.info("→ Bảng 'xe_bonbanh' chưa tồn tại → đang tạo...")
        execute_sql_file(cursor, SQL_SCHEMA_FILE)
    else:
        logger.info("→ Bảng 'xe_bonbanh' đã tồn tại.")

    if not procedure_exists(cursor):
        logger.info("→ Stored Procedure chưa tồn tại → đang tạo...")
        execute_sql_file(cursor, SQL_SP_FILE)
    else:
        logger.info("→ Stored Procedure 'sp_transform_row' đã tồn tại.")

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Khởi tạo database hoàn tất!\n")

# ===========================
# Hàm chính
# ===========================
def main():
    init_database()

    if not os.path.exists(CSV_FILE):
        logger.error("Không tìm thấy file CSV: %s", CSV_FILE)
        logger.info("Chạy lệnh: python get_data.py")
        return

    logger.info("Đang đọc %s...", CSV_FILE)
    df = pd.read_csv(CSV_FILE, encoding="utf-8-sig", dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    total = len(df)
    cnt = 0

    logger.info("Bắt đầu đẩy %d bản ghi vào MySQL...", total)

    for idx, row in df.iterrows():
        params = tuple(row.get(col, "") for col in [
            "Loại xe + Năm SX", "Tên xe", "Giá xe_raw", "Nơi bán", "Liên hệ", "Link xe",
            "Ngày đăng", "Lượt xem", "Số Km đã đi:", "Tình trạng:", "Xuất xứ:", "Kiểu dáng:",
            "Động cơ:", "Màu ngoại thất:", "Màu nội thất:", "Số chỗ ngồi:", "Số cửa:", "Năm sản xuất:"
        ])
        try:
            cursor.execute("CALL sp_transform_row(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", params)
            cnt += 1
            if cnt % 100 == 0:
                conn.commit()
                logger.info("   Đã đẩy %d/%d...", cnt, total)
        except Error as e:
            logger.error("Lỗi dòng %d: %s", idx+2, e)
            logger.error("   Link: %s", row.get("Link xe", "N/A"))

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("HOÀN TẤT! Đã xử lý %d/%d bản ghi thành công!", cnt, total)

# ===========================
if __name__ == "__main__":
    main()
