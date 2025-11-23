import mysql.connector
import os
from mysql.connector import Error
import logging

# ===========================
# Cấu hình logger
# ===========================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
from datetime import datetime
today_str_log = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = os.path.join(LOG_DIR, f"load_to_dw_{today_str_log}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("LoadDWLogger")

# ===========================
# Cấu hình MySQL + file SQL
# ===========================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",         
    "charset": "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
    "autocommit": False
}

SQL_DW_SCHEMA = "dataWarehouse/db_dw_setup.sql"
SQL_PROCEDURE = "dataWarehouse/sp_load_dw.sql"

# ===========================
# Hàm đọc và chạy SQL file
# ===========================
def execute_sql_file(cursor, filepath, desc=""):
    if not os.path.exists(filepath):
        logger.error("Không tìm thấy file: %s", filepath)
        return False

    logger.info("Đang thực thi %s: %s ...", desc, filepath)
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    statements = []
    current = ""
    delimiter = ";"

    for line in sql.splitlines():
        line = line.strip()
        if line.upper().startswith("DELIMITER"):
            if current:
                statements.append((delimiter, current))
                current = ""
            delimiter = line.split()[-1]
        else:
            current += line + "\n"
            if line.endswith(delimiter):
                statements.append((delimiter, current.strip()))
                current = ""

    if current.strip():
        statements.append((delimiter, current.strip()))

    for delim, stmt in statements:
        for part in [s.strip() for s in stmt.split(delim) if s.strip()]:
            if part.upper().startswith("DELIMITER"):
                continue
            try:
                cursor.execute(part)
            except Error as e:
                if "already exists" not in str(e).lower():
                    logger.error("Lỗi SQL trong %s: %s", filepath, e)
                    logger.error("   Câu lệnh lỗi: %s...", part[:200])
                    raise
    return True

# ===========================
# Hàm main
# ===========================
def main():
    logger.info("BẮT ĐẦU LOAD DATA WAREHOUSE (Staging → DW)\n")

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logger.info("Kết nối MySQL thành công!")

        # 1. Chạy file schema
        execute_sql_file(cursor, SQL_DW_SCHEMA, "Tạo Data Warehouse schema")
        conn.commit()
        logger.info("Data Warehouse schema đã sẵn sàng.\n")

        # 2. Tạo/Update stored procedure
        execute_sql_file(cursor, SQL_PROCEDURE, "Tạo Stored Procedure sp_load_dw")
        conn.commit()
        logger.info("Stored Procedure sp_load_dw đã sẵn sàng.\n")

        # 3. Chạy procedure ETL
        logger.info("BẮT ĐẦU CHUYỂN ĐỔI DỮ LIỆU TỪ STAGING → DATA WAREHOUSE...")
        logger.info("   (Có thể mất vài phút nếu dữ liệu lớn)\n")

        cursor.execute("USE bonbanh_datawarehouse")
        cursor.callproc("sp_load_dw")
        conn.commit()
        logger.info("HOÀN TẤT! Toàn bộ dữ liệu đã được load vào Data Warehouse (Star Schema)")

        # 4. Thống kê số dòng
        cursor.execute("SELECT COUNT(*) FROM fact_danh_sach_xe")
        fact_count = cursor.fetchone()[0]
        logger.info("   → fact_danh_sach_xe: %s bản ghi", f"{fact_count:,}")

        cursor.execute("SELECT COUNT(*) FROM dim_mau_xe")
        logger.info("   → dim_mau_xe: %s bản ghi", f"{cursor.fetchone()[0]:,}")

        cursor.execute("SELECT COUNT(*) FROM dim_vi_tri")
        logger.info("   → dim_vi_tri: %s bản ghi", f"{cursor.fetchone()[0]:,}")

    except Error as e:
        logger.error("LỖI KẾT NỐI HOẶC THỰC THI: %s", e)
        if 'conn' in locals():
            conn.rollback()

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
        logger.info("\nĐóng kết nối MySQL.")

    logger.info("\nQUY TRÌNH HOÀN TẤT! Bạn có thể query DW bằng câu lệnh:")
    logger.info("   USE bonbanh_datawarehouse;")
    logger.info("   SELECT * FROM fact_danh_sach_xe LIMIT 5;")

# ===========================
if __name__ == "__main__":
    main()
