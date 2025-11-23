import pandas as pd
import mysql.connector
import math
import os
from mysql.connector import Error
from datetime import datetime

# Cấu hình kết nối MySQL KHÔNG kèm database (dùng để tạo DB nếu chưa có)
DB_CONFIG_NO_DB = {
    "host": "localhost",
    "user": "root",
    "password": "",         
    "charset": "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
    "autocommit": True
}

# Cấu hình kết nối MySQL KÈM database bonbanh_staging
DB_CONFIG = {
    **DB_CONFIG_NO_DB,
    "database": "bonbanh_staging"
}

# Tạo tên file CSV theo ngày hiện tại
# Ví dụ: data/bonbanh_raw_2025-11-23.csv
today_str = datetime.now().strftime("%Y-%m-%d")
CSV_FILE = f"data/bonbanh_raw_{today_str}.csv"

# Nếu chưa có file CSV thì báo lỗi
if not os.path.exists(CSV_FILE):
    print(f"Chưa có dữ liệu ngày hôm nay: {CSV_FILE}")
    print("Chạy: python get_data.py trước!")
    exit()

# File chứa lệnh SQL khởi tạo database + bảng
SQL_SCHEMA_FILE = "staging/bonbanh_staging.sql"
# File chứa stored procedure transform
SQL_SP_FILE = "staging/transform.sql"


def fix_nan(x):
    """
    Hàm xử lý giá trị bị NaN hoặc None trong pandas.
    Trả về chuỗi rỗng nếu là NaN, ngược lại trả về chuỗi strip().
    """
    return "" if (x is None or (isinstance(x, float) and math.isnan(x))) else str(x).strip()


def db_exists(cursor, db_name="bonbanh_staging"):
    """
    Kiểm tra database có tồn tại chưa.
    SELECT database theo tên.
    Trả về True nếu có, False nếu không.
    """
    cursor.execute("SHOW DATABASES LIKE %s", (db_name,))
    return cursor.fetchone() is not None


def table_exists(cursor):
    """
    Kiểm tra bảng xe_bonbanh đã tồn tại trong database chưa.
    Dựa vào information_schema.TABLES.
    """
    cursor.execute("""
        SELECT 1 FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = 'bonbanh_staging' AND TABLE_NAME = 'xe_bonbanh'
    """)
    return cursor.fetchone() is not None


def procedure_exists(cursor):
    """
    Kiểm tra stored procedure sp_transform_row đã tồn tại.
    Dựa vào information_schema.ROUTINES.
    """
    cursor.execute("""
        SELECT 1 FROM information_schema.ROUTINES 
        WHERE ROUTINE_SCHEMA = 'bonbanh_staging' 
          AND ROUTINE_NAME = 'sp_transform_row' 
          AND ROUTINE_TYPE = 'PROCEDURE'
    """)
    return cursor.fetchone() is not None


def execute_sql_file(cursor, filepath):
    """
    Đọc file SQL và thực thi từng câu lệnh.
    Hỗ trợ xử lý DELIMITER $$ dành cho stored procedure.
    Trả về True nếu thực thi xong.
    """
    if not os.path.exists(filepath):
        print(f"Không tìm thấy file: {filepath}")
        return False

    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    statements = []
    current_stmt = ""
    current_delim = ";"

    # Tách các lệnh SQL theo delimiter
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

    # Thực thi từng phần
    for delim, stmt in statements:
        for part in [s.strip() for s in stmt.split(delim) if s.strip()]:
            if part.upper().startswith("DELIMITER"):
                continue
            try:
                cursor.execute(part)
            except Error as e:
                if "already exists" not in str(e).lower():
                    print(f"Lỗi SQL: {e}")
    return True


def init_database():
    """
    Bước khởi tạo database:
    1. Kết nối MySQL không DB → kiểm tra database bonbanh_staging
    2. Nếu chưa có DB → chạy file SQL để tạo
    3. Kết nối lại với DB → kiểm tra bảng, stored procedure
    4. Tạo bảng hoặc SP nếu chưa có
    """
    print("Bước 1: Kiểm tra và khởi tạo database...")

    # Kết nối MySQL không database
    conn = mysql.connector.connect(**DB_CONFIG_NO_DB)
    cursor = conn.cursor()

    # Tạo DB nếu chưa có
    if not db_exists(cursor):
        print("→ Database 'bonbanh_staging' chưa tồn tại → đang tạo...")
        execute_sql_file(cursor, SQL_SCHEMA_FILE)
    else:
        print("→ Database 'bonbanh_staging' đã tồn tại.")

    cursor.close()
    conn.close()

    # Kết nối lại để kiểm tra bảng + SP
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Kiểm tra bảng
    if not table_exists(cursor):
        print("→ Bảng 'xe_bonbanh' chưa tồn tại → đang tạo...")
        execute_sql_file(cursor, SQL_SCHEMA_FILE)
    else:
        print("→ Bảng 'xe_bonbanh' đã tồn tại.")

    # Kiểm tra stored procedure
    if not procedure_exists(cursor):
        print("→ Stored Procedure chưa tồn tại → đang tạo...")
        execute_sql_file(cursor, SQL_SP_FILE)
    else:
        print("→ Stored Procedure 'sp_transform_row' đã tồn tại.")

    conn.commit()
    cursor.close()
    conn.close()
    print("Khởi tạo database hoàn tất!\n")


def main():
    """
    Hàm chính của ETL:
    - Gọi init_database để chuẩn bị DB
    - Đọc file CSV theo ngày
    - Duyệt từng dòng → gọi stored procedure sp_transform_row để xử lý + insert
    - Commit sau mỗi 100 dòng để tăng tốc
    - In log lỗi nếu dòng nào lỗi
    """
    init_database()

    if not os.path.exists(CSV_FILE):
        print(f"Không tìm thấy file CSV: {CSV_FILE}")
        print("Chạy lệnh: python get_data.py")
        return

    print(f"Đang đọc {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE, encoding="utf-8-sig", dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    total = len(df)
    cnt = 0

    print(f"Bắt đầu đẩy {total} bản ghi vào MySQL...\n")

    for idx, row in df.iterrows():
        # Tạo tuple tham số truyền vào SP
        params = (
            row.get("Loại xe + Năm SX", ""),
            row.get("Tên xe", ""),
            row.get("Giá xe_raw", ""),
            row.get("Nơi bán", ""),
            row.get("Liên hệ", ""),
            row.get("Link xe", ""),
            row.get("Ngày đăng", ""),
            row.get("Lượt xem", ""),
            row.get("Số Km đã đi:", ""),
            row.get("Tình trạng:", ""),
            row.get("Xuất xứ:", ""),
            row.get("Kiểu dáng:", ""),
            row.get("Động cơ:", ""),
            row.get("Màu ngoại thất:", ""),
            row.get("Màu nội thất:", ""),
            row.get("Số chỗ ngồi:", ""),
            row.get("Số cửa:", ""),
            row.get("Năm sản xuất:", "")
        )

        try:
            cursor.execute("CALL sp_transform_row(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", params)
            cnt += 1
            if cnt % 100 == 0:
                conn.commit()
                print(f"   Đã đẩy {cnt}/{total}...")
        except Error as e:
            print(f"Lỗi dòng {idx+2}: {e}")
            print(f"   Link: {row.get('Link xe', 'N/A')}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\nHOÀN TẤT! Đã xử lý {cnt}/{total} bản ghi thành công!")


if __name__ == "__main__":
    main()
