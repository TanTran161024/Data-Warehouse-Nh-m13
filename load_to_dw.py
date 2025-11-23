import mysql.connector
import os
from mysql.connector import Error

# ==================== CẤU HÌNH KẾT NỐI ====================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",         
    "charset": "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
    "autocommit": False      # để cho phép rollback khi có lỗi
}

# Các file SQL cần chạy
SQL_DW_SCHEMA = "dataWarehouse/db_dw_setup.sql"      # Tạo DB + bảng dim/fact
SQL_PROCEDURE = "dataWarehouse/sp_load_dw.sql"       # Stored Procedure chính

# ====================================================================
# HÀM ĐỌC VÀ THỰC THI 1 FILE SQL (dùng cho schema & stored procedure)
# ====================================================================
def execute_sql_file(cursor, filepath, desc=""):
    """
    Đọc toàn bộ nội dung file SQL và xử lý để chạy từng câu lệnh một.
    Hỗ trợ DELIMITER khi file SQL chứa Stored Procedure.

    cursor : cursor MySQL đang kết nối
    filepath : đường dẫn file SQL
    desc : mô tả để in log
    """
    if not os.path.exists(filepath):
        print(f"Không tìm thấy file: {filepath}")
        return False

    print(f"Đang thực thi {desc}: {filepath} ...")
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    # Tách câu lệnh SQL nếu file có chứa DELIMITER (dùng trong stored procedure)
    statements = []
    current = ""
    delimiter = ";"  # delimiter mặc định

    # Duyệt từng dòng để gom thành các câu SQL hoàn chỉnh
    for line in sql.splitlines():
        line = line.strip()

        # Nếu gặp dòng đổi delimiter: DELIMITER $$
        if line.upper().startswith("DELIMITER"):
            # Lưu lại câu SQL đang chưa đóng nếu có
            if current:
                statements.append((delimiter, current))
                current = ""

            delimiter = line.split()[-1]  # lấy $$ hoặc //
        else:
            # Tiếp tục gom câu SQL
            current += line + "\n"

            # Nếu kết thúc bằng delimiter hiện tại → hoàn thành câu SQL
            if line.endswith(delimiter):
                statements.append((delimiter, current.strip()))
                current = ""

    # Trường hợp còn dư câu SQL cuối cùng
    if current.strip():
        statements.append((delimiter, current.strip()))

    # Thực thi các câu SQL đã gom
    for delim, stmt in statements:

        # Một số câu có nhiều phần tách ra theo delimiter
        for part in [s.strip() for s in stmt.split(delim) if s.strip()]:

            # Nếu vô tình chứa DELIMITER nữa thì bỏ qua
            if part.upper().startswith("DELIMITER"):
                continue

            try:
                cursor.execute(part)  # chạy câu SQL
            except Error as e:
                # Bỏ qua lỗi table exists
                if "already exists" not in str(e).lower():
                    print(f"Lỗi SQL trong {filepath}: {e}")
                    print(f"   Câu lệnh lỗi: {part[:200]}...")
                    raise  # bắn lỗi ra ngoài để rollback
    return True


# ====================================================================
# HÀM MAIN: CHẠY TOÀN BỘ QUY TRÌNH LOAD DATA WAREHOUSE
# ====================================================================
def main():
    print("BẮT ĐẦU LOAD DATA WAREHOUSE (Staging → DW)\n")

    try:
        # -------------------------------------------------------------
        # 1. KẾT NỐI MYSQL
        # -------------------------------------------------------------
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Kết nối MySQL thành công!")

        # -------------------------------------------------------------
        # 2. CHẠY FILE TẠO DB + BẢNG DIM/FACT (Star Schema)
        # -------------------------------------------------------------
        execute_sql_file(cursor, SQL_DW_SCHEMA, "Tạo Data Warehouse schema")
        conn.commit()
        print("Data Warehouse schema đã sẵn sàng.\n")

        # -------------------------------------------------------------
        # 3. TẠO OR UPDATE STORED PROCEDURE sp_load_dw
        # -------------------------------------------------------------
        execute_sql_file(cursor, SQL_PROCEDURE, "Tạo Stored Procedure sp_load_dw")
        conn.commit()
        print("Stored Procedure sp_load_dw đã sẵn sàng.\n")

        # -------------------------------------------------------------
        # 4. CHẠY PROCEDURE ĐỂ LOAD DỮ LIỆU (ETL thực sự)
        # -------------------------------------------------------------
        print("BẮT ĐẦU CHUYỂN ĐỔI DỮ LIỆU TỪ STAGING → DATA WAREHOUSE...")
        print("   (Có thể mất vài phút nếu dữ liệu lớn)\n")

        cursor.execute("USE bonbanh_datawarehouse")
        cursor.callproc("sp_load_dw")  # chạy procedure ETL chính

        conn.commit()
        print("HOÀN TẤT! Toàn bộ dữ liệu đã được load vào Data Warehouse (Star Schema)")

        # -------------------------------------------------------------
        # 5. THỐNG KÊ SỐ DÒNG ĐÃ LOAD
        # -------------------------------------------------------------
        cursor.execute("SELECT COUNT(*) FROM fact_danh_sach_xe")
        fact_count = cursor.fetchone()[0]
        print(f"   → fact_danh_sach_xe: {fact_count:,} bản ghi")

        cursor.execute("SELECT COUNT(*) FROM dim_mau_xe")
        print(f"   → dim_mau_xe: {cursor.fetchone()[0]:,} bản ghi")

        cursor.execute("SELECT COUNT(*) FROM dim_vi_tri")
        print(f"   → dim_vi_tri: {cursor.fetchone()[0]:,} bản ghi")

    except Error as e:
        print(f"LỖI KẾT NỐI HOẶC THỰC THI: {e}")
        # rollback nếu có lỗi trong quá trình chạy
        if 'conn' in locals():
            conn.rollback()

    finally:
        # -------------------------------------------------------------
        # 6. ĐÓNG KẾT NỐI MYSQL
        # -------------------------------------------------------------
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

        print("\nĐóng kết nối MySQL.")

    print("\nQUY TRÌNH HOÀN TẤT! Bạn có thể query DW bằng câu lệnh:")
    print("   USE bonbanh_datawarehouse;")
    print("   SELECT * FROM fact_danh_sach_xe LIMIT 5;")


# ====================================================================
# CHẠY CHƯƠNG TRÌNH
# ====================================================================
if __name__ == "__main__":
    main()
