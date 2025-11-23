import mysql.connector
import os
from mysql.connector import Error
from datetime import date

# ================================================================
#              CẤU HÌNH KẾT NỐI MYSQL
# ================================================================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",                 # ← nếu MySQL có mật khẩu thì sửa tại đây
    "charset": "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
    "autocommit": False             # để hệ thống cho phép rollback khi lỗi
}

# File SQL dùng để tạo database + bảng DataMart
SQL_DATAMART_SCHEMA = "dataMart/db_datamart_setup.sql"


# ================================================================
#              HÀM THỰC THI FILE SQL ĐƠN GIẢN (không DELIMITER)
# ================================================================
def execute_sql_file(cursor, filepath, desc=""):
    """
    Hàm đọc file SQL schema và thực thi tuần tự từng câu lệnh.
    File schema không có Stored Procedure nên chỉ cần tách theo dấu ';'.
    
    cursor: con trỏ MySQL
    filepath: đường dẫn file SQL
    desc: mô tả để hiển thị log
    """
    if not os.path.exists(filepath):
        print(f"Không tìm thấy file: {filepath}")
        return False

    print(f"Đang thực thi {desc}: {filepath} ...")

    # Đọc toàn bộ nội dung SQL file
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    # Tách theo dấu ';'
    stmts = [s.strip() for s in sql.split(';') if s.strip()]

    # Thực thi từng câu SQL
    for stmt in stmts:
        try:
            cursor.execute(stmt)
        except Error as e:
            lower = str(e).lower()

            # Bỏ qua các lỗi: bảng đã tồn tại, duplicate
            if "already exists" in lower or "duplicate" in lower:
                continue

            print("Lỗi thực thi SQL:", e)
            print("Câu lệnh lỗi (rút gọn):", stmt[:200])
            raise  # bắn lỗi ra ngoài cho hàm main rollback

    return True


# =================================================================
#            HÀM REFRESH DATAMART (DW → DataMart)
# =================================================================
def refresh_datamart(conn, cursor):
    """
    Hàm refresh toàn bộ DataMart:
    - Xóa dữ liệu cũ (TRUNCATE)
    - Chạy lại toàn bộ các bảng aggregate mới từ DataWarehouse.
    """

    print("Truncating DataMart tables (refresh)...")

    # Danh sách bảng cần xoá dữ liệu trước khi nạp lại
    tables = [
        "agg_price_by_make",
        "agg_count_by_location",
        "agg_views_by_day",
        "agg_price_bucket",
        "top_listings_by_views"
    ]

    # TRUNCATE từng bảng
    for t in tables:
        cursor.execute(f"TRUNCATE TABLE bonbanh_datamart.{t}")

    # ===========================================================
    #         1) BẢNG TỔNG HỢP GIÁ THEO HÃNG (agg_price_by_make)
    # ===========================================================
    print("Populating agg_price_by_make ...")
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

    # ===========================================================
    #         2) TỔNG HỢP THEO TỈNH/THÀNH (agg_count_by_location)
    # ===========================================================
    print("Populating agg_count_by_location ...")
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

    # ===========================================================
    #     3) TỔNG HỢP LƯỢT XEM THEO NGÀY ĐĂNG (agg_views_by_day)
    # ===========================================================
    print("Populating agg_views_by_day ...")
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

    # ===========================================================
    #      4) PHÂN BUCKET THEO MỨC GIÁ (agg_price_bucket)
    # ===========================================================
    print("Populating agg_price_bucket ...")
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

    # ===========================================================
    #         5) LẤY TOP 100 TIN ĐĂNG NHIỀU LƯỢT XEM NHẤT
    # ===========================================================
    print("Populating top_listings_by_views (top 100)...")
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

    # Commit toàn bộ insert
    conn.commit()


# =================================================================
#                       HÀM MAIN (CHẠY CHÍNH)
# =================================================================
def main():
    print("BẮT ĐẦU LOAD DATA MART (DW -> DataMart)\n")

    try:
        # Kết nối MySQL
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Kết nối MySQL thành công!")

        # =======================================================
        #   BƯỚC 1: Tạo schema DataMart nếu chưa có
        # =======================================================
        execute_sql_file(cursor, SQL_DATAMART_SCHEMA, "Tạo DataMart schema")
        conn.commit()
        print("DataMart schema đã sẵn sàng.\n")

        # =======================================================
        #   BƯỚC 2: Refresh & Populate DataMart
        # =======================================================
        refresh_datamart(conn, cursor)

        # =======================================================
        #   BƯỚC 3: Thống kê nhanh số lượng bản ghi
        # =======================================================
        cursor.execute("SELECT COUNT(*) FROM bonbanh_datamart.agg_price_by_make")
        print(f"   → agg_price_by_make: {cursor.fetchone()[0]:,} bản ghi")

        cursor.execute("SELECT COUNT(*) FROM bonbanh_datamart.agg_count_by_location")
        print(f"   → agg_count_by_location: {cursor.fetchone()[0]:,} bản ghi")

        cursor.execute("SELECT COUNT(*) FROM bonbanh_datamart.top_listings_by_views")
        print(f"   → top_listings_by_views: {cursor.fetchone()[0]:,} bản ghi")

        print("\nHOÀN TẤT! DataMart đã được cập nhật.")

    except Error as e:
        print("LỖI KẾT NỐI HOẶC THỰC THI:", e)

        # rollback nếu có lỗi trong quá trình populate
        if 'conn' in locals():
            conn.rollback()

    finally:
        # Đóng cursor và connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

        print("Đóng kết nối MySQL.")


# =================================================================
#                     CHẠY FILE PYTHON
# =================================================================
if __name__ == "__main__":
    main()
