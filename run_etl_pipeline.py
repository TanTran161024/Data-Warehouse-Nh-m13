import subprocess
import sys
import datetime

def run_step(title, command):
    print("\n" + "="*70)
    print(f"{title}")
    print("="*70)

    try:
        result = subprocess.run(
            [sys.executable, command],
            capture_output=False,
            text=True
        )

        if result.returncode != 0:
            print(f"LỖI: {title}")
            sys.exit(result.returncode)

        print(f"HOÀN TẤT: {title}")

    except Exception as e:
        print(f" LỖI KHÔNG XÁC ĐỊNH trong {title}: {e}")
        sys.exit(1)


def main():
    print("\n================ ETL PIPELINE BONBANH ==================\n")
    print("Ngày chạy:", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # 1. Crawl dữ liệu
    run_step("1: Crawl dữ liệu (get_data.py)", "get_data.py")

    # 2. Load vào staging
    run_step("2: Load vào STAGING (load_to_staging.py)", "load_to_staging.py")

    # 3. Load vào Data Warehouse
    run_step("3: Load vào DATA WAREHOUSE (load_to_dw.py)", "load_to_dw.py")
    # 4. Load vào Data Mart
    run_step("4: Load vào DATA MART (load_to_datamart.py)", "load_to_mart.py")
    print("\n TOÀN BỘ QUÁ TRÌNH ETL ĐÃ HOÀN THÀNH KHÔNG LỖI! ")
    


if __name__ == "__main__":
    main()
