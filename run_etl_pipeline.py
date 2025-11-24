# run_etl_pipeline.py (đã cập nhật hoàn chỉnh)
import logging
import os
import sys
import subprocess
from datetime import datetime
from  load_to_controler import  ETLLogger  # <-- THÊM DÒNG NÀY

# =========================== Cấu hình logging file (giữ nguyên) ===========================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
today_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = os.path.join(LOG_DIR, f"etl_pipeline_{today_str}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ETL_Pipeline_Logger")

# =========================== Hàm chạy từng bước ===========================
def run_step(title, script_path, db_logger, step_order):
    log_id = db_logger.start_step(title, step_order)
    logger.info("=" * 70)
    logger.info("BẮT ĐẦU: %s", title)
    logger.info("=" * 70)

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=3600  # 60 phút
        )

        if result.returncode == 0:
            db_logger.end_step(log_id, 'SUCCESS', log_file=LOG_FILE)
            logger.info("HOÀN TẤT: %s", title)
            return True
        else:
            error = result.stderr[-2000:] if result.stderr else "No stderr"
            db_logger.end_step(log_id, 'FAILED', error_msg=error, log_file=LOG_FILE)
            logger.error("THẤT BẠI: %s\nSTDERR: %s", title, error)
            return False

    except subprocess.TimeoutExpired:
        db_logger.end_step(log_id, 'FAILED', error_msg="Timeout > 60 phút", log_file=LOG_FILE)
        logger.error("TIMEOUT: %s", title)
        return False
    except Exception as e:
        db_logger.end_step(log_id, 'FAILED', error_msg=str(e), log_file=LOG_FILE)
        logger.exception("LỖI KHÔNG MONG ĐỢI: %s", title)
        return False

# =========================== Main ===========================
def main():
    logger.info("\n================ BONBANH ETL PIPELINE ==================")
    logger.info("Thời gian bắt đầu: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Khởi tạo DB Logger
    db_logger = ETLLogger("BonBanh Full ETL Pipeline")

    steps = [
        ("1. Thu thập dữ liệu (Crawl)",     "get_data.py",         1),
        ("2. Load vào Staging",             "load_to_staging.py",  2),
        ("3. Load vào Data Warehouse",      "load_to_dw.py",       3),
        ("4. Load vào Data Mart",           "load_to_mart.py",     4),
    ]

    all_success = True
    for title, script, order in steps:
        if not run_step(title, script, db_logger, order):
            all_success = False

    status = "HOÀN TẤT THÀNH CÔNG" if all_success else "CÓ LỖI XẢY RA"
    logger.info("\n" + "="*60)
    logger.info(" KẾT QUẢ CUỐI CÙNG: %s ", status)
    logger.info("="*60)

    db_logger.close()

if __name__ == "__main__":
    main()