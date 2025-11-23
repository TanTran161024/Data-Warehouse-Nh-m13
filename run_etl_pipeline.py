import logging
import os
import sys
import subprocess
from datetime import datetime

# ===========================
# Cấu hình logging
# ===========================
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

# ===========================
# Hàm chạy một file Python và log mọi thứ
# ===========================
def run_step(title, script_path):
    logger.info("=" * 70)
    logger.info("BẮT ĐẦU: %s", title)
    logger.info("=" * 70)

    if not os.path.exists(script_path):
        logger.error("File không tồn tại: %s", script_path)
        sys.exit(1)

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True
        )

        if result.stdout:
            logger.info("STDOUT:\n%s", result.stdout.strip())
        if result.stderr:
            logger.error("STDERR:\n%s", result.stderr.strip())

        if result.returncode != 0:
            logger.error("LỖI: %s (return code: %d)", title, result.returncode)
            sys.exit(result.returncode)

        logger.info("HOÀN TẤT: %s", title)

    except Exception as e:
        logger.exception("LỖI KHÔNG XÁC ĐỊNH trong %s: %s", title, e)
        sys.exit(1)

# ===========================
# Hàm main
# ===========================
def main():
    logger.info("\n================ ETL PIPELINE BONBANH ==================\n")
    logger.info("Ngày chạy: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("File log: %s", LOG_FILE)

    # Danh sách các file Python của pipeline
    etl_scripts = [
        "get_data.py",
        "load_to_staging.py",
        "load_to_dw.py",
        "load_to_mart.py"
    ]

    for idx, script in enumerate(etl_scripts, 1):
        run_step(f"{idx}: {script}", script)

    logger.info("\n TOÀN BỘ QUÁ TRÌNH ETL ĐÃ HOÀN THÀNH KHÔNG LỖI! ")

if __name__ == "__main__":
    main()
