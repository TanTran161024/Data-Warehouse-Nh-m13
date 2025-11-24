import os
from datetime import datetime, date

# ===========================
# Logger Configuration
# ===========================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def get_log_file(script_name):
    """
    Generate log file path based on script name.
    For staging and dw: use datetime with time
    For mart: use date only
    """
    if script_name in ["load_to_staging", "load_to_dw"]:
        today_str_log = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    else:  # for mart
        today_str_log = date.today().strftime("%Y-%m-%d")
    return os.path.join(LOG_DIR, f"{script_name}_{today_str_log}.log")

LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(levelname)s - %(message)s",
    "encoding": "utf-8"
}

# ===========================
# MySQL Database Configuration
# ===========================
DB_CONFIG_BASE = {
    "host": "localhost",
    "user": "root",
    "password": "",  # Replace with your actual password
    "charset": "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
    "autocommit": False
}

# For staging without database specified
DB_CONFIG_NO_DB = DB_CONFIG_BASE.copy()
DB_CONFIG_NO_DB["autocommit"] = True  # Specific for staging init

# For staging with database
DB_CONFIG_STAGING = {**DB_CONFIG_BASE, "database": "bonbanh_staging"}

# ===========================
# File Paths
# ===========================
# Staging
STAGING_CSV_FILE_PATTERN = "data/bonbanh_raw_{today}.csv"  # Use .format(today=datetime.now().strftime("%Y-%m-%d"))
STAGING_SQL_SCHEMA_FILE = "staging/bonbanh_staging.sql"
STAGING_SQL_SP_FILE = "staging/transform.sql"

# Data Warehouse
DW_SQL_SCHEMA_FILE = "dataWarehouse/db_dw_setup.sql"
DW_SQL_PROCEDURE_FILE = "dataWarehouse/sp_load_dw.sql"

# Data Mart
DATAMART_SQL_SCHEMA_FILE = "dataMart/db_datamart_setup.sql"

# ===========================
# Other Constants
# ===========================
STAGING_DB_NAME = "bonbanh_staging"
DW_DB_NAME = "bonbanh_datawarehouse"
DATAMART_DB_NAME = "bonbanh_datamart"