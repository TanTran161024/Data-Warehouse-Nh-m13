import mysql.connector
import os
from datetime import datetime
import config


class ETLLogger:
    def __init__(self, pipeline_name="BonBanh ETL Pipeline"):
        self.pipeline_name = pipeline_name
        self.conn = None
        self.cursor = None
        self._connect_and_setup()

    def _connect_and_setup(self):
        try:
            # Kết nối không chọn DB trước (để tạo DB nếu cần)
            self.conn = mysql.connector.connect(**config.DB_CONFIG_BASE)
            self.cursor = self.conn.cursor()

            # 1. Tạo database nếu chưa có
            self.cursor.execute("CREATE DATABASE IF NOT EXISTS bonbanh_control CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            self.cursor.execute("USE bonbanh_control")

            # 2. Tạo bảng etl_run_log nếu chưa có
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS etl_run_log (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                pipeline_name VARCHAR(100) NOT NULL,
                step_name VARCHAR(100) NOT NULL,
                step_order INT NOT NULL,
                start_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                end_time DATETIME,
                status ENUM('RUNNING', 'SUCCESS', 'FAILED') NOT NULL DEFAULT 'RUNNING',
                records_processed INT DEFAULT 0,
                error_message TEXT,
                log_file_path VARCHAR(512),
                INDEX idx_step (step_name),
                INDEX idx_status (status),
                INDEX idx_date (start_time)
            ) ENGINE=InnoDB
            """
            self.cursor.execute(create_table_sql)
            self.conn.commit()

        except Exception as e:
            print(f"[ETLLogger] Lỗi khởi tạo control DB: {e}")
            self.conn = None
            self.cursor = None

    def start_step(self, step_name, step_order):
        if not self.cursor:
            return None
        try:
            sql = """
            INSERT INTO etl_run_log 
            (pipeline_name, step_name, step_order, status) 
            VALUES (%s, %s, %s, 'RUNNING')
            """
            self.cursor.execute(sql, (self.pipeline_name, step_name, step_order))
            self.conn.commit()
            self.cursor.execute("SELECT LAST_INSERT_ID()")
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"[ETLLogger] Lỗi start_step: {e}")
            return None

    def end_step(self, log_id, status='SUCCESS', records=0, error_msg=None, log_file=None):
        if not self.cursor or not log_id:
            return
        try:
            sql = """
            UPDATE etl_run_log 
            SET end_time = CURRENT_TIMESTAMP,
                status = %s,
                records_processed = %s,
                error_message = %s,
                log_file_path = %s
            WHERE id = %s
            """
            self.cursor.execute(sql, (status, records, error_msg or "", log_file or "", log_id))
            self.conn.commit()
        except Exception as e:
            print(f"[ETLLogger] Lỗi end_step: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn and self.conn.is_connected():
            self.conn.close()