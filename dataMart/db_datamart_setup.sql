-- db_datamart_setup.sql
CREATE DATABASE IF NOT EXISTS bonbanh_datamart
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE bonbanh_datamart;

-- 1) Tổng hợp theo mẫu xe (tên xe + năm sx) -> số lượng, giá trung bình/min/max
CREATE TABLE IF NOT EXISTS agg_price_by_make (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ten_xe VARCHAR(512),
    nam_san_xuat INT,
    listings_count INT,
    avg_price DECIMAL(18,2),
    min_price DECIMAL(18,2),
    max_price DECIMAL(18,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX (ten_xe),
    INDEX (nam_san_xuat)
) ENGINE=InnoDB;

-- 2) Tổng hợp theo vị trí (tỉnh/TP)
CREATE TABLE IF NOT EXISTS agg_count_by_location (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    noi_ban VARCHAR(255),
    listings_count INT,
    avg_price DECIMAL(18,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX (noi_ban)
) ENGINE=InnoDB;

-- 3) Views / lượt xem theo ngày (snapshot theo ngày)
CREATE TABLE IF NOT EXISTS agg_views_by_day (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ngay DATE,
    total_listings INT,
    total_views BIGINT,
    avg_views_per_listing DECIMAL(12,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX (ngay)
) ENGINE=InnoDB;

-- 4) Phân bố theo bucket giá (ví dụ: <200m, 200-500m, 500-1t, >1t)
CREATE TABLE IF NOT EXISTS agg_price_bucket (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    bucket_label VARCHAR(64),
    bucket_min BIGINT,
    bucket_max BIGINT, -- NULL nếu không giới hạn trên
    listings_count INT,
    avg_price DECIMAL(18,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX (bucket_label)
) ENGINE=InnoDB;

-- 5) Top listings theo lượt xem (snapshot top N)
CREATE TABLE IF NOT EXISTS top_listings_by_views (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_fact_id BIGINT,
    ten_xe VARCHAR(512),
    gia_xe BIGINT,
    luot_xem INT,
    ngay_dang DATE,
    noi_ban VARCHAR(255),
    link_xe VARCHAR(1024),
    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX (luot_xem),
    INDEX (ngay_dang)
) ENGINE=InnoDB;
