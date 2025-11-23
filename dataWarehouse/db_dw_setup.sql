CREATE DATABASE IF NOT EXISTS bonbanh_datawarehouse
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE bonbanh_datawarehouse;

-- ===== DIMENSIONS =====
CREATE TABLE IF NOT EXISTS dim_mau_xe (
    surrogate_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(64) UNIQUE,
    ten_xe VARCHAR(512),
    loai_xe_nam_sx VARCHAR(255),
    nam_san_xuat INT,
    dong_co VARCHAR(128),
    mau_ngoai_that VARCHAR(128),
    mau_noi_that VARCHAR(128),
    so_cho_ngoi INT,
    so_cua INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_vi_tri (
    surrogate_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(64) UNIQUE,
    noi_ban VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_nguoi_ban (
    surrogate_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(64) UNIQUE,
    lien_he VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_xuat_xu (
    surrogate_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(64) UNIQUE,
    xuat_xu VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_tinh_trang (
    surrogate_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(64) UNIQUE,
    tinh_trang VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_kieu_dang (
    surrogate_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(64) UNIQUE,
    kieu_dang VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ===== FACT TABLE =====
CREATE TABLE IF NOT EXISTS fact_danh_sach_xe (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    mau_xe_sk BIGINT,
    vi_tri_sk BIGINT,
    nguoi_ban_sk BIGINT,
    xuat_xu_sk BIGINT,
    tinh_trang_sk BIGINT,
    kieu_dang_sk BIGINT,
    gia_xe BIGINT,
    so_km BIGINT,
    ngay_dang DATE,
    luot_xem INT,
    link_xe VARCHAR(1024),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (mau_xe_sk) REFERENCES dim_mau_xe(surrogate_key),
    FOREIGN KEY (vi_tri_sk) REFERENCES dim_vi_tri(surrogate_key),
    FOREIGN KEY (nguoi_ban_sk) REFERENCES dim_nguoi_ban(surrogate_key)
) ENGINE=InnoDB;
