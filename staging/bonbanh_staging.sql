CREATE DATABASE IF NOT EXISTS bonbanh_staging
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
USE bonbanh_staging;
CREATE TABLE IF NOT EXISTS xe_bonbanh (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  loai_xe_nam_sx VARCHAR(255),
  ten_xe VARCHAR(512),
  gia_xe_raw VARCHAR(255),
  gia_xe_vnd BIGINT DEFAULT NULL,
  noi_ban VARCHAR(255),
  lien_he TEXT,
  link_xe VARCHAR(1024),
  ngay_dang DATE DEFAULT NULL,
  luot_xem INT DEFAULT NULL,
  so_km INT DEFAULT NULL,
  tinh_trang VARCHAR(128),
  xuat_xu VARCHAR(128),
  kieu_dang VARCHAR(128),
  dong_co VARCHAR(128),
  mau_ngoai_that VARCHAR(128),
  mau_noi_that VARCHAR(128),
  so_cho_ngoi VARCHAR(32),
  so_cua VARCHAR(32),
  nam_san_xuat VARCHAR(32),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY ux_link (link_xe(700))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;