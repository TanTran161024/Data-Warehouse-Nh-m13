-- *** Stored procedure sp_transform_row ***
DELIMITER $$
CREATE OR REPLACE PROCEDURE sp_transform_row(
  IN p_loai_xe_nam_sx VARCHAR(255),
  IN p_ten_xe VARCHAR(512),
  IN p_gia_raw VARCHAR(255),
  IN p_noi_ban VARCHAR(255),
  IN p_lien_he TEXT,
  IN p_link_xe VARCHAR(1024),
  IN p_ngay_dang_raw VARCHAR(50),
  IN p_luot_xem_raw VARCHAR(50),
  IN p_so_km_raw VARCHAR(50),
  IN p_tinh_trang VARCHAR(128),
  IN p_xuat_xu VARCHAR(128),
  IN p_kieu_dang VARCHAR(128),
  IN p_dong_co VARCHAR(128),
  IN p_mau_ngoai VARCHAR(128),
  IN p_mau_noi VARCHAR(128),
  IN p_so_cho VARCHAR(32),
  IN p_so_cua VARCHAR(32),
  IN p_nam_sx VARCHAR(32)
)
BEGIN
  DECLARE v_price_ty BIGINT DEFAULT 0;
  DECLARE v_price_tr BIGINT DEFAULT 0;
  DECLARE v_price BIGINT DEFAULT NULL;
  DECLARE v_km INT DEFAULT NULL;
  DECLARE v_ngay DATE DEFAULT NULL;
  DECLARE v_luot INT DEFAULT NULL;
  DECLARE v_tmp VARCHAR(255);

  -- parse price (supports patterns like '4 Tỷ 350 Tr.' '695 Triệu' ...)
  IF p_gia_raw IS NOT NULL AND p_gia_raw <> '' THEN
    -- phần Tỷ
    SET v_tmp = REGEXP_SUBSTR(p_gia_raw, '[0-9]+(?=\s*[Tt]ỷ|\s*[Tt]y)');
    IF v_tmp IS NOT NULL AND v_tmp <> '' THEN
      SET v_price_ty = CAST(v_tmp AS UNSIGNED) * 1000000000;
    ELSE
      SET v_price_ty = 0;
    END IF;

    -- phần Triệu / Tr
    SET v_tmp = REGEXP_SUBSTR(p_gia_raw, '[0-9]+(?=\s*(Tr|Triệ[u]?) )');
    IF v_tmp IS NULL THEN
      -- fallback: tìm số liền sau Tỷ nếu có, hoặc tìm số trước 'Triệu' không match trên regex
      SET v_tmp = REGEXP_SUBSTR(p_gia_raw, '(?<=Tỷ).*?[0-9]+');
    END IF;
    IF v_tmp IS NOT NULL AND v_tmp <> '' THEN
      -- chỉ lấy số trong v_tmp
      SET v_tmp = REGEXP_REPLACE(v_tmp, '[^0-9]', '');
      IF v_tmp <> '' THEN
        SET v_price_tr = CAST(v_tmp AS UNSIGNED) * 1000000;
      END IF;
    END IF;

    SET v_price = v_price_ty + v_price_tr;
    IF v_price = 0 THEN
      -- thử bắt số nguyên (những chuỗi like '695 Triệu' hoặc '695')
      SET v_tmp = REGEXP_REPLACE(p_gia_raw, '[^0-9]', '');
      IF v_tmp <> '' THEN
        -- nếu chuỗi ban đầu có chữ 'Triệu' thì nhân với 1e6
        IF p_gia_raw LIKE '%Triệu%' OR p_gia_raw LIKE '%Tr.%' OR p_gia_raw LIKE '%Tr %' THEN
          SET v_price = CAST(v_tmp AS UNSIGNED) * 1000000;
        ELSE
          -- không rõ, đặt NULL để người xử lý biết
          SET v_price = NULL;
        END IF;
      END IF;
    END IF;
  END IF;

  -- parse km
  IF p_so_km_raw IS NOT NULL AND p_so_km_raw <> '' THEN
    SET v_tmp = REGEXP_REPLACE(p_so_km_raw, '[^0-9]', '');
    IF v_tmp <> '' THEN
      SET v_km = CAST(v_tmp AS UNSIGNED);
    END IF;
  END IF;

  -- parse date dd/mm/yyyy
  IF p_ngay_dang_raw IS NOT NULL AND p_ngay_dang_raw <> '' THEN
    SET v_ngay = STR_TO_DATE(p_ngay_dang_raw, '%d/%m/%Y');
  END IF;

  -- parse luot xem
  IF p_luot_xem_raw IS NOT NULL AND p_luot_xem_raw <> '' THEN
    SET v_tmp = REGEXP_REPLACE(p_luot_xem_raw, '[^0-9]', '');
    IF v_tmp <> '' THEN
      SET v_luot = CAST(v_tmp AS UNSIGNED);
    END IF;
  END IF;

  -- insert or update (upsert) - dùng link_xe làm unique key
  INSERT INTO xe_bonbanh (
    loai_xe_nam_sx, ten_xe, gia_xe_raw, gia_xe_vnd, noi_ban, lien_he, link_xe,
    ngay_dang, luot_xem, so_km, tinh_trang, xuat_xu, kieu_dang, dong_co,
    mau_ngoai_that, mau_noi_that, so_cho_ngoi, so_cua, nam_san_xuat
  ) VALUES (
    p_loai_xe_nam_sx, p_ten_xe, p_gia_raw, v_price, p_noi_ban, p_lien_he, p_link_xe,
    v_ngay, v_luot, v_km, p_tinh_trang, p_xuat_xu, p_kieu_dang, p_dong_co,
    p_mau_ngoai, p_mau_noi, p_so_cho, p_so_cua, p_nam_sx
  )
  ON DUPLICATE KEY UPDATE
    loai_xe_nam_sx = VALUES(loai_xe_nam_sx),
    ten_xe = VALUES(ten_xe),
    gia_xe_raw = VALUES(gia_xe_raw),
    gia_xe_vnd = VALUES(gia_xe_vnd),
    noi_ban = VALUES(noi_ban),
    lien_he = VALUES(lien_he),
    ngay_dang = VALUES(ngay_dang),
    luot_xem = VALUES(luot_xem),
    so_km = VALUES(so_km),
    tinh_trang = VALUES(tinh_trang),
    xuat_xu = VALUES(xuat_xu),
    kieu_dang = VALUES(kieu_dang),
    dong_co = VALUES(dong_co),
    mau_ngoai_that = VALUES(mau_ngoai_that),
    mau_noi_that = VALUES(mau_noi_that),
    so_cho_ngoi = VALUES(so_cho_ngoi),
    so_cua = VALUES(so_cua),
    nam_san_xuat = VALUES(nam_san_xuat),
    updated_at = CURRENT_TIMESTAMP;

END$$
DELIMITER ;

-- End of transform.sql
