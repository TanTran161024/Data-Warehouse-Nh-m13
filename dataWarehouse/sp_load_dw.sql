DROP PROCEDURE IF EXISTS sp_load_dw;
DELIMITER $$

CREATE PROCEDURE sp_load_dw()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_id BIGINT;

    -- staging columns
    DECLARE v_ten VARCHAR(512);
    DECLARE v_loai VARCHAR(255);
    DECLARE v_namsx VARCHAR(32);
    DECLARE v_dongco VARCHAR(255);
    DECLARE v_maungoai VARCHAR(255);
    DECLARE v_maunoi VARCHAR(255);
    DECLARE v_cho INT;
    DECLARE v_cua INT;
    DECLARE v_noiban VARCHAR(255);
    DECLARE v_lienhe VARCHAR(255);
    DECLARE v_xuatxu VARCHAR(255);
    DECLARE v_tinhtrang VARCHAR(255);
    DECLARE v_kieudang VARCHAR(255);
    DECLARE v_gia BIGINT;
    DECLARE v_km BIGINT;
    DECLARE v_ngaydang DATE;
    DECLARE v_luot INT;
    DECLARE v_link VARCHAR(1024);

    DECLARE cur CURSOR FOR 
        SELECT 
            ten_xe, loai_xe_nam_sx, nam_san_xuat, dong_co, mau_ngoai_that, mau_noi_that,
            so_cho_ngoi, so_cua, noi_ban, lien_he, xuat_xu, tinh_trang, kieu_dang,
            gia_xe_vnd, so_km, ngay_dang, luot_xem, link_xe
        FROM bonbanh_staging.xe_bonbanh;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO v_ten, v_loai, v_namsx, v_dongco, v_maungoai, v_maunoi,
            v_cho, v_cua, v_noiban, v_lienhe, v_xuatxu, v_tinhtrang, v_kieudang,
            v_gia, v_km, v_ngaydang, v_luot, v_link;

        IF done THEN 
            LEAVE read_loop; 
        END IF;

        -- ========================================
        -- 🔵 DIM MAU XE (SCD TYPE 1)
        -- ========================================
        SET @bk_car = MD5(CONCAT(IFNULL(v_ten,''), '_', IFNULL(v_namsx,'')));

        INSERT INTO dim_mau_xe (
            business_key, ten_xe, loai_xe_nam_sx, nam_san_xuat,
            dong_co, mau_ngoai_that, mau_noi_that, so_cho_ngoi, so_cua
        ) VALUES (
            @bk_car, v_ten, v_loai, v_namsx, v_dongco, v_maungoai, v_maunoi, v_cho, v_cua
        )
        ON DUPLICATE KEY UPDATE
            ten_xe = VALUES(ten_xe),
            loai_xe_nam_sx = VALUES(loai_xe_nam_sx),
            nam_san_xuat = VALUES(nam_san_xuat),
            dong_co = VALUES(dong_co),
            mau_ngoai_that = VALUES(mau_ngoai_that),
            mau_noi_that = VALUES(mau_noi_that),
            so_cho_ngoi = VALUES(so_cho_ngoi),
            so_cua = VALUES(so_cua);

        SELECT surrogate_key INTO @sk_car 
        FROM dim_mau_xe WHERE business_key=@bk_car;

        -- ========================================
        -- 🔵 DIM VỊ TRÍ
        -- ========================================
        SET @bk_loc = MD5(IFNULL(v_noiban,''));
        INSERT INTO dim_vi_tri (business_key, noi_ban)
        VALUES (@bk_loc, v_noiban)
        ON DUPLICATE KEY UPDATE noi_ban = VALUES(noi_ban);

        SELECT surrogate_key INTO @sk_loc 
        FROM dim_vi_tri WHERE business_key=@bk_loc;

        -- DIM NGƯỜI BÁN
        SET @bk_seller = MD5(IFNULL(v_lienhe,''));
        INSERT INTO dim_nguoi_ban (business_key, lien_he)
        VALUES (@bk_seller, v_lienhe)
        ON DUPLICATE KEY UPDATE lien_he = VALUES(lien_he);

        SELECT surrogate_key INTO @sk_seller 
        FROM dim_nguoi_ban WHERE business_key=@bk_seller;

        -- DIM XUẤT XỨ
        SET @bk_xx = MD5(IFNULL(v_xuatxu,''));
        INSERT INTO dim_xuat_xu (business_key, xuat_xu)
        VALUES (@bk_xx, v_xuatxu)
        ON DUPLICATE KEY UPDATE xuat_xu = VALUES(xuat_xu);

        SELECT surrogate_key INTO @sk_xx 
        FROM dim_xuat_xu WHERE business_key=@bk_xx;

        -- DIM TÌNH TRẠNG
        SET @bk_cond = MD5(IFNULL(v_tinhtrang,''));
        INSERT INTO dim_tinh_trang (business_key, tinh_trang)
        VALUES (@bk_cond, v_tinhtrang)
        ON DUPLICATE KEY UPDATE tinh_trang = VALUES(tinh_trang);

        SELECT surrogate_key INTO @sk_cond 
        FROM dim_tinh_trang WHERE business_key=@bk_cond;

        -- DIM KIỂU DÁNG
        SET @bk_style = MD5(IFNULL(v_kieudang,''));
        INSERT INTO dim_kieu_dang (business_key, kieu_dang)
        VALUES (@bk_style, v_kieudang)
        ON DUPLICATE KEY UPDATE kieu_dang = VALUES(kieu_dang);

        SELECT surrogate_key INTO @sk_style 
        FROM dim_kieu_dang WHERE business_key=@bk_style;

        -- ========================================
        -- 🟠 INSERT FACT
        -- ========================================
        INSERT INTO fact_danh_sach_xe (
            mau_xe_sk, vi_tri_sk, nguoi_ban_sk, xuat_xu_sk, tinh_trang_sk, kieu_dang_sk,
            gia_xe, so_km, ngay_dang, luot_xem, link_xe
        ) VALUES (
            @sk_car, @sk_loc, @sk_seller, @sk_xx, @sk_cond, @sk_style,
            v_gia, v_km, v_ngaydang, v_luot, v_link
        );

    END LOOP;

    CLOSE cur;

END$$
DELIMITER ;
