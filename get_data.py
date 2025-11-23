import requests
from bs4 import BeautifulSoup
import time
import os
import re
import csv
from datetime import datetime

BASE_URL = "https://bonbanh.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ==== Đường dẫn CSV ====
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Tạo tên file CSV theo ngày chạy
today_str = datetime.now().strftime("%Y-%m-%d")
CSV_FILE = os.path.join(DATA_DIR, f"bonbanh_raw_{today_str}.csv")

# Tạo file CSV + header nếu chưa tồn tại
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Loại xe + Năm SX", "Tên xe", "Giá xe_raw", "Nơi bán", "Liên hệ", "Link xe",
            "Ngày đăng", "Lượt xem", "Số Km đã đi:", "Tình trạng:", "Xuất xứ:", "Kiểu dáng:",
            "Động cơ:", "Màu ngoại thất:", "Màu nội thất:", "Số chỗ ngồi:", "Số cửa:", "Năm sản xuất:"
        ])

# ====================================================================
# Hàm ghi 1 dòng dữ liệu vào file CSV
# ====================================================================
def append_csv(row_dict):
    """
    Ghi một dòng thông tin xe vào file CSV.
    row_dict: dict chứa các trường thông tin của xe.
    """
    with open(CSV_FILE, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            row_dict.get("Loại xe + Năm SX", ""),
            row_dict.get("Tên xe", ""),
            row_dict.get("Giá xe_raw", ""),
            row_dict.get("Nơi bán", ""),
            row_dict.get("Liên hệ", ""),
            row_dict.get("Link xe", ""),
            row_dict.get("Ngày đăng", ""),
            row_dict.get("Lượt xem", ""),
            row_dict.get("Số Km đã đi:", ""),
            row_dict.get("Tình trạng:", ""),
            row_dict.get("Xuất xứ:", ""),
            row_dict.get("Kiểu dáng:", ""),
            row_dict.get("Động cơ:", ""),
            row_dict.get("Màu ngoại thất:", ""),
            row_dict.get("Màu nội thất:", ""),
            row_dict.get("Số chỗ ngồi:", ""),
            row_dict.get("Số cửa:", ""),
            row_dict.get("Năm sản xuất:", "")
        ])

# ====================================================================
# Hàm tải HTML từ 1 URL
# ====================================================================
def get_page(url):
    """
    Gửi request đến URL và trả về HTML.
    Có timeout và raise_for_status để báo lỗi khi request lỗi.
    """
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text

# ====================================================================
# Hàm parse trang danh sách xe
# ====================================================================
def parse_list_page(html):
    """
    Parse HTML của trang danh sách để lấy:
    - Loại xe + năm
    - Tên xe
    - Giá
    - Nơi bán
    - Liên hệ
    - Link chi tiết

    Trả về danh sách dict.
    """
    soup = BeautifulSoup(html, "html.parser")
    cars = []

    # .car-item = mỗi khối xe trên trang bonbanh
    for item in soup.select(".car-item"):
        try:
            # Lấy link xe
            a_tag = item.select_one("a")
            link = ""
            if a_tag and a_tag.get("href"):
                href = a_tag["href"].strip()
                if not href.startswith("/"):
                    href = "/" + href
                link = BASE_URL + href

            # Loại xe và năm sản xuất
            cb1 = item.select_one(".cb1")
            loai_xe = cb1.contents[0].strip() if cb1 and cb1.contents else ""
            nam_sx = cb1.select_one("b").get_text(strip=True) if cb1 and cb1.select_one("b") else ""
            info = f"{loai_xe} - {nam_sx}".strip(" -")

            # Các trường cơ bản
            ten_xe = item.select_one(".cb2 b").get_text(strip=True) if item.select_one(".cb2 b") else ""
            gia = item.select_one(".cb3 b").get_text(strip=True) if item.select_one(".cb3 b") else ""
            noi_ban = item.select_one(".cb4 b").get_text(strip=True) if item.select_one(".cb4 b") else ""
            lien_he = item.select_one(".cb7").get_text(" ", strip=True) if item.select_one(".cb7") else ""

            # Lưu vào list
            cars.append({
                "Loại xe + Năm SX": info,
                "Tên xe": ten_xe,
                "Giá xe_raw": gia,
                "Nơi bán": noi_ban,
                "Liên hệ": lien_he,
                "Link xe": link,
            })

        except Exception as e:
            print(f"Lỗi parse list item: {e}")
            continue

    return cars

# ====================================================================
# Hàm parse trang chi tiết 1 xe
# ====================================================================
def parse_detail_page(url):
    """
    Parse HTML của trang chi tiết:
    - Lấy ngày đăng
    - Lượt xem
    - Lấy các trường chi tiết trong bảng thông số (div#mail_parent.row)
    """
    try:
        html = get_page(url)
        soup = BeautifulSoup(html, "html.parser")

        # ---- Lấy ngày đăng + lượt xem ----
        notes = soup.find("div", class_="notes")
        notes_text = notes.get_text(strip=True) if notes else ""

        ngay_dang = ""
        luot_xem = ""

        if notes_text:
            # Regex lấy ngày đăng
            m1 = re.search(r"Đăng\s+ngày\s+(\d{1,2}/\d{1,2}/\d{4})", notes_text)
            if m1:
                ngay_dang = m1.group(1)

            # Regex lấy lượt xem
            m2 = re.search(r"Xem\s+(\d+)\s+lượt", notes_text)
            if m2:
                luot_xem = m2.group(1)

        # ---- Lấy thông số xe ----
        details = {}
        for row in soup.select("div#mail_parent.row"):
            label = row.find("label")
            value = row.find("span", class_="inp")
            if label and value:
                # Ví dụ: "Số cửa:" : "4"
                details[label.get_text(strip=True)] = value.get_text(strip=True)

        # Gom thông tin lại
        data = {"Ngày đăng": ngay_dang, "Lượt xem": luot_xem}
        data.update(details)

        return data

    except Exception as e:
        print(f"Lỗi khi lấy chi tiết {url}: {e}")
        return {}

# ====================================================================
# Hàm chính chạy crawl
# ====================================================================
def main():
    """
    Chạy vòng lặp qua các trang:
    - Crawl danh sách xe
    - Với mỗi xe: crawl thêm trang chi tiết
    - Ghi vào CSV
    """
    all_count = 0

    # Duyệt 1 trang (bạn có thể chỉnh range để crawl nhiều trang)
    for page in range(1, 2):
        url = f"{BASE_URL}/oto/page,{page}/" if page > 1 else BASE_URL
        print(f"Đang tải trang danh sách {page}... {url}")

        # Load HTML + parse danh sách
        html = get_page(url)
        car_list = parse_list_page(html)
        print(f"\n➡ Tìm thấy {len(car_list)} xe trên trang {page}\n")

        # Duyệt từng xe => lấy chi tiết
        for car in car_list:
            link = car.get("Link xe")
            if link:
                print(f"→ Lấy chi tiết: {link}")
                detail_data = parse_detail_page(link)

                # Gộp thông tin list + detail
                car.update(detail_data)

                # Ghi CSV
                append_csv(car)

                all_count += 1
                time.sleep(1)  # tránh bị chặn IP

    print(f"\n🎉 Đã crawl + ghi CSV {all_count} bản ghi.")

# ====================================================================
# Chạy script
# ====================================================================
if __name__ == "__main__":
    main()
