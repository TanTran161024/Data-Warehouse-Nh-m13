import requests
from bs4 import BeautifulSoup
import time
import os
import re
import csv
from datetime import datetime
import logging

# ===========================
# Cấu hình logger
# ===========================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
today_str_log = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = os.path.join(LOG_DIR, f"get_data_{today_str_log}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("GetDataLogger")

# ===========================
# Cấu hình crawl
# ===========================
BASE_URL = "https://bonbanh.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
today_str = datetime.now().strftime("%Y-%m-%d")
CSV_FILE = os.path.join(DATA_DIR, f"bonbanh_raw_{today_str}.csv")

if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Loại xe + Năm SX", "Tên xe", "Giá xe_raw", "Nơi bán", "Liên hệ", "Link xe",
            "Ngày đăng", "Lượt xem", "Số Km đã đi:", "Tình trạng:", "Xuất xứ:", "Kiểu dáng:",
            "Động cơ:", "Màu ngoại thất:", "Màu nội thất:", "Số chỗ ngồi:", "Số cửa:", "Năm sản xuất:"
        ])

# ===========================
# Ghi CSV
# ===========================
def append_csv(row_dict):
    try:
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
    except Exception as e:
        logger.exception("Lỗi khi ghi CSV: %s", e)

# ===========================
# Lấy HTML từ URL
# ===========================
def get_page(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.exception("Lỗi khi tải URL %s: %s", url, e)
        return ""

# ===========================
# Parse trang danh sách
# ===========================
def parse_list_page(html):
    soup = BeautifulSoup(html, "html.parser")
    cars = []

    for item in soup.select(".car-item"):
        try:
            a_tag = item.select_one("a")
            link = ""
            if a_tag and a_tag.get("href"):
                href = a_tag["href"].strip()
                if not href.startswith("/"):
                    href = "/" + href
                link = BASE_URL + href

            cb1 = item.select_one(".cb1")
            loai_xe = cb1.contents[0].strip() if cb1 and cb1.contents else ""
            nam_sx = cb1.select_one("b").get_text(strip=True) if cb1 and cb1.select_one("b") else ""
            info = f"{loai_xe} - {nam_sx}".strip(" -")

            ten_xe = item.select_one(".cb2 b").get_text(strip=True) if item.select_one(".cb2 b") else ""
            gia = item.select_one(".cb3 b").get_text(strip=True) if item.select_one(".cb3 b") else ""
            noi_ban = item.select_one(".cb4 b").get_text(strip=True) if item.select_one(".cb4 b") else ""
            lien_he = item.select_one(".cb7").get_text(" ", strip=True) if item.select_one(".cb7") else ""

            cars.append({
                "Loại xe + Năm SX": info,
                "Tên xe": ten_xe,
                "Giá xe_raw": gia,
                "Nơi bán": noi_ban,
                "Liên hệ": lien_he,
                "Link xe": link,
            })

        except Exception as e:
            logger.exception("Lỗi parse list item: %s", e)
            continue

    return cars

# ===========================
# Parse trang chi tiết
# ===========================
def parse_detail_page(url):
    try:
        html = get_page(url)
        soup = BeautifulSoup(html, "html.parser")

        notes = soup.find("div", class_="notes")
        notes_text = notes.get_text(strip=True) if notes else ""

        ngay_dang = ""
        luot_xem = ""

        if notes_text:
            m1 = re.search(r"Đăng\s+ngày\s+(\d{1,2}/\d{1,2}/\d{4})", notes_text)
            if m1:
                ngay_dang = m1.group(1)
            m2 = re.search(r"Xem\s+(\d+)\s+lượt", notes_text)
            if m2:
                luot_xem = m2.group(1)

        details = {}
        for row in soup.select("div#mail_parent.row"):
            label = row.find("label")
            value = row.find("span", class_="inp")
            if label and value:
                details[label.get_text(strip=True)] = value.get_text(strip=True)

        data = {"Ngày đăng": ngay_dang, "Lượt xem": luot_xem}
        data.update(details)

        return data

    except Exception as e:
        logger.exception("Lỗi khi lấy chi tiết %s: %s", url, e)
        return {}

# ===========================
# Hàm main
# ===========================
def main():
    all_count = 0

    for page in range(1, 2):
        url = f"{BASE_URL}/oto/page,{page}/" if page > 1 else BASE_URL
        logger.info("Đang tải trang danh sách %d... %s", page, url)

        html = get_page(url)
        car_list = parse_list_page(html)
        logger.info("➡ Tìm thấy %d xe trên trang %d", len(car_list), page)

        for car in car_list:
            link = car.get("Link xe")
            if link:
                logger.info("→ Lấy chi tiết: %s", link)
                detail_data = parse_detail_page(link)
                car.update(detail_data)
                append_csv(car)
                all_count += 1
                time.sleep(1)

    logger.info("🎉 Đã crawl + ghi CSV %d bản ghi.", all_count)

# ===========================
# Chạy script
# ===========================
if __name__ == "__main__":
    main()
