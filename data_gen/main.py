"""
data_gen/main.py
================
Sinh dữ liệu logistics giả lập và đẩy lên S3 (Floci) vào vùng /bronze.

Dữ liệu gồm:
  - carriers.csv   : Công ty vận chuyển
  - routes.csv     : Tuyến đường
  - shipments.csv  : Đơn hàng vận chuyển hàng ngày

Chạy:
  python main.py
  # hoặc với tuỳ chỉnh:
  python main.py --rows 5000 --date 2024-01-15
"""

import argparse
import io
import logging
import os
import random
from datetime import date, datetime, timedelta

import boto3
import pandas as pd
from botocore.config import Config
from faker import Faker

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
S3_ENDPOINT   = os.environ["S3_ENDPOINT_URL"]
AWS_KEY       = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET    = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION    = os.environ.get("AWS_DEFAULT_REGION", "us-east-1") # Region can have default

BUCKET        = "logistics-lake"
BRONZE_PREFIX = "bronze"

fake = Faker("vi_VN")
Faker.seed(42)

# ── Static reference data ────────────────────────────────────────────────────
CARRIER_NAMES = [
    "Viettel Post", "GHTK", "GHN", "J&T Express",
    "DHL Express", "FedEx Vietnam", "VNPost", "Ninja Van",
    "Best Express", "Kerry Express",
]

VEHICLE_TYPES = ["Xe máy", "Xe tải nhỏ", "Xe tải lớn", "Container", "Xe lạnh"]

PROVINCES = [
    "Hà Nội", "TP. Hồ Chí Minh", "Đà Nẵng", "Cần Thơ",
    "Hải Phòng", "Biên Hòa", "Nha Trang", "Huế",
    "Quy Nhơn", "Vũng Tàu", "Bình Dương", "Long An",
]

STATUS_OPTIONS = ["Đang vận chuyển", "Đã giao", "Hoàn hàng", "Chờ lấy hàng", "Thất lạc"]
STATUS_WEIGHTS = [0.3, 0.55, 0.08, 0.05, 0.02]

PRODUCT_TYPES = [
    "Điện tử", "Thực phẩm", "Quần áo", "Mỹ phẩm",
    "Đồ gia dụng", "Sách vở", "Đồ chơi", "Vật tư y tế",
]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
        config=Config(s3={"addressing_style": "path"}),
    )


def ensure_bucket(s3):
    """Tạo bucket nếu chưa tồn tại."""
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)
        log.info(f"Đã tạo bucket: {BUCKET}")
    else:
        log.info(f"Bucket '{BUCKET}' đã tồn tại, bỏ qua.")


def upload_df(s3, df: pd.DataFrame, key: str):
    """Upload DataFrame dưới dạng CSV lên S3."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, encoding="utf-8-sig")
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue().encode("utf-8-sig"),
        ContentType="text/csv",
    )
    log.info(f"✅ Đã upload {len(df):,} dòng → s3://{BUCKET}/{key}")


# ── Data generators ──────────────────────────────────────────────────────────
def gen_carriers(n: int = len(CARRIER_NAMES)) -> pd.DataFrame:
    """Sinh bảng công ty vận chuyển (static, ít thay đổi)."""
    random.seed(0)
    records = []
    for i, name in enumerate(CARRIER_NAMES, start=1):
        records.append({
            "carrier_id":      f"CAR{i:03d}",
            "carrier_name":    name,
            "carrier_type":    random.choice(["Nội địa", "Quốc tế"]),
            "max_weight_kg":   random.choice([5, 20, 100, 500, 1000]),
            "rating":          round(random.uniform(3.5, 5.0), 1),
            "is_active":       True,
            "created_at":      "2023-01-01",
        })
    return pd.DataFrame(records)


def gen_routes() -> pd.DataFrame:
    """Sinh bảng tuyến đường từ mọi cặp tỉnh."""
    records = []
    route_id = 1
    for origin in PROVINCES:
        for dest in PROVINCES:
            if origin != dest:
                distance = random.randint(50, 1800)
                records.append({
                    "route_id":          f"RT{route_id:04d}",
                    "origin_province":   origin,
                    "dest_province":     dest,
                    "distance_km":       distance,
                    "est_transit_days":  max(1, distance // 300),
                })
                route_id += 1
    return pd.DataFrame(records)


def gen_shipments(target_date: date, n_rows: int, carriers_df: pd.DataFrame, routes_df: pd.DataFrame) -> pd.DataFrame:
    """Sinh đơn hàng vận chuyển cho một ngày cụ thể."""
    carrier_ids = carriers_df["carrier_id"].tolist()
    route_ids   = routes_df["route_id"].tolist()

    records = []
    for i in range(n_rows):
        created_dt = datetime.combine(target_date, datetime.min.time()) + timedelta(
            hours=random.randint(6, 22),
            minutes=random.randint(0, 59),
        )
        weight    = round(random.uniform(0.1, 500), 2)
        distance  = routes_df.loc[routes_df["route_id"] == random.choice(route_ids), "distance_km"].values[0]
        base_fee  = weight * 5_000 + distance * 2_000
        final_fee = round(base_fee * random.uniform(0.8, 1.3), -3)
        status    = random.choices(STATUS_OPTIONS, STATUS_WEIGHTS)[0]

        delivered_at = None
        if status == "Đã giao":
            delivered_at = (created_dt + timedelta(days=random.randint(1, 7))).isoformat()

        records.append({
            "shipment_id":    f"SHP{target_date.strftime('%Y%m%d')}{i+1:05d}",
            "carrier_id":     random.choice(carrier_ids),
            "route_id":       random.choice(route_ids),
            "product_type":   random.choice(PRODUCT_TYPES),
            "vehicle_type":   random.choice(VEHICLE_TYPES),
            "weight_kg":      weight,
            "shipping_fee":   final_fee,
            "status":         status,
            "sender_name":    fake.name(),
            "receiver_name":  fake.name(),
            "created_at":     created_dt.isoformat(),
            "delivered_at":   delivered_at,
            "data_date":      target_date.isoformat(),
        })

    return pd.DataFrame(records)


# ── Main ─────────────────────────────────────────────────────────────────────
def run(target_date: date, n_rows: int):
    log.info(f"🚀 Bắt đầu sinh dữ liệu cho ngày {target_date} ({n_rows:,} đơn hàng)")

    s3 = get_s3_client()
    ensure_bucket(s3)

    # 1. Carriers (upload 1 lần hoặc ghi đè)
    carriers_df = gen_carriers()
    upload_df(s3, carriers_df, f"{BRONZE_PREFIX}/carriers/carriers.csv")

    # 2. Routes (upload 1 lần hoặc ghi đè)
    routes_df = gen_routes()
    upload_df(s3, routes_df, f"{BRONZE_PREFIX}/routes/routes.csv")

    # 3. Shipments theo ngày (partition theo date)
    shipments_df = gen_shipments(target_date, n_rows, carriers_df, routes_df)
    date_str = target_date.strftime("%Y-%m-%d")
    upload_df(s3, shipments_df, f"{BRONZE_PREFIX}/shipments/date={date_str}/shipments.csv")

    log.info("✅ Hoàn thành nạp dữ liệu vào S3 Bronze layer.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Logistics Data Generator")
    parser.add_argument("--rows", type=int, default=1000, help="Số đơn hàng mỗi ngày")
    parser.add_argument("--date", type=str, default=str(date.today()), help="Ngày dữ liệu (YYYY-MM-DD)")
    args = parser.parse_args()

    run(
        target_date=date.fromisoformat(args.date),
        n_rows=args.rows,
    )
