import requests
import csv
import re
import sys
from datetime import datetime

CUSTOMER_API = "https://dummyjson.com/users?limit=10"
ORDER_API = "https://dummyjson.com/carts?limit=10"

CUSTOMER_FILE = "customers_clean.csv"
ORDER_FILE = "orders_clean.csv"

EMAIL_REGEX = re.compile(r"[^@]+@[^@]+\.[^@]+")

# ---------- EXTRACT ----------

def fetch_customers():
    return requests.get(CUSTOMER_API).json()["users"]

def fetch_orders():
    return requests.get(ORDER_API).json()["carts"]

# ---------- TRANSFORM ----------

def transform_customer(c):
    return [
        c["id"],
        f"{c['firstName']} {c['lastName']}",
        c["email"].lower(),
        c["address"]["city"],
        datetime.utcnow().isoformat()
    ]

def transform_order(o):
    priority = "HIGH" if o["total"] > 500 else "LOW"
    return [
        o["id"],
        o["userId"],
        o["total"],
        priority,
        datetime.utcnow().isoformat()
    ]

# ---------- DATA QUALITY ----------

def validate_customers(rows):
    ids = set()
    for r in rows:
        cid, _, email, city, _ = r

        if not cid:
            fail("Customer ID NULL")

        if cid in ids:
            fail(f"Duplicate Customer ID {cid}")
        ids.add(cid)

        if not EMAIL_REGEX.match(email):
            fail(f"Invalid email {email}")

        if not city:
            fail("City NULL")

def validate_orders(rows):
    for r in rows:
        oid, cid, total, priority, _ = r

        if not oid or not cid:
            fail("Order or Customer ID NULL")

        if total < 0:
            fail("Negative order total")

        if priority not in ("HIGH", "LOW"):
            fail("Invalid priority")

def fail(msg):
    print(f"❌ DATA QUALITY FAILED: {msg}")
    sys.exit(1)

# ---------- LOAD ----------

def write_csv(file, header, rows):
    with open(file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

# ---------- PIPELINE ----------

if __name__ == "__main__":
    print("Starting ETL pipeline")

    customers = fetch_customers()
    orders = fetch_orders()

    clean_customers = [transform_customer(c) for c in customers]
    clean_orders = [transform_order(o) for o in orders]

    validate_customers(clean_customers)
    validate_orders(clean_orders)

    write_csv(
        CUSTOMER_FILE,
        ["customer_id", "full_name", "email", "city", "processed_time"],
        clean_customers
    )

    write_csv(
        ORDER_FILE,
        ["order_id", "customer_id", "total", "priority", "processed_time"],
        clean_orders
    )

    print("✅ ETL completed successfully")
