import csv
import os
import random
import string
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from pypinyin import pinyin, Style, lazy_pinyin

# Create a Faker instance with the Chinese locale
fake = Faker('zh_CN')

# customer_names = ["Li Wei", "Wang Fang", "Zhang Hua", "Zhao Ming", "Chen Jie", "Liu Yang", "Xu Lei", "Sun Qiang", "Ma Bin", "Gao Ying"]
mobile_models = ["iPhone 13", "Galaxy S21", "OnePlus 9", "Xiaomi Mi 11", "iPhone 13 Pro", "Huawei P40", "Oppo Reno5", "Vivo X60"]
mobile_colors = ["Red", "Black", "White", "Blue", "Purple"]
mobile_storage = ["8GB", "32GB", "64GB", "128GB", "254GB", "1TB"]
promotion_codes = ["SPRING20", "SUMMER15", "WINTER10", "AUTUMN5", "NEWYEAR25", "FESTIVE30"]
payment_methods = ["Credit Card", "PayPal", "Debit Card", "Bank Transfer"]
payment_providers = ["Visa", "MasterCard", "American Express", "PayPal", "Alipay", "WeChat Pay"]
shipping_statuses = ["Shipped", "In Transit", "Delivered", "Pending"]
payment_statuses = ["Paid", "Pending", "Failed"]


def random_date(start, end):
    return start + timedelta(days=random.randint(0, int((end - start).days)))

def generate_name():
    chinese_name = fake.name()
    english_name = pinyin(chinese_name, style=Style.NORMAL)
    return ' '.join([word[0].capitalize() for word in english_name])

def generate_address():
    chinese_address = fake.address()
    english_address = lazy_pinyin(chinese_address, style=Style.NORMAL)
    return ' '.join(english_address).title()

def generate_custom_id():
    # Generate a 10-character alphanumeric string
    random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=9))
    timestamp = str(datetime.now().time()).replace(":", "").replace(".", "")
    custom_id = random_string + timestamp
    return custom_id


date_range = pd.date_range(start="2023-01-01", end="2024-05-31", freq='D')
for date in date_range:
    records = []
    # Generate random amount of orders per date
    order_amount = random.randint(250, 1000)
    for i in range(0, order_amount):
        model = random.choice(mobile_models)
        quantity = random.randint(1, 10)
        price_per_unit = random.randint(50000, 150000)                                                             # Yen not Dollars
        total_price = quantity * price_per_unit
        promo_code = random.choice(promotion_codes)
        discount = float(''.join(filter(str.isdigit, promo_code))) / 100 if promo_code else 0
        order_amount = total_price * (1 - discount)
        gst = order_amount * 0.15

        record = {
            "Order ID": generate_custom_id(),
            "Customer Name": generate_name(),
            "Mobile Model": model.split(" ", 1)[0]+"/"+model.split(" ", 1)[1]+"/"+random.choice(mobile_colors)+"/"+random.choice(mobile_storage),
            "Quantity": quantity,
            "Price per Unit": price_per_unit,
            "Total Price": total_price,
            "Promotion Code": promo_code,
            "Order Amount": order_amount,
            "GST": gst,
            "Order Date": date,
            "Payment Status": random.choice(payment_statuses),
            "Shipping Status": random.choice(shipping_statuses),
            "Payment Method": random.choice(payment_methods),
            "Payment Provider": random.choice(payment_providers),
            "Mobile": fake.phone_number(),
            "Delivery Address": generate_address()
        }
        records.append(record)

    # Write to CSV
    headers = ["Order ID", "Customer Name", "Mobile Model", "Quantity", "Price per Unit", "Total Price", "Promotion Code",
               "Order Amount", "GST", "Order Date", "Payment Status", "Shipping Status","Payment Method", "Payment Provider",
               "Mobile", "Delivery Address"]
    date_folder = date.strftime('%Y-%m-%d')
    date_file = date.strftime('%Y%m%d')
    csv_file_path = "../data/sales/source=CHN/format=csv/date="+date_folder+"/order-"+date_file+".csv"

    # Ensure the directory exists
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for row in records:
            writer.writerow(row)

print(f"Generated China CSV orders")
