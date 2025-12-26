import uuid
import random
import time
import os
import logging
from datetime import datetime
from faker import Faker
import pyarrow as pa
import pyarrow.parquet as pq
import psutil
from tqdm import tqdm


TOTAL_RECORDS = 1_000_000
BATCH_SIZE = 100_000
OUTPUT_DIR = "../data/raw"
LOG_DIR = "../logs"
LOG_FILE = os.path.join(LOG_DIR, "data_generation.log")

PRODUCT_CATEGORIES = [
    "Electronics", "Fashion", "Home", "Books", "Groceries",
    "Health", "Sports"
]

PAYMENT_METHODS = [
    "Credit Card", "Debit Card", "Mobile Money", "Bank Transfer"
]

STATUSES = ["SUCCESS", "FAILED"]

fake = Faker()
Faker.seed(42)

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


logger = logging.getLogger("DataGenerationLogger")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)



def generate_record():
    price = round(random.uniform(5, 500), 2)
    quantity = random.randint(1, 5)
    ts = fake.date_time_between(start_date="-2y", end_date="now")

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "customer_name": fake.name(),
        "email": fake.email(),
        "country": fake.country_code(),
        "city": fake.city(),
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "product_name": fake.word(),
        "price": price,
        "quantity": quantity,
        "total_amount": round(price * quantity, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choices(STATUSES, weights=[0.9, 0.1])[0],
        "transaction_timestamp": ts
    }

def write_batch(records):
    table = pa.Table.from_pylist(records)

    year_month = records[0]["transaction_timestamp"].strftime("%Y-%m")
    partition_path = os.path.join(OUTPUT_DIR, f"year_month={year_month}")
    os.makedirs(partition_path, exist_ok=True)

    file_name = f"transactions_{uuid.uuid4().hex}.parquet"
    file_path = os.path.join(partition_path, file_name)

    pq.write_table(table, file_path, compression="snappy")

    return file_path

def main():
    start_time = time.time()
    process = psutil.Process()

    records_written = 0
    batch = []

    logger.info("Data generation started")
    logger.info(f"Target records: {TOTAL_RECORDS:,}")
    logger.info(f"Batch size: {BATCH_SIZE:,}")

    try:
        for _ in tqdm(range(TOTAL_RECORDS)):
            batch.append(generate_record())

            if len(batch) >= BATCH_SIZE:
                file_path = write_batch(batch)
                records_written += len(batch)
                batch.clear()

                mem_mb = process.memory_info().rss / (1024 * 1024)

                logger.info(
                    f"Batch written | Total records: {records_written:,} | "
                    f"Memory usage: {mem_mb:.2f} MB | File: {file_path}"
                )

        # write remaining records
        if batch:
            file_path = write_batch(batch)
            records_written += len(batch)
            logger.info(f"Final batch written | Records: {records_written:,}")

        duration = time.time() - start_time
        mem_mb = process.memory_info().rss / (1024 * 1024)

        logger.info(" Data generation completed successfully")
        logger.info(f"Total records generated: {records_written:,}")
        logger.info(f"Total time taken: {duration:.2f} seconds")
        logger.info(f"Final memory usage: {mem_mb:.2f} MB")

    except Exception as e:
        logger.error("Data generation failed", exc_info=True)
        raise

if __name__ == "__main__":
    main()
