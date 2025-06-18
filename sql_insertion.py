import pandas as pd
import pyodbc
import os
import time
from dotenv import load_dotenv

load_dotenv()

# Start timer
start_time = time.time()

# Load .env variables
server = os.getenv("SQL_SERVER")  # e.g. "localhost\\SQLEXPRESS"
database = "US_Airbnb"

# SQL Server connection string
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
    f"CHARSET=UTF8;" # Added to handle potential encoding issues
)
conn = pyodbc.connect(conn_str, autocommit=False) # Changed to autocommit=False for explicit control
cursor = conn.cursor()

# === Load CSVs ===
df = pd.read_csv("AB_US_2023.csv", low_memory=False)

# === Clean Data ===
df["price"] = pd.to_numeric(df["price"], errors="coerce")
# Fill NaN prices with a default value (e.g., 0) or drop them if preferred
df["price"].fillna(0, inplace=True)

df["minimum_nights"] = df["minimum_nights"].astype(int)
df["accommodation_id"] = df["accommodation_id"].astype(str)
df["name"] = df["name"].astype(str)
df["room_type"] = df["room_type"].astype(str)
df["availability_365"] = df["availability_365"].astype(int)
df["neighbourhood"] = df["neighbourhood"].astype(str)
df["latitude"] = df["latitude"].astype(float)
df["longitude"] = df["longitude"].astype(float)
df["city"] = df["city"].astype(str)
df["host_name"] = df["host_name"].astype(str)
df["host_id"] = df["host_id"].astype("int32")
df["calculated_host_listings_count"] = df["calculated_host_listings_count"].astype(int)
df["number_of_reviews"] = df["number_of_reviews"].astype(int)
df["reviews_per_month"] = df["reviews_per_month"].astype(float).fillna(0)
df["number_of_reviews_ltm"] = df["number_of_reviews_ltm"].astype(int)


# === Optimize Host Inserts ===
# Extract unique host data
unique_hosts = df[["host_id", "host_name", "calculated_host_listings_count"]].drop_duplicates()

# Prepare data for executemany
host_data = [tuple(row) for row in unique_hosts.itertuples(index=False)]

# Use executemany for bulk insertion, and handle IF NOT EXISTS within the loop
# This is still row-by-row for the IF NOT EXISTS check, but `executemany` handles the Python loop
# more efficiently than `for _ in df.iterrows()`
print(" ➡️ Inserting/Updating Host data...")
for host_row in host_data:
    try:
        cursor.execute("""
            MERGE INTO dbo.host AS target
            USING (VALUES (?, ?, ?)) AS source (host_id, host_name, calculated_host_listings_count)
            ON (target.host_id = source.host_id)
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (host_id, host_name, calculated_host_listings_count)
                VALUES (source.host_id, source.host_name, source.calculated_host_listings_count);
        """, host_row)
    except pyodbc.IntegrityError as e:
        print(f"Skipping duplicate host_id: {host_row[0]} - {e}")
        conn.rollback() # Rollback in case of an error if autocommit is True, but not strictly needed with MERGE
    except Exception as e:
        print(f"Error inserting host data {host_row}: {e}")
        conn.rollback()

# === Optimize Location and Reviews Inserts ===
# Create dataframes for unique locations and reviews
unique_locations = df[["neighbourhood", "latitude", "longitude", "city"]].drop_duplicates()
unique_reviews = df[["number_of_reviews", "number_of_reviews_ltm", "reviews_per_month"]].drop_duplicates()

# Insert unique locations and retrieve their IDs
print(" ➡️ Inserting Location data and retrieving IDs...")
location_mapping = {}
for _, row in unique_locations.iterrows():
    cursor.execute("""
        INSERT INTO dbo.location (neighbourhood, latitude, longitude, city)
        OUTPUT INSERTED.location_id
        VALUES (?, ?, ?, ?)
    """, row["neighbourhood"], row["latitude"], row["longitude"], row["city"])
    location_id = cursor.fetchone()[0]
    location_mapping[(row["neighbourhood"], row["latitude"], row["longitude"], row["city"])] = location_id

# Insert unique reviews and retrieve their IDs
print(" ➡️ Inserting Review data and retrieving IDs...")
review_mapping = {}
for _, row in unique_reviews.iterrows():
    cursor.execute("""
        INSERT INTO dbo.reviews (number_of_reviews, number_of_reviews_ltm, reviews_per_month)
        OUTPUT INSERTED.review_id
        VALUES (?, ?, ?)
    """, row["number_of_reviews"], row["number_of_reviews_ltm"], row["reviews_per_month"])
    review_id = cursor.fetchone()[0]
    review_mapping[(row["number_of_reviews"], row["number_of_reviews_ltm"], row["reviews_per_month"])] = review_id


# === Optimize Accommodation Inserts ===
print(" ➡️ Preparing Accommodation data for bulk insert...")
accommodation_data = []
for _, row in df.iterrows():
    loc_key = (row["neighbourhood"], row["latitude"], row["longitude"], row["city"])
    rev_key = (row["number_of_reviews"], row["number_of_reviews_ltm"], row["reviews_per_month"])
    location_id = location_mapping.get(loc_key)
    review_id = review_mapping.get(rev_key)

    if location_id is not None and review_id is not None:
        accommodation_data.append((
            row["accommodation_id"],
            row["host_id"],
            row["name"],
            row["room_type"],
            row["price"],
            row["minimum_nights"],
            row["availability_365"],
            location_id,
            review_id
        ))
    else:
        print(f"Skipping accommodation_id {row['accommodation_id']} due to missing location/review ID.")

# Bulk insert for Accommodation
print(f" ➡️ Inserting {len(accommodation_data)} Accommodation records...")
try:
    cursor.executemany("""
        INSERT INTO dbo.accommodation (
            accommodation_id, host_id, name, room_type, price,
            minimum_nights, availability_365, location_id, review_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, accommodation_data)
except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    if sqlstate == '23000': # Integrity Constraint Violation
        print("Error: Duplicate key or constraint violation during accommodation insert. Check your data or table constraints.")
    else:
        print(f"An error occurred during accommodation insert: {ex}")
    conn.rollback() # Rollback on error
finally:
    conn.commit() # Commit after all inserts

# Finalize
cursor.close()
conn.close()

# End timer and report
end_time = time.time()
print(" ✅ All data inserted successfully (or with handled exceptions).")
print(f" ⏱️ Execution time: {end_time - start_time:.2f} seconds")
