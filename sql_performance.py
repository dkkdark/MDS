import pyodbc
import time
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
server = os.getenv("SQL_SERVER")
database = "US_Airbnb"

# Connection string
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)

print("\n===== QUERY RESULTS AND PERFORMANCE MEASUREMENT =====\n")

# a. Most popular neighbourhoods grouped by min nights (filtered by price)
start = time.time()
query_a = """
SELECT 
    l.neighbourhood,
    a.minimum_nights,
    COUNT(*) AS total_listings,
    AVG(a.price) AS avg_price
FROM accommodation a
JOIN location l ON a.location_id = l.location_id
WHERE a.price > 300
GROUP BY l.neighbourhood, a.minimum_nights
ORDER BY total_listings DESC;
"""
df_a = pd.read_sql(query_a, conn)
print("🔹 a. Most Popular Neighbourhoods (Grouped by Min Nights, Price > 300):\n", df_a.head())
print("⏱️ Time:", round(time.time() - start, 2), "s\n")

# b. Most booked room type
start = time.time()
query_b = """
SELECT 
    room_type,
    COUNT(*) AS total_bookings
FROM accommodation
GROUP BY room_type
ORDER BY total_bookings DESC;
"""
df_b = pd.read_sql(query_b, conn)
print("🔹 b. Most Booked Room Type:\n", df_b.head())
print("⏱️ Time:", round(time.time() - start, 2), "s\n")

# c. Highest and lowest reviews
start = time.time()
query_c_high = """
SELECT TOP 1 * FROM reviews
ORDER BY number_of_reviews DESC;
"""
query_c_low = """
SELECT TOP 1 * FROM reviews
ORDER BY number_of_reviews ASC;
"""
df_c_high = pd.read_sql(query_c_high, conn)
df_c_low = pd.read_sql(query_c_low, conn)
print("🔹 c. Review Extremes:")
print("   🔼 Highest:\n", df_c_high)
print("   🔽 Lowest:\n", df_c_low)
print("⏱️ Time:", round(time.time() - start, 2), "s\n")

# d. Available accommodation
start = time.time()
query_d = """
SELECT 
    accommodation_id, name, availability_365, price
FROM accommodation
WHERE availability_365 > 0
ORDER BY availability_365 DESC;
"""
df_d = pd.read_sql(query_d, conn)
print("🔹 d. Available Accommodations:\n", df_d.head())
print("⏱️ Time:", round(time.time() - start, 2), "s\n")

# e. Most active host
start = time.time()
query_e = """
SELECT 
    host_name,
    calculated_host_listings_count
FROM host
ORDER BY calculated_host_listings_count DESC;
"""
df_e = pd.read_sql(query_e, conn)
print("🔹 e. Most Active Hosts:\n", df_e.head())
print("⏱️ Time:", round(time.time() - start, 2), "s\n")

# Close connection
conn.close()
