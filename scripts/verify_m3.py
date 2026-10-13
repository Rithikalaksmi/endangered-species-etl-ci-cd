import trino

conn = trino.dbapi.connect(
    host="localhost",
    port=8080,
    user="admin",
    catalog="iceberg",
    schema="lakehouse"
)
cur = conn.cursor()

print("--- Iceberg Table Counts ---")
cur.execute("SELECT COUNT(*) FROM fact_observations")
print("fact_observations:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM dead_letter_queue")
print("dead_letter_queue:", cur.fetchone()[0])

print("\n--- Recent Snapshots ---")
cur.execute("""
    SELECT snapshot_id, committed_at, operation, summary['total-records']
    FROM "fact_observations$snapshots"
    ORDER BY committed_at DESC
    LIMIT 5
""")
for row in cur.fetchall():
    print(f"Snapshot: id={row[0]}, committed_at={row[1]}, op={row[2]}, total_records={row[3]}")

print("\n--- Top 5 Species by Count ---")
cur.execute("""
    SELECT animal_category, iucn_status, COUNT(*) 
    FROM fact_observations 
    GROUP BY animal_category, iucn_status 
    ORDER BY COUNT(*) DESC 
    LIMIT 5
""")
for row in cur.fetchall():
    print(f"Species: {row[0]:<25} | IUCN: {row[1]:<18} | Count: {row[2]}")
