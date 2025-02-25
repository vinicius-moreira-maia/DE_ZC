import duckdb

# conectando a um banco duckdb em meória
conn = duckdb.connect("ny_taxi_manual.db")

# criando a tabela, já com a estrutura normalizada
conn.execute("""
CREATE TABLE IF NOT EXISTS rides (
    record_hash TEXT PRIMARY KEY,
    vendor_name TEXT,
    pickup_time TIMESTAMP,
    dropoff_time TIMESTAMP,
    start_lon DOUBLE,
    start_lat DOUBLE,
    end_lon DOUBLE,
    end_lat DOUBLE
);
""")

data = [
    {
        "vendor_name": "VTS",
        "record_hash": "b00361a396177a9cb410ff61f20015ad",
        "time": {
            "pickup": "2009-06-14 23:23:00",
            "dropoff": "2009-06-14 23:48:00"
        },
        "coordinates": {
            "start": {"lon": -73.787442, "lat": 40.641525},
            "end": {"lon": -73.980072, "lat": 40.742963}
        }
    }
]

# tratamento de dados para inserção
flattened_data = [
    (
        ride["record_hash"],
        ride["vendor_name"],
        ride["time"]["pickup"],
        ride["time"]["dropoff"],
        ride["coordinates"]["start"]["lon"],
        ride["coordinates"]["start"]["lat"],
        ride["coordinates"]["end"]["lon"],
        ride["coordinates"]["end"]["lat"]
    )
    for ride in data
]

# inserção
conn.executemany("""
INSERT INTO rides (record_hash, vendor_name, pickup_time, dropoff_time, start_lon, start_lat, end_lon, end_lat)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", flattened_data)

print("Data successfully loaded into DuckDB!")

# consultando dados inseridos
# retorna um data frame do pandas!
df = conn.execute("SELECT * FROM rides").df()

conn.close()