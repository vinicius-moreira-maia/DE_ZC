import dlt

# imaginando um JSON/dicionário assim
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
        },
        "passengers": [
            {"name": "John", "rating": 4.9},
            {"name": "Jack", "rating": 3.9}
        ]
    }
]

# criando um pipeline dlt com normalização automática
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_data", # pipeline
    destination="duckdb", # bd
    dataset_name="taxi_rides", # dataset / schema
)

# executa a pipeline com os dados brutos
# 'replace' é para substituir os dados
# cria a tabela 'rides'
info = pipeline.run(data, table_name="rides", write_disposition="replace")

# exibindo o sumário da carga
# print(info)
# print(pipeline.last_trace)

# o pipeline produz 2 tabelas, a 'rides' e a 'rides__passengers' (de forma automática), pois listas devem ser quebradas em tabelas filhas
# pra visualizar melhor, usar notebook
print(pipeline.dataset(dataset_type="default").rides.df())
print(pipeline.dataset(dataset_type="default").rides__passengers.df())