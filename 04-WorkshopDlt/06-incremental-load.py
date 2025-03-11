import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

'''
Sempre que eu executo a pipeline, o dlt mantém guardado o último registro inserido, no caso o valor da coluna 'Trip_Dropoff_DateTime'.
'''

# 'incremental' é um filtro que permite buscar os dados após uma certa data (parâmetros são o nome da coluna e o valor inicial da data)
# cursor_date = dlt.sources.incremental("Trip_Dropoff_DateTime", initial_value="2009-06-15")

# o filtro é definido como parâmetro da função
@dlt.resource(name="rides", write_disposition="append")
def ny_taxi( cursor_date=dlt.sources.incremental( "Trip_Dropoff_DateTime",   # coluna a ser controlada
                                                  initial_value="2009-06-15",   # data de início
                                                )):
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

# definindo a pipeline
pipeline = dlt.pipeline(pipeline_name="ny_taxi", destination="duckdb", dataset_name="ny_taxi_data")

# executando a pipeline
load_info = pipeline.run(ny_taxi)
print(pipeline.last_trace)

# checando o valor mais cedo da coluna de data
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            MIN(trip_dropoff_date_time)
            FROM rides;
            """
        )
    print(res)