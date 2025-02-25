import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

'''
Criando essa variável de ambiente eu autentico o acesso ao BigQuery através do dlt (ele reconhece essa variável de ambiente)
export DESTINATION__BIGQUERY__CREDENTIALS="/caminho/para/seu/arquivo.json"
'''

# 'carga_dlt' será o nome da tabela
@dlt.resource(name="carga_dlt", write_disposition="replace")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

# na definição da pipeline eu incluo o destino
# ele criou um novo dataset aqui (não acessou o dataset existente)
pipeline = dlt.pipeline(
    pipeline_name='taxi_data',
    destination='bigquery',
    dataset_name='demo_dataset',
    dev_mode=True
)

info = pipeline.run(ny_taxi)
print(info)