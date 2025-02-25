import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import os

'''
Notar que as cargas do bigquery sempre vem acompanhadas da gravação de tabelas/registros adicionais para fazer controle dos estados dos dados inseridos pelo próprio dlt.
'''

'''
O 'dlt' também permite a carga de tabelas 'Delta' e 'Iceberg' (usa as libs  deltalake e pyiceberg). Ele prepara 1 ou mais parquet's durante a extração e a normalização. Na carga, esses parquets são expostos à estrutura de dado 'Arrow' ('Apache Arrow'), antes de serem submetidos a uma das 2 bibliotecas.
'''

os.environ["BUCKET_URL"] = "/home/vmm/DE_ZC/04-WorkshopDlt/loaded_data"

# 'carga_dlt' será o nome do arquivo
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
pipeline = dlt.pipeline(
    pipeline_name='bucket_pipeline',
    destination="filesystem", # para data lakes / sistema de arquivos
    dataset_name='fs_data_delta',
    dev_mode=True
)

# executando a pipeline
# na carga os dados serão transformados em tabelas delta (que são vários parquet's com uma camnada de metadados em cima)
info = pipeline.run(ny_taxi, loader_file_format="parquet", table_format="delta")
print(info)