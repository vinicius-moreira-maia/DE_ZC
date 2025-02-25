import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# esse decorador faz um agrupamento lógico dos dados, criando um recurso
# 'rides' será o nome do recurso, e será o nome da tabela
@dlt.resource(name="rides") 
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    # retorna um iterador
    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page


# criando a pipeline dlt
pipeline = dlt.pipeline(destination="duckdb")

# executando a pipeline com o novo recurso
# 'ny_taxi' como uma função de primeira ordem (objeto)
load_info = pipeline.run(ny_taxi, write_disposition="replace")
print(load_info)

# visualizando os dados carregados
# ver em um notebook pra visualizar melhor
print(pipeline.dataset(dataset_type="default").rides.df())