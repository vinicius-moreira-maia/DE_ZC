import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


def paginated_getter():
    
    # para a classe RESTClient eu forneço a url e o objeto paginator (tipo PageNumerPaginator), que define a estratégia de paginação
    # começa da página 1, 'total_path=None' pois a documentação não trás a info de quantas páginas, portanto irá parar quando não houver mais dado 
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(base_page=1, total_path=None)
    )

    # paginate é um gerador
    for page in client.paginate("data_engineering_zoomcamp_api"): 
        yield page 

# a função retorna um iterador (função geradora)
for page_data in paginated_getter():
    print(page_data)