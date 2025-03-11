import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb

'''
- corridas será o nome da tabela
- o nome do bd será 'dlt_' +  nome_do_arquivo + '.duckdb' (SE eu não definir o nome da pipeline)
- o dataset/schema será 'dlt_' +  nome_do_arquivo + '_dataset' (SE eu não definir o nome do dataset)
'''

@dlt.resource(name="corridas") 
def corridas_ny():
    client = RESTClient(base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
                        paginator=PageNumberPaginator( base_page=1, total_path=None))

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

def load_data():
    pipeline = dlt.pipeline(pipeline_name="pipeline_teste", destination="duckdb", dataset_name="taxi_data")
    
    # o 'run' já lida com o iterador automaticamente
    informacoes_carga = pipeline.run(corridas_ny, write_disposition="replace")
    print(informacoes_carga)

    #print(pipeline.dataset(dataset_type="default").corridas.df())
    
    # acessando o nome das tabelas (desse jeito é mais útil com o jupyter) -> BEM MAIS DESCRITIVO
    # aqui é DUCKDB, e não dlt
    conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
    conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
    print(conn.sql("DESCRIBE").df())

def view_data():
    '''
        Essa forma de consultar o duckdb é melhor para o caso do VS Code.
    '''

    conn = duckdb.connect("////home/vmm/DE_ZC/pipeline_teste.duckdb")
    
    # 'df' retorna um data frame do pandas!
    
    # Question 3
    question3 = conn.execute("SELECT COUNT(*) FROM taxi_data.corridas").df()
    print(question3)
    
    # Question 4
    question4 = conn.execute(""" SELECT
                                    AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
                                 FROM 
                                    taxi_data.corridas; """).df()
    print(question4)
    
    # Question 2
    # tabelas = conn.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'taxi_data'").fetchdf()
    # print(tabelas)

if __name__ == "__main__":
    #load_data()
    view_data()