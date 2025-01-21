import pandas as pd
import sqlalchemy as sqla
from time import time
import argparse
import os

def main(params):
    user = params.user
    pwd = params.pwd
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    csv_name = "output.csv"

    # -O é para salvar já renomeado
    os.system(f"wget {url} -O {csv_name}")
    
    df = pd.read_csv(csv_name, compression='gzip')
    
    # criando conexão com o postgres local (conteiner)
    engine = sqla.create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=10_000)
    
    # next() é função de iteradores no python
    df = next(df_iter)

    # criando a tabela (linha 0 são os metadados)
    df.head(n=0).to_sql(name=table_name, 
                        con=engine,
                        if_exists='replace')

    # quando não houver mais em que iterar, vai levantar uma exceção
    while True:
        t_start = time()
    
        df = next(df_iter)
    
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    
        df.to_sql(name=table_name, 
                  con=engine,
                  if_exists='append')
    
        t_end = time()
    
        print(f"{len(df)} linhas inseridas em {t_end - t_start} segundos...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão de dados de CSV no Postgres")

    # usuario, senha, host, porta, nome do bd, nome da tabela
    # url do csv
    parser.add_argument('--user', help='usuario postgres')
    parser.add_argument('--pwd', help='senha postgres')
    parser.add_argument('--host', help='host postgres')
    parser.add_argument('--port', help='porta postgres')
    parser.add_argument('--db', help='bd postgres')
    parser.add_argument('--table_name', help='tabela destino')
    parser.add_argument('--url', help='url do CSV')

    args = parser.parse_args()
    
    main(args)

