import pandas as pd
import sqlalchemy as sqla
from time import time

# convertendo em csv para facilitar
# taxi_data_df = pd.read_parquet("yellow_tripdata_2021-01.parquet")
# taxi_data_df.to_csv("yellow_tripdata_2021-01.csv", index=False)

# 1369769 de linhas
taxi_data_df = pd.read_csv("yellow_tripdata_2021-01.csv")
# print(taxi_data_df.info())

# convertendo as datas de string para datetime
taxi_data_df["tpep_pickup_datetime"] = pd.to_datetime(taxi_data_df["tpep_pickup_datetime"])
taxi_data_df["tpep_dropoff_datetime"] = pd.to_datetime(taxi_data_df["tpep_pickup_datetime"])

# print(taxi_data_df.dtypes)

# criando conexão com o postgres local (conteiner)
engine = sqla.create_engine("postgresql://root:root@localhost:5432/ny_taxi")

# criando/checando o DDL a partir do data frame
# recebe o data frame e o nome da tabela
# 'con=engine' faz com que o pandas reconheça o banco de dados, que é postgres no caso, e forneça um DDL compatível
taxi_data_schema = pd.io.sql.get_schema(taxi_data_df, name="YellowTaxiData", con=engine)
# print(taxi_data_schema)

# 1369769 (muitas linhas)
# o pandas fornece mecanismos de particionamento dos dados em "chunks"/pedaços
# também fornece a opção de criar um iterador a partir do data frame
# 10000, '_' é só formatação
# não é mais um dataframe, é um iterador com dataframes de 10000 linhas
df_iter = pd.read_csv("yellow_tripdata_2021-01.csv", iterator=True, chunksize=10_000)

# next() é função de iteradores no python
df = next(df_iter)
# print(df)
# print(len(df))

# retorna apenas os nomes das colunas
# print(df.head(n=0))

# criando a tabela
df.head(n=0).to_sql(name="YellowTaxiData", 
                    con=engine,
                    if_exists='replace')

# quando não houver mais em que iterar, vai levantar uma exceção
while True:
    t_start = time()
    
    df = next(df_iter)
    
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    
    df.to_sql(name="YellowTaxiData", 
              con=engine,
              if_exists='append')
    
    t_end = time()
    
    print(f"{len(df)} linhas inseridas em {t_end - t_start} segundos...")
    

