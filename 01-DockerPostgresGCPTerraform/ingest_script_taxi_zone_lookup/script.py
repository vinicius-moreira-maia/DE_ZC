import pandas as pd
import sqlalchemy as sqla

def main():
    df = pd.read_csv("taxi_zone_lookup.csv")
    
    engine = sqla.create_engine("postgresql://root:root@localhost:5432/ny_taxi")
    
    # checando o schema da tabela
    df_schema = pd.io.sql.get_schema(df, name="zone_lookup", con=engine)
    #print(df_schema)
    
    # poucas linhas...
    # print(len(df))
    
    # criando a tabela
    df.head(n=0).to_sql(name="zone_lookup", 
                        con=engine,
                        if_exists='replace')
    
    # inserindo os dados
    df.to_sql(name="zone_lookup", 
              con=engine,
              if_exists='append')
    
if __name__ == "__main__":
    main()