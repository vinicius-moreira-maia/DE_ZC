import pandas as pd
import sqlalchemy as sqla
from time import time
import argparse
import os
import sys

def main(params):
    user = params.user
    pwd = params.pwd
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    engine = sqla.create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")
    
    csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")
    
    if url.endswith(".csv.gz"):
        df_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=10_000, encoding='latin1')
        
        df_header = next(df_iter)

        # creating table
        df_header.head(n=0).to_sql(name=table_name, 
                                   con=engine,
                                   if_exists='replace')
        
        insert_big_data_set(df_iter, table_name, engine)
        
    else:
        df = pd.read_csv(csv_name, encoding='latin1')
        
        df.head(n=0).to_sql(name=table_name, 
                            con=engine,
                            if_exists='replace')
        
        df.to_sql(name=table_name, 
                  con=engine,
                  if_exists='append')
        
        sys.exit(f"\nall data was correctly inserted")

def insert_big_data_set(df_iter, table_name, engine):
    try:
        # ingesting data
        while True:
            t_start = time()
    
            df = next(df_iter)
    
            df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
            df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    
            df.to_sql(name=table_name, 
                      con=engine,
                      if_exists='append')
    
            t_end = time()
    
            print(f"{len(df)} lines inserted in {t_end - t_start} seconds...")
            
    except StopIteration as e:
        print(f"\nall data was correctly inserted: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSV ingestion into PostgreSQL")

    parser.add_argument('--user', help='postgres user')
    parser.add_argument('--pwd', help='postgres password')
    parser.add_argument('--host', help='postgres host')
    parser.add_argument('--port', help='postgres port')
    parser.add_argument('--db', help='postgres db')
    parser.add_argument('--table_name', help='table name')
    parser.add_argument('--url', help='CSV url')

    args = parser.parse_args()
    
    main(args)

