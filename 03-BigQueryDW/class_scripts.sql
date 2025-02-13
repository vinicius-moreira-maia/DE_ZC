-- external table é usada para consultar dados fora do BigQuery sem precisar replicar os dados

-- criando tabelas com base em arquivos parquet contidos no bucket
-- '*' é coringa para quaisquer caracteres
-- o BQ não conhece detalhes como aramazenamento, número de linhas, etc. de EXTERNAL TABLES, pois a fonte de dados em si está fora do escopo do bq
CREATE OR REPLACE EXTERNAL TABLE demo_dataset.external_yellow_tripdata
OPTIONS (
  format = 'PARQUET',
  uris = ['https://storage.cloud.google.com/taxi-rides-ny-448023-terra-bucket/yellow_tripdata_2024-*.parquet'] 
);

SELECT * FROM demo_dataset.external_yellow_tripdata limit 10; -- NÃO é bom consultar assim

-- criando uma tabela materializada a partir de tabela externa
CREATE OR REPLACE TABLE demo_dataset.yellow_tripdata AS
SELECT * FROM demo_dataset.external_yellow_tripdata;

SELECT * FROM demo_dataset.yellow_tripdata limit 10;

-- criando uma tabela particionada a partir de tabela externa
CREATE OR REPLACE TABLE demo_dataset.yellow_tripdata_part
PARTITION BY
  DATE(tpep_pickup_datetime) AS -- granularidade default é por dia
SELECT * FROM demo_dataset.external_yellow_tripdata;

-- Impactos do particionamento
-- 310.24 MB de dados processados
SELECT DISTINCT(VendorID)
FROM demo_dataset.yellow_tripdata
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-06-30';

-- 54 MB de dados processados
SELECT DISTINCT(VendorID)
FROM demo_dataset.yellow_tripdata_part
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-06-01' AND '2024-06-30';

-- consultando as partições criadas
SELECT table_name, partition_id, total_rows
FROM `demo_dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_part'
ORDER BY total_rows DESC;

-- criando tabela particionada E clusterizada, a partir da tabela externa
CREATE OR REPLACE TABLE demo_dataset.yellow_tripdata_part_clust
PARTITION BY 
    DATE(tpep_pickup_datetime)
CLUSTER BY 
    VendorID 
AS
SELECT * FROM demo_dataset.external_yellow_tripdata;

-- 310.24 MB processados na tabela apenas particionada
SELECT count(*) as trips
FROM demo_dataset.yellow_tripdata_part
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-06-30' AND VendorID=1;

-- 261.76 MB processados na tabela particionada E clusterizada
SELECT count(*) as trips
FROM demo_dataset.yellow_tripdata_part_clust
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-06-30' AND VendorID=1;