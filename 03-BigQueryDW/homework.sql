-- Question 1
-- 20_332_093 records
SELECT COUNT(*) FROM demo_dataset.yellow_tripdata;

-- Question 2
-- 155.12 MB (estimated)
SELECT DISTINCT PULocationID FROM demo_dataset.yellow_tripdata;

-- 0 MB (BigQuery can't estimate since data is external)
SELECT DISTINCT PULocationID FROM demo_dataset.external_yellow_tripdata;

-- Question 3
SELECT PULocationID FROM demo_dataset.yellow_tripdata; -- 152.12 MB (estimated)
SELECT PULocationID, DOLocationID FROM demo_dataset.yellow_tripdata; -- 310.24 MB (estimated)

/*
Answer is
"BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed."
*/

-- Question 4 
-- 8333 records
SELECT COUNT(*) FROM demo_dataset.yellow_tripdata WHERE fare_amount = 0;

-- Question 5
-- 'Partition by tpep_dropoff_datetime and Cluster on VendorID'

-- Question 6
-- 310.24 MB (estimated)
SELECT DISTINCT VendorID FROM demo_dataset.yellow_tripdata
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- don't know why, but is 310.24 MB also (estimated)
SELECT DISTINCT VendorID FROM demo_dataset.yellow_tripdata_part
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

/*
By elimination i will choose "310.24 MB for non-partitioned table and 26.84 MB for the partitioned table"
*/

-- 7
-- Parquet files are in a bucket, so "GCP Bucket" is the answer

-- 8
-- False, should only use for tables that have considerable amounts of data
