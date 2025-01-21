## 1 - Running python container and checking pip version
```bash
docker run -it --entrypoint=bash python:3.12.8
```
```bash
pip --version
```
**Result:**
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)

## 2 - Containers in network with docker compose 

**PGAdmin should use 'db' as the hostname** because that is the name of the service defined in the YAML file, and it's the name that should be used by other services within the same Docker network.
**The port number is '5432'**, because that is the port the PostgreSQL service is listening on inside the container.

## Preparing Postgres

In the folder 'postgres-prepare' are all the files that i used to prepare postgres with all homework data.

Commands used:

1. to put pgadmin and postgres up
```bash
docker compose up -d
```

2. to create the image based on the ingestion script
```bash
docker build -t taxi_ingest:v001 .
```

3. creating variable to store the dataset URL (did that for the taxi-zone table also)
```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
```

4. executing container (did that for the taxi-zone table also)
```bash
docker run -it \
  --network=01-dockerpostgresgcpterraform_default \
  taxi_ingest:v001 \
    --user=root \
    --pwd=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green-tripdata \
    --url="${URL}"

docker run -it \
  --network=01-dockerpostgresgcpterraform_default \
  taxi_ingest:v001 \
    --user=root \
    --pwd=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=taxi-zone \
    --url="${URL}"
```

## Questions 5 to 6 are the .sql files

## 7 - Sequence of commands in Terraform

As saw in the classes, that's the correct sequence of commands (plan is optional):
'terraform init, terraform apply -auto-approve, terraform destroy
