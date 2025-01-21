# imagem base
FROM python:3.9 

# comandos após iniciar a execução
RUN apt-get install wget 
RUN pip install pandas sqlalchemy psycopg2 pyarrow

# define o contexto do conteiner
WORKDIR /app

# nome na fonte e nome no destino
# o destino é no contexto de WORKDIR
COPY ingest_data.py ingest_data.py

# primeiro comando a ser executado
# ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python", "ingest_data.py" ]