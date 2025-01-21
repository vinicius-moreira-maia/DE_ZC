FROM python:3.9 

RUN apt-get install wget 
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app

COPY script.py script.py

ENTRYPOINT [ "python", "script.py" ]