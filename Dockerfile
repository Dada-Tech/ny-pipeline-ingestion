FROM python:3.11

RUN apt-get install wget

RUN pip install pandas sqlalchemy pyarrow psycopg2

# set the working dir
WORKDIR /app

# copies the ingest_data.py to the docker conainer as second argument, 'ingest_data.py'
COPY ingest_data.py ingest_data.py

# ENTRYPOINT rather than CMD, so you can append arguments
ENTRYPOINT ["python", "ingest_data.py"]