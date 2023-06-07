FROM python:3.9

ADD proyecto_coderhouse.py .
ADD keys.py .

RUN pip install requests  
RUN pip install pandas
RUN pip install psycopg2

CMD ["python", "./proyecto_coderhause"]

# FROM apache/airflow:2.3.3

# ADD webserver_config.py /opt/airflow/webserver_config.py

# USER root
# RUN apt-get update \
#   && apt-get install -y --no-install-recommends \
#          vim \
#   && apt-get autoremove -yqq --purge \
#   && apt-get clean \
#   && rm -rf /var/lib/apt/lists/*
# USER airflow