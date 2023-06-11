FROM python:3.9

RUN  mkdir proyecto_coderhouse
RUN  cd  proyecto_coderhouse

WORKDIR  /proyecto_coderhouse

RUN pip install requests  
RUN pip install pandas
RUN pip install psycopg2
RUN pip install python-dotenv

ADD proyecto_coderhouse.py .
ADD .env .

CMD ["python", "./proyecto_coderhouse.py"]