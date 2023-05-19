FROM python:3.9

ADD proyecto_coderhause.py .
ADD keys.py .

RUN pip install requests pandas psycopg2 time json

CMD ["python", "./proyecto_coderhause"]