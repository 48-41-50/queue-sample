FROM python:3.7
RUN pip install psycopg2-binary requests ipython

COPY ./server /opt/server

CMD ["python3", "/opt/server/queue_server.py"]

