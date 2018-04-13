FROM ubuntu:17.10
RUN apt-get update && apt-get install -y python3-pip

COPY app /app
WORKDIR /app
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3"]
CMD ["cassandra_migration.py"]
