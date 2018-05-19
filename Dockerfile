FROM ubuntu:17.10

RUN apt-get update && apt-get install -y python3-pip iputils-ping telnet vim less

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY app /app

WORKDIR /app

ENTRYPOINT ["python3"]
CMD ["cassandra_migration.py"]
