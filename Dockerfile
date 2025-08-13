FROM python:3.9-slim

WORKDIR /app

RUN pip install psutil grpcio grpcio-tools

COPY./app /app

RUN pip install psutil

CMD ["python", "node.py"]