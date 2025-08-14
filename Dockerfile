FROM python:3.9-slim

WORKDIR /app

RUN pip install psutil grpcio grpcio-tools cryptography

COPY ./app /app

CMD ["python", "node.py"]