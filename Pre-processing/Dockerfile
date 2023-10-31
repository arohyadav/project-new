FROM python:3.9 AS builder

WORKDIR /app

COPY requirements.txt /app

RUN python -m venv /env

RUN /env/bin/pip install --no-cache-dir -r requirements.txt


FROM python:3.9

WORKDIR /app

COPY --from=builder /env /env

COPY data_processing /app/data_processing

COPY main.py /app

COPY dataplatform.json /app

CMD ["/env/bin/python", "main.py"]
