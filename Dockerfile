FROM python:3.11.4

RUN apt-get update && \
    apt-get -y install gcc mono-mcs && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /scan_analytics_api

WORKDIR /scan_analytics_api

COPY requirements.txt .

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

COPY . .

WORKDIR app

CMD ["uvicorn", "main:app", "--host=0.0.0.0", "--port=8511"]