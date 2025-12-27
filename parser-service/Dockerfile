FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libxml2 \
    libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY parser_worker.py .
COPY parser_logic/ parser_logic/

CMD ["python", "parser_worker.py"]

