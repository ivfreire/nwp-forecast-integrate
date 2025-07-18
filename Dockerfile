FROM python:3.11-slim

COPY . /app
WORKDIR /app

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends gcc && \
	pip install --upgrade pip && \
	pip install -r requirements.txt && \
	apt-get purge -y --auto-remove gcc && \
	rm -rf /var/lib/apt/lists/*

CMD ["uvicorn", "--host", "0.0.0.0", "--port", "8080", "main:app"]