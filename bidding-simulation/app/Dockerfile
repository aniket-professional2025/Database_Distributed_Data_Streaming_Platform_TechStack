FROM python:3.11-slim
WORKDIR /app
# system deps for psycopg2 and cassandra driver
# RUN apt-get update && apt-get install -y build-essential libpq-dev gcc curl default-libmysqlclient-dev librdkafka-dev netcat && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
# Expose metrics and streamlit port
EXPOSE 8000 8501
# entrypoint: run streamlit app (the streamlit app itself will start metrics server and background consumers)
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.headless=true"]