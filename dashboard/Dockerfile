FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN useradd --create-home app
USER app
WORKDIR /home/app/dashboard

COPY --chown=app requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt \
 # pre-download DuckDB's S3 extension so the app doesn't fetch it at runtime
 && python -c "import duckdb; duckdb.connect().sql('INSTALL httpfs')"
ENV PATH=/home/app/.local/bin:$PATH

COPY --chown=app . .

EXPOSE 8501
HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
