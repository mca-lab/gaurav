
FROM python:3.11-slim


WORKDIR /app


COPY . /app


RUN pip install --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

# Run the full pipeline (fetch + clean)
CMD ["python", "src/run_pipeline.py"]
