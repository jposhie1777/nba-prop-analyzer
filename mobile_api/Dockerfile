FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

ENV PYTHONUNBUFFERED=1

# IMPORTANT: module path includes mobile_api
CMD ["python", "-m", "uvicorn", "mobile_api.main:app", "--host", "0.0.0.0", "--port", "8080"]