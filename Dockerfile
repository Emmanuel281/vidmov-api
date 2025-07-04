FROM python:3.12.3-alpine

# Install required system dependencies
RUN apk add --no-cache \
    build-base \
    gcc \
    g++ \
    musl-dev \
    python3-dev \
    libffi-dev \
    openssl-dev

# Add your application code
ADD . /app

# Set working directory
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 1899

# Command to run your application
CMD ["python", "main.py"]
