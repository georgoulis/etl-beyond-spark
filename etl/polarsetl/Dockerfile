FROM python:3.11

# Install system dependencies (for pyarrow, deltalake, etc.)
RUN apt-get update && apt-get install -y \
    ca-certificates\
    iputils-ping\
    build-essential \
    libssl-dev \
    libcurl4-openssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create working dir
WORKDIR /app

# Copy files
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

# Entry point
ENTRYPOINT ["/bin/sh", "-c", "python"]
