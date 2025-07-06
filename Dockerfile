# Use the official Python 3.10 image as the base
FROM python:3.10-slim

# Set a working directory in the container
WORKDIR /app

# Install system dependencies (if any), for example for pandas and sqlalchemy
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your pipeline code and data into the container
COPY . .

# By default, run the main script (adjust filename if needed)
CMD ["python", "your_script_name.py"]
