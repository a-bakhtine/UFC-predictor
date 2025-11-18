FROM python:3.12-slim

# install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# set working directory inside the container
WORKDIR /app

# unbuffered mode better for logs
ENV PYTHONUNBUFFERED=1

# copy and install Python dependencies 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy the rest of the project
COPY . .

# so can run scripts manually
CMD ["/bin/bash"]