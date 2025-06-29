FROM python:3.11-bullseye
 
WORKDIR /app
 
# Install system dependencies and SQL Server ODBC driver
RUN apt-get update && \
    apt-get install -y curl apt-transport-https gnupg lsb-release unixodbc unixodbc-dev && \
    # Add Microsoft signing key
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    # Add Microsoft repository
    echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/repos/microsoft-debian-bullseye-prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list && \
    # Update and install ODBC driver
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    # Clean up
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
 
# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
# Copy application code
COPY . .
 
# Expose port
EXPOSE 8005
 
# Start the application
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8004"]
 