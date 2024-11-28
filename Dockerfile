# Use the official Python image as a base
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install FastAPI and any other dependencies
RUN pip install --no-cache-dir fastapi[standard]
RUN pip install pandas networkx requests

# Expose the port that FastAPI will run on
EXPOSE 8000

# Command to run the FastAPI application
CMD ["fastapi", "run", "dbfiddle_api.py", "--host", "0.0.0.0", "--port", "8000"]
