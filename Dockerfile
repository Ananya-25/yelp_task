# Use an official PySpark runtime as the base image
FROM python:3.8


WORKDIR /nycs

COPY requirements.txt .
RUN apt-get update -y
RUN apt install default-jdk -y
RUN pip install -r requirements.txt

COPY . /nycs/nycs



# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH /nycs

# Define default command to run when the container starts
CMD ["python3", "core/app.py"]
