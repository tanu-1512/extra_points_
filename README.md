# MinMaxAvgTempInSpark
This repository gives a basic idea of how to get started with spark with a small example of min,max, avg of temperature in spark



# Spark Docker Compose Setup

This Docker Compose configuration allows you to easily deploy an Apache Spark cluster locally using Docker containers.

## Prerequisites

- Docker installed on your machine

## Getting Started


1. Navigate to the cloned directory:

   ```bash
   cd spark-docker-compose
   ```

2. Start the Spark cluster:

   ```bash
   docker-compose up -d
   ```

   This command will start the Spark Master, Worker nodes, and History Server in detached mode.

3. To stop the cluster:

   ```bash
   docker-compose down
   ```

## Accessing the UI

- **Spark Master UI:** Visit [http://localhost:8080](http://localhost:8080) in your web browser to access the Spark Master UI.
- **Spark History Server UI:** Visit [http://localhost:18081](http://localhost:18081) to access the Spark History Server UI after jobs have been run.

## Configuration

- By default, this Docker Compose setup uses the `bde2020/spark-master:3.3.0-hadoop3.3` and `bde2020/spark-worker:3.3.0-hadoop3.3` images for Spark Master and Worker nodes respectively.
- You can customize the configuration by modifying the `docker-compose.yml` file.
## Running the Analysis

1. **Access the Spark Master Container**
   - Use the following command to access the Spark master container:
     ```bash
     docker exec -it spark-master /bin/bash
     ```

2. **Navigate to the Notebooks Directory**
   - Change to the `/notebooks` directory where your Python script is located:
     ```bash
     cd /notebooks
     ```

3. **Submit the Spark Job**
   - Run your Spark application using `spark-submit`:
     ```bash
     /spark/bin/spark-submit TemperatureAnalysis.py /data/2024.csv /data/out
     ```
   - Replace `TemperatureAnalysis.py` with your actual Python script name, if different.
   - `/data/2024.csv` is the input CSV file path inside the container.
   - `/data/out` is the directory inside the container where the output will be saved.
     ![image](https://github.com/aravind2060/MinMaxAvgTempInSpark/assets/38257404/13d7eb8b-35ee-4034-8ace-35516704859e)


## Monitoring and Results

- After submitting the job, you can monitor the execution and performance in the Spark Web UI at `http://localhost:8080`.
- The output of the analysis will be saved in the specified output directory (`/data/out` in this case).
  ![image](https://github.com/aravind2060/MinMaxAvgTempInSpark/assets/38257404/243d2cb4-a555-4a4d-8365-ee22b5926ddf)


## Notes

- The Spark Web UI provides insights into job execution, resource utilization, and detailed task metrics.
- Ensure that the Docker volumes in `docker-compose.yml` are correctly mapped to your local directories for data and notebooks.
