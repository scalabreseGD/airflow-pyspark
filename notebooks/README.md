# Jupyter Notebooks for Spark Cluster

This directory contains Jupyter notebooks for interacting with the Spark cluster.

## Getting Started

### 1. Start the Jupyter Container

```bash
docker-compose up -d jupyter
```

### 2. Access Jupyter

Open your browser and navigate to: http://localhost:8888

No password is required (configured for development use).

### 3. Available Notebooks

- **spark_cluster_demo.ipynb**: Comprehensive demo showing how to:
  - Connect to the Spark cluster
  - Create and manipulate DataFrames
  - Save/read data from MinIO (S3)
  - Create and query Hive tables
  - Work with managed and external tables

## Environment Details

The Jupyter container is configured with:
- **Spark Master**: spark://spark-master:7077
- **MinIO (S3)**: http://minio:9000
- **Hive Metastore**: thrift://hive-metastore:9083
- **AWS Credentials**: admin/admin123

## Monitoring

- Spark Master UI: http://localhost:8080
- Spark Application UI: http://localhost:4040 (when jobs are running)
- MinIO Console: http://localhost:9001 (login: admin/admin123)
- Jupyter Lab: http://localhost:8888

## Tips

1. The `spark-apps` directory is mounted read-only, so you can reference existing PySpark scripts
2. The notebooks are persisted in the `./notebooks` directory
3. All notebooks share the same Spark cluster resources
4. Use the Spark UI to monitor your jobs and debug performance issues

## Troubleshooting

### Spark Session Won't Connect
- Ensure spark-master container is running: `docker-compose ps spark-master`
- Check Spark Master UI at http://localhost:8080

### Can't Access MinIO
- Verify MinIO is running: `docker-compose ps minio`
- Check MinIO Console at http://localhost:9001

### Hive Metastore Issues
- Check metastore logs: `docker logs hive-metastore`
- Verify connection: `docker exec hive-metastore nc -zv hive-metastore 9083`

## Adding New Notebooks

Simply create new `.ipynb` files in this directory. They will automatically be available in Jupyter.
