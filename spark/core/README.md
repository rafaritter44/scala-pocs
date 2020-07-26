# Spark Core

## Building

`$ sbt clean assembly`

## Running

### Locally

`$ spark-submit target/scala-2.11/TotalSpentByCustomer-assembly-1.0.0.jar`

### On Amazon EMR

1. Create S3 bucket
1. Upload .jar
1. Upload .csv
1. Spin up EMR cluster
- Release: emr-5.28.0
- Applications: Spark 2.4.4 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.2
5. Connect to the master node using SSH
1. Run the following commands:
- `$ aws s3 cp s3://{YOUR_S3_BUCKET}/TotalSpentByCustomer-assembly-1.0.0.jar .`
- `$ spark-submit TotalSpentByCustomer-assembly-1.0.0.jar s3n://{YOUR_S3_BUCKET}/customer-orders.csv`
