# flink-snowflake-connector-sink-app
Pipe streaming data into snowflake table using flink snowflake connector

## Local kafka setup

```sh
docker compose up
```

## Local Flink cluster setup

```sh
mkdir ~/flink-test && cd ~/flink-test
wget https://dlcdn.apache.org/flink/flink-1.19.1/flink-1.19.1-bin-scala_2.12.tgz
tar zxf flink-1.19.1-bin-scala_2.12.tgz 
cd flink-1.19.1/bin
``` 

```sh
# You can visit http://localhost:8081/ now to see your status, running jobs, etc
./start-cluster.sh
``` 

```sh
# Submit a jar to the cluster
./flink run /Users/you/path/to/target/jarfile.jar
```

## Building the Java app

### Java and maven version

### command to build the Java app

### Mapping the snowflake table columns, Kafka JSON message and the Java app serialization mapper class

## Running the JAR in the local Flink cluster

## Send the Kafka message and verifying it in Snowflake table
