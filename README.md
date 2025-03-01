# flink-snowflake-connector-sink-app
Pipe streaming data into snowflake table using flink snowflake connector

## Local kafka setup

```sh
docker compose up
```

### Publishing a message to the Kafka topic
Let the topic be `snow-topic` and the message be in `myMessage.txt`
```sh
kcat -b 127.0.0.1:9092 -t snow-topic -P example.txt
````

### Consuming the message from the Kafka topic
```sh
kcat -b 127.0.0.1:9092 -t snow-topic -C -f 'Topic %t, partition %p, offset %o, key %k: %s\n'

kcat -b 127.0.0.1:9092 -t snow-topic -p 0 -o 5 -C -f 'Topic %t, partition %p, offset %o, key %k: %s\n'
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
