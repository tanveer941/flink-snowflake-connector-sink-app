# flink-snowflake-connector-sink-app
Pipe streaming data into snowflake table using flink snowflake connector

## Local kafka setup

```sh
cd local-kafka-testing
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


### Java and maven version
Java app is built using maven. The dependencies are added in the `pom.xml` file.
Java and maven version are specified in the `pom.xml` file.
```angular2html
Apache Maven 3.9.9
Maven home: /opt/homebrew/Cellar/maven/3.9.9/libexec
Java version: 23.0.1, vendor: Homebrew, runtime: /opt/homebrew/Cellar/openjdk/23.0.1/libexec/openjdk.jdk/Contents/Home
Default locale: en_IN, platform encoding: UTF-8
OS name: "mac os x", version: "15.3.1", arch: "aarch64", family: "mac"
```

## Building the Java app
The output jar is built in the `target` directory
```sh
cd snowflake_mapping
mvn package
```

### Mapping the snowflake table columns, Kafka JSON message and the Java app serialization mapper class

## Running the JAR in the local Flink cluster
```sh
# Submit a jar to the cluster
./flink run /Users/you/path/to/target/jarfile.jar
```

## Send the Kafka message and verifying it in Snowflake table
