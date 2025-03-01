# flink-snowflake-connector-sink-app
Pipe streaming data into snowflake table using flink snowflake connector
![Architecture Diagram](local-kafka-docker/local-kafka-flink-snowflake-architecture.png)

## Local kafka setup

```sh
cd local-kafka-testing
docker compose up
```


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
Snowflake credentials are put in the main function of the java app, in the file `FlinkSnowflakeJob.java`.
It becomes important to map the JSON data produced by kafka into the Java class so that it dumps into snowflake seamlessly.

* Make sure the snowflake destination table created has column names that match exactly as the JSON keys of the Kafka data stream
* If the message to produced to the kafka topic is in myMessage.txt. The contents of the text file are as given below then the Snowflake table definition should be as subsequently shown.
* Note: The JSON keys and snowflake table columns should not haveany special character apart from underscrore.

## Running the JAR in the local Flink cluster
```sh
# Submit a jar to the cluster
./flink run /Users/you/path/to/target/jarfile.jar
```

## Send the Kafka message and verifying it in Snowflake table
### Publishing a message to the Kafka topic
Let the topic be `snow-topic` and the message be in `myMessage.txt`
```sh
kcat -b 127.0.0.1:9092 -t snow-topic -P myMessage.txt
````