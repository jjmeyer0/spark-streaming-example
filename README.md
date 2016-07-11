This is a simple maven project that incorporates Spark Streaming (Kafka),
Spark Testing Base, and Scala Test.

To setup a cluster please follow:

https://cwiki.apache.org/confluence/display/AMBARI/Quick+Start+Guide

With the above create a cluster with three nodes.

Note: that if you are using IntelliJ version 16 doesn't properly import this project. 
Version 15 works just fine.

## Creating Topic
```bash
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper c6401.ambari.apache.org:2181 --replication-factor 1 --partitions 1 --topic item
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper c6401.ambari.apache.org:2181 --replication-factor 1 --partitions 1 --topic processed_item
```


## Pushing message on topic
```bash
$ /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list c6401.ambari.apache.org:6667 --topic item
{"id":1,"name":"name","description":"description"}
```


Please see the below links for more information:

* https://spark-packages.org/package/holdenk/spark-testing-base 
* https://github.com/holdenk/spark-testing-base
* http://allegro.tech/2015/08/spark-kafka-integration.html


Testing the Application


Maven:

```bash 
$ mvn clean test
```


Running the Application

1. First make sure you have setup a cluster and created the two topics above.
2. Below shows how to run with Maven or Gradle

    * Maven:
    
    ```bash
    $ mvn clean install exec:java -Dexec.mainClass="com.jj.streaming.ItemApp"
    ```
