## Data Streams Java Consumer examples

This project provides examples on how to consume data from the data streams product with java. 
Examples include basic implementations using a plain kafka consumer or a kafka streams application with the Json format.

### Configuration

In `application.properties` you'll find the basic configuration for running the examples. 
To consume data from any of your streams you will have to adapt the configuration as follows:
* `consumer-group` - The consumer group as provided by the Data Streams Frontend
* `endpoints` - Kafka Broker host list provided by Webtrekk
  * May be retrieved from the Data Streams Overview page
* `topic` - the Topic you want to consume
  * Topics available for your account are displayed on the the Data Streams Overview page
* `username` - The username as provided by the Data Streams Frontend
* `password` - Your password
  * May be retrieved from the Data Streams Accounts - Password change page

### Execution

The Application of your choice may be executed as follows.
```
mvn compile exec:java -Dexec.mainClass=[main-class]
```
```
// e.g.
mvn compile exec:java -Dexec.mainClass=com.webtrekk.datastreams.examples.KafkaConsumerJsonExample
mvn compile exec:java -Dexec.mainClass=com.webtrekk.datastreams.examples.KafkaStreamsJsonExample
```