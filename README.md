Oak Kafka
=========

Apache Kafka integration for
[Apache Jackrabbit Oak](http://jackrabbit.apache.org/oak/).

This is a prove of concept implementation of an Oak Observer sending all
changes to a Oak repository to a Kafka message log.

Getting started
---------------
To get started with Oak Kafka, build the latest sources with Maven 3 and Java
6 (or higher):

    mvn clean install

Then start a local Kafka server. Change into the Kafka installation directory
and start a Zookeeper instance and then the Kafka server:

    bin/zookeeper-server-start.sh config/zookeeper.properties
    bin/kafka-server-start.sh config/server.properties

To see the output of the test you need to attach a message consumer to Kafka:

    ./bin/kafka-console-consumer.sh --zookeeper localhost:2181 --blacklist \
    none --property print.key=true

Now running `KafkaObserverTest.main` should dump the changes to the console:

    commit	CommitInfo{sessionId=session-6, userId=admin, date=1404470600166, info={}}
    NODE_CHANGED	/
    PROPERTY_ADDED	/new node 1404470600164/jcr:primaryType
    NODE_ADDED	/new node 1404470600164
    NODE_CHANGED	/oak:index
    NODE_CHANGED	/oak:index/nodetype

OSGi integration
----------------
Through `KafkaObserverService` Oak Kafka can be run inside an OSGi environment
(e.g. [Apache Sling](http://sling.apache.org)). Note that due to
[OAK-1950](https://issues.apache.org/jira/browse/OAK-1950) Oak Kafka currently
has a dependency on `oak-core:1.1-SNAPSHOT`.
