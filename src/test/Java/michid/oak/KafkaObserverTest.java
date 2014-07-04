/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package michid.oak;

import java.util.Properties;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.jcr.Jcr;

/**
 * A local Kafka server is needed to run this test. For Kafka 0.8.1.1
 * change into the Kafka installation directory and start a Zookeeper
 * instance and then the Kafka server:
 * <pre>
 *     bin/zookeeper-server-start.sh config/zookeeper.properties
 *     bin/kafka-server-start.sh config/server.properties
 * </pre>
 * To see the output of the test you need to attach a message consumer
 * to Kafka:
 * <pre>
 *     ./bin/kafka-console-consumer.sh --zookeeper localhost:2181 --blacklist none \
 *     --property print.key=true
 * </pre>
 */
public class KafkaObserverTest {
    private KafkaObserverTest() { }

    public static void main(String[] args) throws RepositoryException {
        Producer<String, String> producer = createProducer();
        Repository repository = createRepository(producer);

        Session session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        session.getRootNode().addNode("new node " + System.currentTimeMillis());
        session.save();
        session.logout();

        shutdown(repository);
    }

    private static Producer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        return new Producer<String, String>(new ProducerConfig(properties));
    }

    private static Repository createRepository(Producer<String, String> producer) {
        return new Jcr()
                .with(new KafkaObserver("/", producer))
                .createRepository();
    }

    private static void shutdown(Repository repository) {
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
    }

}
