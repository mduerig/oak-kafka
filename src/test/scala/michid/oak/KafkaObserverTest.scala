package michid.oak

import java.util.Properties

import org.apache.jackrabbit.api.JackrabbitRepository
import org.apache.jackrabbit.oak.jcr.Jcr

import javax.jcr.Repository
import javax.jcr.SimpleCredentials
import kafka.producer.Producer
import kafka.producer.ProducerConfig

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
object KafkaObserverTest {

  def main(args: Array[String]) {
    val producer = createProducer()
    val repository = createRepository(producer)
    val session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()))
    session.getRootNode.addNode("new node " + System.currentTimeMillis())
    session.save()
    session.logout()
    shutdown(repository)
  }

  private def createProducer(): Producer[String, String] = {
    val properties = new Properties()
    properties.put("metadata.broker.list", "localhost:9092")
    properties.put("serializer.class", "kafka.serializer.StringEncoder")
    new Producer[String, String](new ProducerConfig(properties))
  }

  private def createRepository(producer: Producer[String, String]): Repository = {
    new Jcr().`with`(new KafkaObserver("/", producer)).createRepository()
  }

  private def shutdown(repository: Repository) {
    if (repository.isInstanceOf[JackrabbitRepository]) {
      repository.asInstanceOf[JackrabbitRepository].shutdown()
    }
  }
}