package michid.oak

import java.util.Dictionary
import java.util.Properties

import org.apache.felix.scr.annotations.Activate
import org.apache.felix.scr.annotations.Component
import org.apache.felix.scr.annotations.Deactivate
import org.apache.felix.scr.annotations.Property
import org.apache.felix.scr.annotations.Service
import org.apache.jackrabbit.oak.spi.commit.CommitInfo
import org.apache.jackrabbit.oak.spi.commit.Observer
import org.apache.jackrabbit.oak.spi.state.NodeState
import org.osgi.service.component.ComponentContext

import kafka.producer.Producer
import kafka.producer.ProducerConfig

/**
 * This `Observer` instance wraps a [[michid.oak.KafkaObserver]] into
 * an OSGi service.
 */
@Component(specVersion = "1.1", immediate = true, metatype = true)
@Service(value = Array(classOf[Observer]))
class KafkaObserverService extends Observer {

  val METADATA_BROKER_LIST_DEFAULT = "localhost:9092"

  @Property(
    name = "metadata.broker.list",
    description = "See http://kafka.apache.org/documentation.html#producerconfigs",
    value = Array("localhost:9092"))
  val METADATA_BROKER_LIST = "metadata.broker.list"

  val SERIALIZER_CLASS_DEFAULT = "kafka.serializer.StringEncoder"

  @Property(
    name = "serializer.class",
    description = "See http://kafka.apache.org/documentation.html#producerconfigs",
    value = Array("kafka.serializer.StringEncoder"))
  val SERIALIZER_CLASS = "serializer.class"

  var delegate: Option[Observer] = None

  @Activate
  protected def activate(context: ComponentContext) {
    val properties = context.getProperties
    val producerProps = new Properties
    producerProps.put(METADATA_BROKER_LIST,
      getString(properties, METADATA_BROKER_LIST, METADATA_BROKER_LIST_DEFAULT))
    producerProps.put(SERIALIZER_CLASS,
      getString(properties, SERIALIZER_CLASS, SERIALIZER_CLASS_DEFAULT))
    val producer = new Producer[String, String](new ProducerConfig(producerProps))
    delegate = Some(new KafkaObserver("/", producer))
  }

  private def getString(properties: Dictionary[_, _], key: String, defaultValue: String): String = {
    val value = properties.get(key)
    if (key == null) defaultValue else value.toString
  }

  @Deactivate
  def deactivate() {
    delegate = None
  }

  override def contentChanged(nodeState: NodeState, commitInfo: CommitInfo) {
    delegate.foreach(_.contentChanged(nodeState, commitInfo))
  }
}