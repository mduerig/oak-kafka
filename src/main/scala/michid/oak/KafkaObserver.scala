package michid.oak

import org.apache.jackrabbit.oak.plugins.observation.filter.VisibleFilter.VISIBLE_FILTER

import scala.collection.JavaConversions.iterableAsScalaIterable

import scala.collection.mutable.ArrayBuffer

import org.apache.jackrabbit.oak.api.PropertyState
import org.apache.jackrabbit.oak.commons.PathUtils.concat
import org.apache.jackrabbit.oak.commons.PathUtils.elements
import org.apache.jackrabbit.oak.namepath.GlobalNameMapper
import org.apache.jackrabbit.oak.namepath.NamePathMapper
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl
import org.apache.jackrabbit.oak.plugins.observation.DefaultEventHandler
import org.apache.jackrabbit.oak.plugins.observation.EventGenerator
import org.apache.jackrabbit.oak.plugins.observation.EventHandler
import org.apache.jackrabbit.oak.plugins.observation.FilteredHandler
import org.apache.jackrabbit.oak.plugins.observation.filter.VisibleFilter
import org.apache.jackrabbit.oak.spi.commit.CommitInfo
import org.apache.jackrabbit.oak.spi.commit.Observer
import org.apache.jackrabbit.oak.spi.state.NodeState
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import kafka.producer.KeyedMessage
import kafka.producer.Producer

/**
 * This `Observer` implementation sends all changes to an
 * Jackrabbit Oak repository to a Kafka server.
 *
 * @see http://kafka.apache.org/documentation.html#producerconfigs
 */
class KafkaObserver(val path: String, val producer: Producer[String, String]) extends Observer {
  val LOG: Logger = LoggerFactory.getLogger(classOf[KafkaObserver])

  private val messages = ArrayBuffer.empty[KeyedMessage[String, String]]

  private var previousRoot: NodeState = _

  override def contentChanged(root: NodeState, info: CommitInfo) {
    if (previousRoot != null) {
      val namePathMapper = new NamePathMapperImpl(new GlobalNameMapper(root))

      var before = previousRoot
      var after = root
      var handler: EventHandler = new FilteredHandler(
        VISIBLE_FILTER, new NodeEventHandler("/", namePathMapper))

      val oakPath = namePathMapper.getOakPath(path)
      if (oakPath == null) {
        LOG.warn("Cannot listen for changes on invalid path: {}", path)
        return
      }

      for (oakName <- elements(oakPath)) {
        before = before.getChildNode(oakName)
        after = after.getChildNode(oakName)
        handler = handler.getChildHandler(oakName, before, after)
      }

      messages += new KeyedMessage[String, String]("commit", "commit", toString(info))

      val generator = new EventGenerator(before, after, handler)
      while (!generator.isDone) {
        generator.generate()
        sendPendingMessages()
      }
      sendPendingMessages()
    }

    previousRoot = root
  }

  private def sendPendingMessages() {
    messages.foreach(producer.send(_))
    messages.clear()
  }

  private def toString(info: CommitInfo): String = {
    if (info == null) CommitInfo.EMPTY.toString
    else info.toString
  }

  sealed trait EventType
  case object NODE_ADDED extends EventType
  case object NODE_DELETED extends EventType
  case object NODE_CHANGED extends EventType
  case object PROPERTY_ADDED extends EventType
  case object PROPERTY_DELETED extends EventType
  case object PROPERTY_CHANGED extends EventType

  private def itemChanged(eventType: EventType, jcrPath: String) {
    messages += new KeyedMessage[String, String](eventType.toString, eventType.toString, jcrPath)
  }

  private class NodeEventHandler private (val path: String, val namePathMapper: NamePathMapper,
                                          val eventType: EventType) extends DefaultEventHandler {

    def this(path: String, namePathMapper: NamePathMapper) {
      this(path, namePathMapper, NODE_CHANGED)
    }

    private def this(parent: NodeEventHandler, name: String, eventType: EventType) {
      this(if ("/" == parent.path) '/' + name else parent.path + '/' + name, parent.namePathMapper,
        eventType)
    }

    override def getChildHandler(name: String, before: NodeState, after: NodeState): EventHandler = {
      if (!before.exists()) new NodeEventHandler(this, name, NODE_ADDED)
      else if (!after.exists()) new NodeEventHandler(this, name, NODE_DELETED)
      else new NodeEventHandler(this, name, NODE_CHANGED)
    }

    override def enter(before: NodeState, after: NodeState) = eventType match {
      case NODE_ADDED => itemChanged(NODE_ADDED, jcrPath(path))
      case NODE_DELETED => itemChanged(NODE_DELETED, jcrPath(path))
      case NODE_CHANGED => itemChanged(NODE_CHANGED, jcrPath(path))
      case _ => throw new IllegalStateException(eventType.toString)
    }

    override def propertyAdded(after: PropertyState) {
      itemChanged(PROPERTY_ADDED, jcrPath(path, after.getName))
    }

    override def propertyChanged(before: PropertyState, after: PropertyState) {
      itemChanged(PROPERTY_CHANGED, jcrPath(path, after.getName))
    }

    override def propertyDeleted(before: PropertyState) {
      itemChanged(PROPERTY_DELETED, jcrPath(path, before.getName))
    }

    private def jcrPath(path: String): String = namePathMapper.getJcrPath(path)

    private def jcrPath(parent: String, name: String): String = jcrPath(concat(path, name))
  }

}