/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package michid.oak;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import java.util.List;

import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.observation.DefaultEventHandler;
import org.apache.jackrabbit.oak.plugins.observation.EventGenerator;
import org.apache.jackrabbit.oak.plugins.observation.EventHandler;
import org.apache.jackrabbit.oak.plugins.observation.FilteredHandler;
import org.apache.jackrabbit.oak.plugins.observation.filter.VisibleFilter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@code Observer} implementation sends all changes to an
 * Jackrabbit Oak repository to a Kafka server.
 *
 * @see <a href="http://kafka.apache.org/documentation.html#producerconfigs">Kafka Producer Configs</a>
 */
public class KafkaObserver implements Observer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaObserver.class);

    private final List<KeyedMessage<String, String>> messages = Lists.newArrayList();

    private final String path;
    private final Producer<String, String> producer;

    private NodeState previousRoot;

    public KafkaObserver(String path, Producer<String, String> producer) {
        this.path = path;
        this.producer = producer;
    }

    @Override
    public void contentChanged(NodeState root, CommitInfo info) {
        if (previousRoot != null) {
            NamePathMapper namePathMapper = new NamePathMapperImpl(
                    new GlobalNameMapper(root));

            NodeState before = previousRoot;
            NodeState after = root;
            EventHandler handler = new FilteredHandler(
                    new VisibleFilter(),
                    new NodeEventHandler("/", namePathMapper));

            String oakPath = namePathMapper.getOakPath(path);
            if (oakPath == null) {
                LOG.warn("Cannot listen for changes on invalid path: {}", path);
                return;
            }

            for (String oakName : PathUtils.elements(oakPath)) {
                before = before.getChildNode(oakName);
                after = after.getChildNode(oakName);
                handler = handler.getChildHandler(oakName, before, after);
            }

            messages.add(new KeyedMessage<String, String>("commit", "commit", toString(info)));

            EventGenerator generator = new EventGenerator(before, after, handler);
            while (!generator.isDone()) {
                generator.generate();
                sendPendingMessages();
            }
            sendPendingMessages();
        }

        previousRoot = root;
    }

    private void sendPendingMessages() {
        if (!messages.isEmpty()) {
            producer.send(messages);
            messages.clear();
        }
    }

    private static String toString(CommitInfo info) {
        return info == null
                ? CommitInfo.EMPTY.toString()
                : info.toString();
    }

    private enum EventType {
        NODE_ADDED, NODE_DELETED, NODE_CHANGED,
        PROPERTY_ADDED, PROPERTY_DELETED, PROPERTY_CHANGED}

    void itemChanged(EventType eventType, String jcrPath) {
        messages.add(new KeyedMessage<String, String>(eventType.toString(), eventType.toString(), jcrPath));
    }

    private class NodeEventHandler extends DefaultEventHandler {
        private final String path;
        private final NamePathMapper namePathMapper;
        private final EventType eventType;

        public NodeEventHandler(String path, NamePathMapper namePathMapper) {
            this(path, namePathMapper, EventType.NODE_CHANGED);
        }

        private NodeEventHandler(NodeEventHandler parent, String name, EventType eventType) {
            this("/".equals(parent.path) ? '/' + name : parent.path + '/' + name, parent.namePathMapper, eventType);
        }

        private NodeEventHandler(String path, NamePathMapper namePathMapper, EventType eventType) {
            this.path = path;
            this.namePathMapper = namePathMapper;
            this.eventType = eventType;
        }

        @Override
        public EventHandler getChildHandler(String name, NodeState before, NodeState after) {
            if (!before.exists()) {
                return new NodeEventHandler(this, name, EventType.NODE_ADDED);
            } else if (!after.exists()) {
                return new NodeEventHandler(this, name, EventType.NODE_DELETED);
            } else {
                return new NodeEventHandler(this, name, EventType.NODE_CHANGED);
            }
        }

        @Override
        public void enter(NodeState before, NodeState after) {
            switch (eventType) {
                case NODE_ADDED:
                    itemChanged(EventType.NODE_ADDED, jcrPath(path));
                    break;
                case NODE_DELETED:
                    itemChanged(EventType.NODE_DELETED, jcrPath(path));
                    break;
                case NODE_CHANGED:
                    itemChanged(EventType.NODE_CHANGED, jcrPath(path));
                    break;
                default:
                    throw new IllegalStateException(eventType.toString());
            }
        }

        @Override
        public void propertyAdded(PropertyState after) {
            itemChanged(EventType.PROPERTY_ADDED, jcrPath(path, after.getName()));
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            itemChanged(EventType.PROPERTY_CHANGED, jcrPath(path, after.getName()));
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            itemChanged(EventType.PROPERTY_DELETED, jcrPath(path, before.getName()));
        }

        private String jcrPath(String path) {
            return namePathMapper.getJcrPath(path);
        }

        private String jcrPath(String parent, String name) {
            return jcrPath(concat(path, name));
        }

    }

}
