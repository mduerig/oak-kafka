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

import java.util.Dictionary;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.service.component.ComponentContext;

/**
 * This {@code Observer} instance wraps a {@link KafkaObserver} into
 * an OSGi service.
 */
@Component(immediate = true, metatype = true)
@Service(value = Observer.class)
public class KafkaObserverService implements Observer {
    private Observer delegate;

    public static final String METADATA_BROKER_LIST_DEFAULT = "localhost:9092";
    @Property(
        description = "See http://kafka.apache.org/documentation.html#producerconfigs",
        value = METADATA_BROKER_LIST_DEFAULT)
    public static final String METADATA_BROKER_LIST = "metadata.broker.list";

    public static final String SERIALIZER_CLASS_DEFAULT = "kafka.serializer.StringEncoder";
    @Property(
            description = "See http://kafka.apache.org/documentation.html#producerconfigs",
            value = SERIALIZER_CLASS_DEFAULT)
    public static final String SERIALIZER_CLASS = "serializer.class";


    @Activate
    protected void activate(ComponentContext context) {
        Dictionary<?,?> properties = context.getProperties();
        Properties producerProps = new Properties();
        producerProps.put(METADATA_BROKER_LIST,
                getString(properties, METADATA_BROKER_LIST, METADATA_BROKER_LIST_DEFAULT));
        producerProps.put(SERIALIZER_CLASS,
                getString(properties, SERIALIZER_CLASS, SERIALIZER_CLASS_DEFAULT));
        Producer<String, String> producer =
                new Producer<String, String>(new ProducerConfig(producerProps));
        delegate = new KafkaObserver("/", producer);
    }

    private static String getString(Dictionary<?, ?> properties, String key, String defaultValue) {
        Object value = properties.get(key);
        return key == null
            ? defaultValue
            : value.toString();
    }

    @Deactivate
    protected void deactivate() {
        delegate = null;
    }

    @Override
    public void contentChanged(NodeState nodeState, CommitInfo commitInfo) {
        if (delegate != null) {
            delegate.contentChanged(nodeState, commitInfo);
        }
    }
}
