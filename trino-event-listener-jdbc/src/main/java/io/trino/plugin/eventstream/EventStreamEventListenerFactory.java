/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.eventstream;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EventStreamEventListenerFactory implements EventListenerFactory
{
    private static Logger logger = LogManager.getLogger(EventStreamEventListenerFactory.class);
    // private static final String REGEX_CONFIG_PREFIX = "^event-stream.";

    @Override
    public String getName()
    {
        return "trino-event-listener-kafka";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        KafkaProducer<String, Object> kafkaProducer; // = createKafkaProducer(toKafkaConfig(config));
        Map<String,Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", "192.168.142.129:9092");
        properties.put("schema.registry.url","http://192.168.142.129:8081");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProducer = new KafkaProducer(properties);
        EventStreamEventListener listener = new EventStreamEventListener(kafkaProducer);

        return new EventStreamEventListener(kafkaProducer);
    }

    /**
     * Transform event listener configuration into a Kafka configuration.
     * @param config event listener configuration object
     * @return Map<String, Object>
     */
    private static Map<String, Object> toKafkaConfig(Map<String, String> config)
    {
        Map<String, Object> builder = new HashMap<>();

        Iterator<String> it = config.keySet().iterator();

        while (it.hasNext()) {
            String key = it.next();
            // String kafkaConfigKey = key.replaceFirst(REGEX_CONFIG_PREFIX,
            //         "");
            logger.debug("Loading event-listener config %s", key);
            builder.put(key, config.get(key));
        }

        // TODO design ways to config/code serializer
        logger.info ("builder config created");
        return builder;
    }

    private KafkaProducer<String, Object> createKafkaProducer(Map<String, Object> properties)
    {
        return new KafkaProducer<>(properties);
    }
}
