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
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An EventListener wraps Kafka producer to send query events to Kafka
 */
public class EventStreamEventListener implements EventListener
{
    private static Logger logger = LogManager.getLogger(EventStreamEventListener.class);
    private final KafkaProducer kafkaProducer;
    // TODO make this topic name configurable
    private static final String TOPIC_TRINO_EVENT = "trino.event";

    public EventStreamEventListener(KafkaProducer<String, Object> kafkaProducer)
    {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        logger.info("handle query completed event");
        com.trivadis.trino.avro.QueryCompletedEvent completed = com.trivadis.trino.avro.QueryCompletedEvent.newBuilder()
                .setQueryType("abc")
                .setQuery(queryCompletedEvent.getMetadata().getQuery())
                .setQueryID(queryCompletedEvent.getMetadata().getQueryId())
                .setQueryStartTime(queryCompletedEvent.getCreateTime())
                .setQueryEndTime(queryCompletedEvent.getEndTime())
                .setOutputRows(queryCompletedEvent.getStatistics().getOutputRows())
                .build();
        try {
            logger.info("about to send message");
            kafkaProducer.send(
                    new ProducerRecord<>(TOPIC_TRINO_EVENT,
                            null,   // TODO decide for a better key
                            completed));
        }
        catch (Exception e) {
            logger.error(e);
            System.out.println(e);
        }
        logger.debug("Sent queryCompleted event. query id %s", queryCompletedEvent.getMetadata().getQueryId());
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
    }
}
