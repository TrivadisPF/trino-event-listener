package io.trino.plugin.eventstream;

import io.trino.spi.TrinoWarning;
import io.trino.spi.eventlistener.*;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;

// Those are just a few very crude tests.
// TODO: Add more cases with proper structure.
// TODO: Test actual JSON output, not just its presence.
class EventStreamEventListenerTest {
    private KafkaProducer kafkaProducer;

    @Test
    void queryCreatedEvents() throws IOException {
        // Given there is a listener for query created event
        Map<String,Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", "192.168.142.129:9092");
        properties.put("schema.registry.url","http://192.168.142.129:8081");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProducer = new KafkaProducer(properties);
        EventStreamEventListener listener = new EventStreamEventListener(kafkaProducer);

        // When two events are created
        listener.queryCompleted(prepareQueryCompletedEvent());

        // Then two events should be present in the log file
        //long logEventsCount = Files.lines(Paths.get("target/queryCreatedEvents.log")).count();
        //assertEquals(2, logEventsCount);
    }


    private QueryCompletedEvent prepareQueryCompletedEvent() {
        return new QueryCompletedEvent(
                prepareQueryMetadata(),
                prepareQueryStatistics(),
                prepareQueryContext(),
                prepareQueryIOMetadata(),
                Optional.empty(),
                new ArrayList<TrinoWarning>(),
                Instant.now(),
                Instant.now(),
                Instant.now()
        );
    }

    private SplitCompletedEvent prepareSplitCompletedEvent() {
        return new SplitCompletedEvent(
                "queryId",
                "stageId",
                "taskId",
                Optional.of("catalogName"),
                Instant.now(),
                Optional.of(Instant.now()),
                Optional.of(Instant.now()),
                getSplitStatistics(),
                Optional.empty(),
                "payload"
        );
    }

    private SplitStatistics getSplitStatistics() {
        return new SplitStatistics(
                ofMillis(1000),
                ofMillis(2000),
                ofMillis(3000),
                ofMillis(4000),
                1,
                2,
                Optional.of(Duration.ofMillis(100)),
                Optional.of(Duration.ofMillis(200))
        );
    }

    private QueryStatistics prepareQueryStatistics() {
        return new QueryStatistics(
                ofMillis(1000),
                ofMillis(1000),
                ofMillis(1000),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                100l,
                100l,
                100l,
                100l,
                100l,
                100l,
                100l,
                100l,
                100l,
                100l,
                100l,
                100l,
                100l,
                0,
                new ArrayList<StageGcStatistics>(),
                1,
                true,
                new ArrayList<StageCpuDistribution>(),
                new ArrayList<String>(),
                Optional.empty()
        );
    }

    private QueryIOMetadata prepareQueryIOMetadata() {
        return new QueryIOMetadata(new ArrayList<QueryInputMetadata>(), Optional.empty());
    }

    private QueryMetadata prepareQueryMetadata() {
        return new QueryMetadata(
                "queryId",
                Optional.empty(),
                "query",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "queryState",
                List.of(),
                List.of(),
                URI.create("http://localhost"),
                Optional.empty(), Optional.empty()
        );
    }

    private QueryContext prepareQueryContext() {
        return new QueryContext(
                "user",
                Optional.of("principal"),
                Set.of(), // groups
                Optional.empty(), // traceToken
                Optional.empty(), // remoteClientAddress
                Optional.empty(), // userAgent
                Optional.empty(), // clientInfo
                new HashSet<>(), // clientTags
                new HashSet<>(), // clientCapabilities
                Optional.of("source"),
                Optional.of("catalog"),
                Optional.of("schema"),
                Optional.of(new ResourceGroupId("name")),
                new HashMap<>(), // sessionProperties
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(1000L)),
                "serverAddress", "serverVersion", "environment",
                Optional.of(QueryType.SELECT)
        );
    }
}