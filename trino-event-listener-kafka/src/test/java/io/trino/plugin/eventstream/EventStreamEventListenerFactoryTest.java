package io.trino.plugin.eventstream;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventStreamEventListenerFactoryTest {

    @Test
    void getName() {
        EventStreamEventListenerFactory listenerFactory = new EventStreamEventListenerFactory();
        assertEquals("trino-event-listener-kafka", listenerFactory.getName());
    }

    /*
    @Test
    void createWithoutConfigShouldThrowException() {
        // Given
        Map<String, String> configs = new HashMap<>();
        configs.put(EventStreamEventListenerFactory.QUERYLOG_CONFIG_LOCATION, null);
        // When
        EventStreamEventListenerFactory listenerFactory = new EventStreamEventListenerFactory();
        // Then
        assertThrows(
                NullPointerException.class,
                () -> listenerFactory.create(configs),
                EventStreamEventListenerFactory.QUERYLOG_CONFIG_LOCATION + " is null"
        );
    }

     */
}