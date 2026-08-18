package com.tracemind.connector.web;

import com.tracemind.connector.contract.CanonicalSignalV1;
import com.tracemind.connector.kafka.CanonicalSignalPublisher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = {CanonicalSignalPublisher.TOPIC},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class AlertIngestControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void validAlertIsAcceptedAndPublishedWithCorrectPartitionKey() throws Exception {
        String payload = """
                {
                  "status": "firing",
                  "alerts": [
                    {
                      "status": "firing",
                      "labels": {
                        "alertname": "DB_CONNECTION_PRESSURE",
                        "service": "payment-service",
                        "environment": "prod",
                        "severity": "CRITICAL",
                        "instance": "payment-service-2"
                      },
                      "annotations": {
                        "summary": "Connection pool utilization reached 100%"
                      },
                      "startsAt": "2026-08-15T14:03:00Z",
                      "endsAt": "0001-01-01T00:00:00Z",
                      "generatorURL": "http://prometheus/graph",
                      "fingerprint": "abc123fingerprint"
                    }
                  ]
                }
                """;

        String responseBody = mockMvc.perform(post("/integrations/prometheus/alerts")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.eventIds", hasSize(1)))
                .andReturn().getResponse().getContentAsString();

        String eventId = responseBody.split("\"eventIds\":\\[\"")[1].split("\"")[0];

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, CanonicalSignalV1> consumer = new KafkaConsumer<>(
                consumerProps, new StringDeserializer(), new JsonDeserializer<>(CanonicalSignalV1.class, false))) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, CanonicalSignalPublisher.TOPIC);
            ConsumerRecord<String, CanonicalSignalV1> record =
                    KafkaTestUtils.getSingleRecord(consumer, CanonicalSignalPublisher.TOPIC);

            assertThat(record.key()).isEqualTo("prod:payment-service");
            assertThat(record.value().eventId()).isEqualTo(eventId);
            assertThat(record.value().signalType()).isEqualTo("DB_CONNECTION_PRESSURE");
        }
    }

    @Test
    void missingRequiredLabelIsRejectedWith400() throws Exception {
        String payload = """
                {
                  "status": "firing",
                  "alerts": [
                    {
                      "status": "firing",
                      "labels": {
                        "alertname": "DB_CONNECTION_PRESSURE",
                        "service": "payment-service"
                      },
                      "annotations": {},
                      "startsAt": "2026-08-15T14:03:00Z",
                      "fingerprint": "abc123fingerprint"
                    }
                  ]
                }
                """;

        mockMvc.perform(post("/integrations/prometheus/alerts")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest());
    }

    @Test
    void emptyAlertsArrayIsRejectedWith400() throws Exception {
        mockMvc.perform(post("/integrations/prometheus/alerts")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\": \"firing\", \"alerts\": []}"))
                .andExpect(status().isBadRequest());
    }
}
