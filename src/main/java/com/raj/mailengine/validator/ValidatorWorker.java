package com.raj.mailengine.validator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.raj.mailengine.model.EmailRequest;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Component
public class ValidatorWorker {

    private Thread workerThread;
    private volatile boolean running = true;
    private final ObjectMapper mapper = new ObjectMapper();

    @PostConstruct
    public void start() {
        workerThread = new Thread(this::runLoop, "validator-worker");
        workerThread.setDaemon(true);
        workerThread.start();
        log.info("ValidatorWorker Started.");
    }

    private void runLoop() {
        // ------------------
        // CONSUMER CONFIG
        // ------------------
        Properties cprops = new Properties();
        cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        cprops.put(ConsumerConfig.GROUP_ID_CONFIG, "validator-group");
        cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cprops);
        consumer.subscribe(Collections.singletonList("email.requests"));

        // ------------------
        // PRODUCER CONFIG
        // ------------------

        Properties pprops = new Properties();
        pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pprops.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        KafkaProducer<String, String> producer = new KafkaProducer<>(pprops);

        try {
            while(running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) continue;

                for(ConsumerRecord<String, String> record: records) {
                    try {
                        EmailRequest request = mapper.readValue(record.value(), EmailRequest.class);

                        boolean valid = isValid(request);

                        if (valid) {
                            ProducerRecord<String, String> out = new ProducerRecord<>("email.outbound", record.key(), record.value());
                            out.headers().add("validated-by", "validator-worker".getBytes());
                            producer.send(out);
                        } else {
                            ProducerRecord<String, String> invalid = new ProducerRecord<>("email.invalid", record.key(), record.value());
                            producer.send(invalid);
                        }
                    } catch (Exception e) {
                        log.error("Error validating message. Sending to invalid.", e);
                        ProducerRecord<String, String> invalid =
                                new ProducerRecord<>("email.invalid", record.key(), record.value());
                        producer.send(invalid);
                    }
                }
                // flush and commit
                producer.flush();
                consumer.commitSync();
            }
        } catch (WakeupException we) {
            // ignored on shutdown
        } finally {
            producer.close();
            consumer.close();
            log.info("Validator Worker stopped");
        }

    }


    private boolean isValid(EmailRequest req) {
        if (req == null) return false;
        if (req.getTo() == null || !req.getTo().contains("@")) return false;
        if (req.getSubject() == null || req.getSubject().isBlank()) return false;
        if (req.getBody() == null || req.getBody().isBlank()) return false;
        return true;
    }
}
