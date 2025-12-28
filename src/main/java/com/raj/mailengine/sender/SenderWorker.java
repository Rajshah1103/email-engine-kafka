package com.raj.mailengine.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raj.mailengine.exception.PermanentEmailException;
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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Component
public class SenderWorker {

    private Thread workerThread;
    private volatile boolean running = true;
    private final ObjectMapper mapper = new ObjectMapper();

    @PostConstruct
    public void start() {
        workerThread = new Thread(this::runLoop, "sender-worker");
        workerThread.setDaemon(true);
        workerThread.start();
        log.info("SenderWorker Started.");
    }

    private void runLoop() {
        // ------------------
        // CONSUMER CONFIG
        // ------------------
        Properties cprops = new Properties();
        cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        cprops.put(ConsumerConfig.GROUP_ID_CONFIG, "sender-group");
        cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cprops);
        consumer.subscribe(Collections.singletonList("email-outbound"));

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

                if(records.isEmpty()) {
                    continue;
                }
                for(ConsumerRecord<String, String> record : records) {
                    try {
                        EmailRequest request = mapper.readValue(record.value(), EmailRequest.class);
                        sendEmail(request);
                    } catch (PermanentEmailException pe) {
                        log.error("Permanent failure. Sending to dead.", pe);
                        ProducerRecord<String, String> dead =
                                new ProducerRecord<>("email.dead", record.key(), record.value());
                        producer.send(dead);
                    } catch (Exception te) {
                        log.warn("Transient failure. Sending to retry.", te);

                        ProducerRecord<String, String> retry =
                                new ProducerRecord<>("email.retry", record.key(), record.value());
                        retry.headers().add("retry-count", ByteBuffer.allocate(4).putInt(0).array());
                        retry.headers().add("last-error", "TRANSIENT_FAILURE".getBytes());
                        retry.headers().add("processed-by", "sender-worker".getBytes());
                        retry.headers().add("timestamp", ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array());
                        producer.send(retry);
                    }
                }
                // IMPORTANT: flush + commit AFTER processing
                producer.flush();
                consumer.commitSync();
            }
        } catch (WakeupException we) {

        } finally {
            producer.close();
            consumer.close();
            log.info("SenderWorker stopped.");
        }
    }

    // ------------------
    // EMAIL SEND LOGIC
    // ------------------

    private void sendEmail(EmailRequest request) {
        simulateLatency();

        if(request.getTo().endsWith("@baddomain.com")) {
            throw new PermanentEmailException("Invalid Email domain");
        }

        if (Math.random() < 0.2) {
            throw new RuntimeException("SMTP timeout");
        }
    }

    private void simulateLatency() {
        try {
            Thread.sleep(300);
        } catch (InterruptedException interruptedException) {

        }
    }
}
