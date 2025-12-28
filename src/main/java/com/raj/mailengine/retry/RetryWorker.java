package com.raj.mailengine.retry;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Component
public class RetryWorker {

    private Thread WorkerThread;
    private volatile boolean running = true;

    private static final int MAX_RETRIES = 3;
    private final ObjectMapper mapper = new ObjectMapper();

    @PostConstruct
    public void start() {
        WorkerThread = new Thread(this::runLoop, "retry-worker");
        WorkerThread.setDaemon(true);
        WorkerThread.start();
        log.info("RetryWorker started");
    }

    private void runLoop() {
        // ------------------
        // CONSUMER CONFIG
        // ------------------
        Properties cprops = new Properties();
        cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        cprops.put(ConsumerConfig.GROUP_ID_CONFIG, "retry-group");
        cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cprops);
        consumer.subscribe(Collections.singletonList("email.retry"));

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

                for (ConsumerRecord<String, String> record : records) {
                    handleRecords(record, producer);
                }
                // flush + commit AFTER batch
                producer.flush();
                consumer.commitSync();
            }
        } catch (WakeupException we) {

        } finally{
            producer.close();
            consumer.close();
            log.info("RetryWorker Stopped.");
        }
    }

    private void handleRecords (ConsumerRecord<String, String> record, KafkaProducer<String, String> producer) {
        try {
            int retryCount = getRetryCount(record);

            if (retryCount >= MAX_RETRIES) {
                log.warn("Max retries exceeded. Sending to DLQ. key={}", record.key());
                send(record, producer, "email.dead", retryCount, "MAX_RETRIES_EXCEEDED");
                return;
            }

            long backOffMs = backOffMs(retryCount);
            log.info("Retrying key={} after {} ms", record.key(), backOffMs);
            Thread.sleep(backOffMs);

            send(record, producer, "email.outbound", retryCount + 1, "RETRY");

        } catch (Exception e) {
            log.error("RetryWorker failed. Will reprocess.", e);
            throw new RuntimeException(e); // no commit → replay
        }
    }

    private int getRetryCount(ConsumerRecord<String, String> record) {
        Header h = record.headers().lastHeader("retry-count");
        if(h == null) return 0;
        return ByteBuffer.wrap(h.value()).getInt();
    }

    private long backOffMs(int retryCount) {
        return 1000L * (retryCount + 1); // 1s, 2s, 3s
    }

    private byte[] intToBytes(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    private byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    private void send(
            ConsumerRecord<String, String> record,
            KafkaProducer<String, String> producer,
            String topic,
            int retryCount,
            String reason
    ) {
        ProducerRecord<String, String> out =
                new ProducerRecord<>(topic, record.key(), record.value());

        // copy headers
        record.headers().forEach(h ->
                out.headers().add(h.key(), h.value())
        );

        // overwrite retry metadata
        out.headers().remove("retry-count");
        out.headers().add("retry-count", intToBytes(retryCount));
        out.headers().add("last-error", reason.getBytes());
        out.headers().add("processed-by", "retry-worker".getBytes());
        out.headers().add("timestamp", longToBytes(System.currentTimeMillis()));

        producer.send(out);
    }


}
