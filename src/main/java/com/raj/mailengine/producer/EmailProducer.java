package com.raj.mailengine.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raj.mailengine.model.EmailRequest;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EmailProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String TOPIC = "email.requests";

    public void sendEmail(EmailRequest request) {
        try {
            String key = shardKey(request.getTo());
            String json = mapper.writeValueAsString(request);
            ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC, key, json);
            record.headers().add("idempotency-key", request.getIdempotencyKey().getBytes());
            record.headers().add("message-id", request.getMessageId().getBytes());
            kafkaTemplate.send(record);
        } catch (Exception e ) {
            throw new RuntimeException(e);
        }
    }

    private String shardKey(String to) {
        return to; // simple per-recipient ordering
    }
}
