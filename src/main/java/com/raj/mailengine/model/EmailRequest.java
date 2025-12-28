package com.raj.mailengine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmailRequest {

    private String messageId;
    private String idempotencyKey;
    private String from;
    private String to;
    private String subject;
    private String body;
    private long timestamp;
    private String clientId;
}
