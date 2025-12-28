package com.raj.mailengine.mapper;

import com.raj.mailengine.api.dto.EmailRequestDto;
import com.raj.mailengine.model.EmailRequest;

import java.util.UUID;

public class EmailMapper {

    private EmailMapper() {};

    public static EmailRequest toDomain(EmailRequestDto dto) {
        String messageId = UUID.randomUUID().toString();
        String idempotencyKey = (dto.getIdempotencyKey() != null && !dto.getIdempotencyKey().isBlank())
                ? dto.getIdempotencyKey() : messageId;
        EmailRequest e = new EmailRequest();
        e.setMessageId(messageId);
        e.setIdempotencyKey(idempotencyKey);
        e.setFrom(dto.getFrom());
        e.setTo(dto.getTo());
        e.setSubject(dto.getSubject());
        e.setBody(dto.getBody());
        e.setTimestamp(System.currentTimeMillis());
        e.setClientId(dto.getClientId());

        return  e;
    }
}
