package com.raj.mailengine.service;

import com.raj.mailengine.api.dto.EmailRequestDto;
import com.raj.mailengine.mapper.EmailMapper;
import com.raj.mailengine.model.EmailRequest;
import com.raj.mailengine.producer.EmailProducer;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EmailService {

    private final EmailProducer emailProducer;

    public String enqueueEmail(EmailRequestDto emailRequestDto) {
        EmailRequest domain = EmailMapper.toDomain(emailRequestDto);
        emailProducer.sendEmail(domain);
        return domain.getMessageId();
    }

}
