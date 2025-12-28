package com.raj.mailengine.api.controller;

import com.raj.mailengine.api.dto.EmailRequestDto;
import com.raj.mailengine.service.EmailService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/email")
@RequiredArgsConstructor
public class EmailController {

    private final EmailService emailService;

    @PostMapping("/send")
    public ResponseEntity<?> sendEmail(@Valid @RequestBody EmailRequestDto dto) {
        String messageId = emailService.enqueueEmail(dto);
        return ResponseEntity.accepted().body(Map.of("messageId", messageId));
    }
}
