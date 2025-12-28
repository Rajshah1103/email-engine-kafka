package com.raj.mailengine.api.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmailRequestDto {

    @Size(max = 128)
    private String idempotencyKey;

    @NotBlank
    private String from;

    @NotBlank
    @Email
    private String to;

    @NotBlank
    @Size(max = 200)
    private String subject;

    @NotBlank
    private String body;


    private String clientId;
}
