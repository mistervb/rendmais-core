package br.com.rendmais.common.dto;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ErrorMessage {
    private String errorCode;
    private String errorMessage;
    private String details;
    private long timestamp;
    private String sourceNodeId;
}