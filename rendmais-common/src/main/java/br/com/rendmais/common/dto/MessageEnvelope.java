package br.com.rendmais.common.dto;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MessageEnvelope {
    private String type;
    private String payload;
}
