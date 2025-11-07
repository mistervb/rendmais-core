package br.com.rendmais.common.dto;

import br.com.rendmais.common.enums.MessageType;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SignedMessage {
    private MessageType type;      // ex: HANDSHAKE, HEARTBEAT (usar MessageType.name())
    private String payload;   // JSON string
    private String signature; // base64 signature over payload
}
