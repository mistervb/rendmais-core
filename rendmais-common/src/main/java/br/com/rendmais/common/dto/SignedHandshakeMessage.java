package br.com.rendmais.common.dto;

import br.com.rendmais.common.enums.MessageType;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SignedHandshakeMessage {
    private MessageType type;
    private PeerInfo peerInfo;
    private String signature;
    private long timestamp;
    private String challenge;
    private String challengeResponse;
}