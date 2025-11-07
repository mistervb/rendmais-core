package br.com.rendmais.common.dto;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PeerInfo {
    private String nodeId;
    private String address;
    private int port;
    // opcional no registro inicial, mas presente no handshake
    private String publicKeyBase64;
}
