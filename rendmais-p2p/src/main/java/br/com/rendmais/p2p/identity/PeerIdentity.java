package br.com.rendmais.p2p.identity;

import br.com.rendmais.common.dto.PeerInfo;
import lombok.Getter;

import java.util.UUID;

@Getter
public class PeerIdentity {
    private final PeerInfo info;

    public PeerIdentity(String address, int port) {
        this.info = PeerInfo.builder()
                .nodeId(UUID.randomUUID().toString())
                .address(address)
                .port(port)
                .build();
    }
}
