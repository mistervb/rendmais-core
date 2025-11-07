package br.com.rendmais.p2p.protocol;

import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.p2p.messaging.Message;
import br.com.rendmais.p2p.registry.PeerRegistry;

public class HandshakeHandler {
    private final PeerRegistry registry;

    public HandshakeHandler(PeerRegistry registry) {
        this.registry = registry;
    }

    public void handle(Message message) {
        PeerInfo peer = new com.google.gson.Gson().fromJson(message.getPayload(), PeerInfo.class);
        registry.addPeer(peer);
    }
}
