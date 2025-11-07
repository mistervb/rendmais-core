package br.com.rendmais.p2p.protocol;

import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.crypto.KeyUtil;
import br.com.rendmais.p2p.registry.PeerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.util.Base64;

public class HandshakeHandler {
    private static final Logger log = LoggerFactory.getLogger(HandshakeHandler.class);
    private final PeerRegistry registry;

    public HandshakeHandler(PeerRegistry registry) {
        this.registry = registry;
    }

    public void handle(SignedMessage signedMessage) {
        try {
            String payload = signedMessage.getPayload();
            PeerInfo peer = new com.google.gson.Gson().fromJson(payload, PeerInfo.class);
            String pubBase64 = peer.getPublicKeyBase64();
            if (pubBase64 == null) {
                log.warn("Handshake without publicKey, ignoring");
                return;
            }
            PublicKey pub = KeyUtil.publicKeyFromBase64(pubBase64);
            byte[] sig = Base64.getDecoder().decode(signedMessage.getSignature());
            boolean ok = KeyUtil.verify(pub, payload.getBytes(java.nio.charset.StandardCharsets.UTF_8), sig);
            if (ok) {
                registry.addPeer(peer);
                log.info("Handshake verified, peer added: {}", peer.getNodeId());
            } else {
                log.warn("Invalid handshake signature from peer {}", peer.getNodeId());
            }
        } catch (Exception e) {
            log.warn("Handshake processing failed: {}", e.getMessage());
        }
    }
}
