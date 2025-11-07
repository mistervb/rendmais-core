package br.com.rendmais.p2p.protocol;

import br.com.rendmais.common.crypto.KeyUtil;
import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.identity.PeerIdentity;
import br.com.rendmais.p2p.registry.PeerRegistry;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.util.Base64;

public class HandshakeHandler {

    private static final Logger log = LoggerFactory.getLogger(HandshakeHandler.class);
    private final PeerRegistry registry;
    private final PeerIdentity self;
    private final Gson gson = new Gson();

    public HandshakeHandler(PeerRegistry registry, PeerIdentity self) {
        this.registry = registry;
        this.self = self;
    }

    /**
     * Process incoming SignedMessage handshake. If valid, register peer.
     * Returns true if verified and registered.
     */
    public boolean handle(SignedMessage signedMessage) {
        try {
            String payload = signedMessage.getPayload();
            PeerInfo peer = gson.fromJson(payload, PeerInfo.class);
            if (peer == null) {
                log.warn("Handshake with empty payload");
                return false;
            }
            String pubBase64 = peer.getPublicKeyBase64();
            if (pubBase64 == null) {
                log.warn("Handshake missing publicKey for node {}", peer.getNodeId());
                return false;
            }
            PublicKey pub = KeyUtil.publicKeyFromBase64(pubBase64);
            byte[] sigBytes = Base64.getDecoder().decode(signedMessage.getSignature());
            boolean ok = KeyUtil.verify(pub, payload.getBytes(java.nio.charset.StandardCharsets.UTF_8), sigBytes);
            if (!ok) {
                log.warn("Invalid handshake signature from peer {}", peer.getNodeId());
                return false;
            }
            // register peer
            registry.addOrUpdatePeer(peer, pub);
            log.info("Handshake verified and peer registered: {}", peer.getNodeId());
            return true;
        } catch (Exception e) {
            log.warn("Error processing handshake: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Build our signed handshake message to send to remote.
     */
    public SignedMessage buildSignedHandshake() {
        try {
            PeerInfo me = self.getInfo();
            String payload = gson.toJson(me);
            byte[] sig = KeyUtil.sign(self.getKeyPair().getPrivate(), payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            String signatureBase64 = Base64.getEncoder().encodeToString(sig);
            return SignedMessage.builder()
                    .type(MessageType.HANDSHAKE)
                    .payload(payload)
                    .signature(signatureBase64)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build signed handshake", e);
        }
    }

}
