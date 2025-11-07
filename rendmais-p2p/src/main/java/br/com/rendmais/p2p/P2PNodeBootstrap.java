package br.com.rendmais.p2p;

import br.com.rendmais.common.crypto.KeyUtil;
import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.p2p.net.P2PClient;
import br.com.rendmais.p2p.net.P2PServer;
import br.com.rendmais.p2p.protocol.HandshakeHandler;
import br.com.rendmais.p2p.registry.PeerRegistry;
import br.com.rendmais.p2p.identity.PeerIdentity;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.Base64;

public class P2PNodeBootstrap {

    private static final Logger log = LoggerFactory.getLogger(P2PNodeBootstrap.class);

    private final PeerIdentity identity;
    @Getter
    private final PeerRegistry registry = new PeerRegistry();
    private final MessageRouter router = new MessageRouter();
    private final P2PServer server;

    public P2PNodeBootstrap(int port) {
        this.identity = new PeerIdentity("0.0.0.0", port);
        this.server = new P2PServer(port, router, registry);

        // register basic handlers (example: handshake)
        router.registerHandler(MessageType.HANDSHAKE, msg -> {
            try {
                PeerInfo info = new com.google.gson.Gson().fromJson(msg.getPayload(), PeerInfo.class);

                // Recupera a chave pública do peer
                PublicKey peerPublicKey = KeyUtil.publicKeyFromBase64(info.getPublicKeyBase64());

                // Verifica assinatura do payload
                boolean verified = KeyUtil.verify(peerPublicKey,
                        msg.getPayload().getBytes(StandardCharsets.UTF_8),
                        Base64.getDecoder().decode(msg.getSignature())
                );

                if (!verified) {
                    log.warn("Handshake signature INVALID for peer {}", info.getNodeId());
                    return;
                }

                // Agora podemos registrar com chave pública de verdade
                registry.addOrUpdatePeer(info, peerPublicKey);
                log.info("Handshake accepted: {}", info.getNodeId());

            } catch (Exception e) {
                log.warn("Invalid handshake payload: {}", e.getMessage());
            }
        });

        router.registerHandler(MessageType.HEARTBEAT, msg -> {
            // ignore for now or update lastSeen
            log.debug("Heartbeat received");
        });
    }

    public void start() throws InterruptedException {
        server.start();
    }

    public P2PClient connectTo(String host, int port) {
        HandshakeHandler handshakeHandler = new HandshakeHandler(registry, identity);
        P2PClient client = new P2PClient(host, port, router, registry, handshakeHandler);
        client.connect();
        // after connect, send handshake
        // we delay a bit to allow connection; in production, use FutureListener
        new Thread(() -> {
            PeerInfo myInfo = identity.getInfo();
            String payload = new com.google.gson.Gson().toJson(myInfo);
            byte[] signatureBytes = KeyUtil.sign(identity.getKeyPair().getPrivate(), payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            String signatureBase64 = java.util.Base64.getEncoder().encodeToString(signatureBytes);

            SignedMessage signed = SignedMessage.builder()
                    .type(MessageType.HANDSHAKE)
                    .payload(payload)
                    .signature(signatureBase64)
                    .build();
            client.send(signed);
        }).start();
        return client;
    }

    public void stop() {
        server.stop();
    }
}
