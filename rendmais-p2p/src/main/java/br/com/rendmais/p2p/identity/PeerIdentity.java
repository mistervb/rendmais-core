package br.com.rendmais.p2p.identity;

import br.com.rendmais.common.crypto.KeyUtil;
import br.com.rendmais.common.dto.PeerInfo;
import lombok.Getter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;

@Getter
public class PeerIdentity {

    private final PeerInfo info;
    private final KeyPair keyPair;
    private final Path keyPath;
    private String nodeId;

    public PeerIdentity(String address, int port) {
        this.keyPath = defaultKeyPath(); // agora sem depender do nodeId ainda

        KeyPair kp;
        if (Files.exists(keyPath)) {
            kp = KeyUtil.loadKeyPairFromFile(keyPath);
        } else {
            kp = KeyUtil.generateEd25519KeyPair();
            KeyUtil.saveKeyPairToFile(kp, keyPath);
        }
        this.keyPair = kp;

        // NodeId derivado da public key (estável e verificável)
        this.nodeId = KeyUtil.publicKeyFingerprint(kp.getPublic());

        // pega public key base64
        String pubBase64 = KeyUtil.publicKeyToBase64(kp.getPublic());

        this.info = PeerInfo.builder()
                .nodeId(nodeId)
                .address(address)
                .port(port)
                .publicKeyBase64(pubBase64)
                .build();
    }

    private static Path defaultKeyPath() {
        String home = System.getProperty("user.home");
        Path dir = Paths.get(home, ".rendmais", "keys");
        if (!Files.exists(dir)) {
            try { Files.createDirectories(dir); } catch (Exception ignored) {}
        }
        return dir.resolve("peer.key"); // agora fixo
    }
}
