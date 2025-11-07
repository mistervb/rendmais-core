package br.com.rendmais.p2p.identity;

import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.common.crypto.KeyUtil;
import lombok.Getter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.util.UUID;

public class PeerIdentity {
    @Getter
    private final PeerInfo info;
    @Getter
    private final KeyPair keyPair;
    private final Path keyPath;

    public PeerIdentity(String address, int port) {
        String nodeId = UUID.randomUUID().toString();
        this.keyPath = defaultKeyPath(nodeId);
        KeyPair kp;
        if (java.nio.file.Files.exists(keyPath)) {
            kp = KeyUtil.loadKeyPairFromFile(keyPath);
        } else {
            kp = KeyUtil.generateEd25519KeyPair();
            KeyUtil.saveKeyPairToFile(kp, keyPath);
        }
        this.keyPair = kp;
        String pubBase64 = KeyUtil.publicKeyToBase64(kp.getPublic());
        this.info = PeerInfo.builder()
                .nodeId(nodeId)
                .address(address)
                .port(port)
                .publicKeyBase64(pubBase64)
                .build();
    }

    private static Path defaultKeyPath(String nodeId) {
        String home = System.getProperty("user.home");
        Path dir = Paths.get(home, ".rendmais", "keys");
        return dir.resolve(nodeId + ".key");
    }
}
