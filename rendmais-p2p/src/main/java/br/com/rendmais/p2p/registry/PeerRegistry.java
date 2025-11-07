package br.com.rendmais.p2p.registry;

import br.com.rendmais.common.dto.PeerInfo;

import java.security.PublicKey;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PeerRegistry {

    public static class PeerEntry {
        private final PeerInfo info;
        private final PublicKey publicKey;
        private volatile Instant lastSeen;

        public PeerEntry(PeerInfo info, PublicKey publicKey) {
            this.info = info;
            this.publicKey = publicKey;
            this.lastSeen = Instant.now();
        }

        public PeerInfo getInfo() { return info; }
        public PublicKey getPublicKey() { return publicKey; }
        public Instant getLastSeen() { return lastSeen; }
        public void touch() { lastSeen = Instant.now(); }
    }

    private final Map<String, PeerEntry> peers = new ConcurrentHashMap<>();

    public void addOrUpdatePeer(PeerInfo peer, PublicKey publicKey) {
        peers.compute(peer.getNodeId(), (k, existing) -> {
            if (existing == null) return new PeerEntry(peer, publicKey);
            existing.touch();
            return existing;
        });
    }

    public Collection<PeerEntry> listPeers() {
        return peers.values();
    }

    public Optional<PeerEntry> getById(String nodeId) {
        return Optional.ofNullable(peers.get(nodeId));
    }

    public void removePeer(String nodeId) { peers.remove(nodeId); }
}
