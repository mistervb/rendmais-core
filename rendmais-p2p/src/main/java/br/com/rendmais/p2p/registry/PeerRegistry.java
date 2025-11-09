package br.com.rendmais.p2p.registry;

import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.common.dto.PeerAdvertisement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class PeerRegistry {

    private static final Logger log = LoggerFactory.getLogger(PeerRegistry.class);
    private static final long PEER_TIMEOUT_MINUTES = 30;
    private static final int MAX_KNOWN_PEERS = 100;

    public static class PeerEntry {
        private final PeerInfo info;
        private final PublicKey publicKey;
        private volatile Instant lastSeen;
        private volatile ConnectionState state;
        private volatile int connectionAttempts;
        private volatile Instant lastConnectionAttempt;

        public enum ConnectionState {
            CONNECTED, DISCONNECTED, CONNECTING, HANDSHAKING, ERROR
        }

        public PeerEntry(PeerInfo info, PublicKey publicKey) {
            this.info = info;
            this.publicKey = publicKey;
            this.lastSeen = Instant.now();
            this.state = ConnectionState.DISCONNECTED;
            this.connectionAttempts = 0;
            this.lastConnectionAttempt = Instant.now();
        }

        public PeerInfo getInfo() { return info; }
        public PublicKey getPublicKey() { return publicKey; }
        public Instant getLastSeen() { return lastSeen; }
        public ConnectionState getState() { return state; }
        public int getConnectionAttempts() { return connectionAttempts; }
        public Instant getLastConnectionAttempt() { return lastConnectionAttempt; }
        
        public void touch() { lastSeen = Instant.now(); }
        public void setState(ConnectionState state) { this.state = state; }
        public void incrementConnectionAttempts() { 
            this.connectionAttempts++;
            this.lastConnectionAttempt = Instant.now();
        }
        public void resetConnectionAttempts() { 
            this.connectionAttempts = 0;
        }
    }

    private final Map<String, PeerEntry> peers = new ConcurrentHashMap<>();
    private final List<PeerDiscoveryListener> discoveryListeners = new CopyOnWriteArrayList<>();

    public interface PeerDiscoveryListener {
        void onPeerDiscovered(PeerInfo peerInfo);
        void onPeerDisconnected(String nodeId);
        void onPeerUpdated(PeerInfo peerInfo);
    }

    public void addDiscoveryListener(PeerDiscoveryListener listener) {
        discoveryListeners.add(listener);
    }

    public void removeDiscoveryListener(PeerDiscoveryListener listener) {
        discoveryListeners.remove(listener);
    }

    public void addOrUpdatePeer(PeerInfo peer, PublicKey publicKey) {
        peers.compute(peer.getNodeId(), (k, existing) -> {
            if (existing == null) {
                PeerEntry newEntry = new PeerEntry(peer, publicKey);
                newEntry.setState(PeerEntry.ConnectionState.HANDSHAKING);
                log.info("New peer discovered: {} at {}:{}", peer.getNodeId(), peer.getAddress(), peer.getPort());
                discoveryListeners.forEach(listener -> listener.onPeerDiscovered(peer));
                return newEntry;
            }
            existing.touch();
            existing.setState(PeerEntry.ConnectionState.CONNECTED);
            existing.resetConnectionAttempts();
            discoveryListeners.forEach(listener -> listener.onPeerUpdated(peer));
            return existing;
        });
    }

    public void updatePeerConnectionState(String nodeId, PeerEntry.ConnectionState state) {
        Optional.ofNullable(peers.get(nodeId)).ifPresent(entry -> {
            entry.setState(state);
            if (state == PeerEntry.ConnectionState.CONNECTING) {
                entry.incrementConnectionAttempts();
            }
        });
    }

    public Collection<PeerEntry> listPeers() {
        return new ArrayList<>(peers.values());
    }

    public Collection<PeerEntry> listActivePeers() {
        return peers.values().stream()
                .filter(entry -> entry.getState() == PeerEntry.ConnectionState.CONNECTED)
                .filter(entry -> entry.getLastSeen().isAfter(Instant.now().minusSeconds(PEER_TIMEOUT_MINUTES * 60)))
                .collect(Collectors.toList());
    }

    public Collection<PeerEntry> listKnownPeersForAdvertisement() {
        return peers.values().stream()
                .filter(entry -> entry.getLastSeen().isAfter(Instant.now().minusSeconds(PEER_TIMEOUT_MINUTES * 60)))
                .limit(MAX_KNOWN_PEERS)
                .collect(Collectors.toList());
    }

    public Optional<PeerEntry> getById(String nodeId) {
        return Optional.ofNullable(peers.get(nodeId));
    }

    /**
     * Update the reachable address and port for a known peer.
     * This mutates the stored PeerInfo so future advertisements and reconnections use the correct endpoint.
     */
    public void updatePeerAddress(String nodeId, String address, int port) {
        Optional.ofNullable(peers.get(nodeId)).ifPresent(entry -> {
            PeerInfo info = entry.getInfo();
            if (info != null) {
                info.setAddress(address);
                info.setPort(port);
                log.debug("Updated peer {} address to {}:{}", nodeId, address, port);
            }
        });
    }

    public void removePeer(String nodeId) {
        PeerEntry removed = peers.remove(nodeId);
        if (removed != null) {
            log.info("Peer removed: {}", nodeId);
            discoveryListeners.forEach(listener -> listener.onPeerDisconnected(nodeId));
        }
    }

    public void cleanupStalePeers() {
        Instant cutoff = Instant.now().minusSeconds(PEER_TIMEOUT_MINUTES * 60);
        List<String> stalePeers = peers.entrySet().stream()
                .filter(entry -> entry.getValue().getLastSeen().isBefore(cutoff))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        
        stalePeers.forEach(this::removePeer);
        
        if (!stalePeers.isEmpty()) {
            log.info("Cleaned up {} stale peers", stalePeers.size());
        }
    }

    public int getConnectedPeerCount() {
        return (int) peers.values().stream()
                .filter(entry -> entry.getState() == PeerEntry.ConnectionState.CONNECTED)
                .count();
    }

    public boolean isPeerConnected(String nodeId) {
        return Optional.ofNullable(peers.get(nodeId))
                .map(entry -> entry.getState() == PeerEntry.ConnectionState.CONNECTED)
                .orElse(false);
    }

    public PeerAdvertisement createPeerAdvertisement(String advertiserNodeId) {
        List<PeerInfo> knownPeers = listKnownPeersForAdvertisement().stream()
                .map(PeerEntry::getInfo)
                .filter(peer -> !peer.getNodeId().equals(advertiserNodeId))
                .collect(Collectors.toList());

        return PeerAdvertisement.builder()
                .advertiserNodeId(advertiserNodeId)
                .knownPeers(knownPeers)
                .timestamp(System.currentTimeMillis())
                .ttl(300) // 5 minutes TTL
                .build();
    }
}
