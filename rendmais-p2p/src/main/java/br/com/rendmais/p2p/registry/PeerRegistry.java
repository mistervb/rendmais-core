package br.com.rendmais.p2p.registry;

import br.com.rendmais.common.dto.PeerInfo;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerRegistry {
    private final Map<String, PeerInfo> peers = new ConcurrentHashMap<>();

    public void addPeer(PeerInfo peer) {
        peers.put(peer.getNodeId(), peer);
    }

    public Collection<PeerInfo> listPeers() {
        return peers.values();
    }
}
