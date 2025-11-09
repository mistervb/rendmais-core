package br.com.rendmais.common.dto;

import lombok.*;
import java.util.List;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PeerAdvertisement {
    private String advertiserNodeId;
    private List<PeerInfo> knownPeers;
    private long timestamp;
    private int ttl; // Time to live for this advertisement
}