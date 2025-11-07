package br.com.rendmais.common.dto;

import lombok.Data;

@Data
public class HandshakeMessage {
    private String nodeId;          // ID derivado da chave pública
    private String publicKeyBase64; // Chave pública em Base64
    private long timestamp;         // Proteção contra replay
    private String nonce;           // Random para dificultar replay+cache
}