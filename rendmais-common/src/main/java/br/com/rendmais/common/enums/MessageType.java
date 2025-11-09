package br.com.rendmais.common.enums;

public enum MessageType {
    HANDSHAKE,
    HANDSHAKE_RESPONSE,
    HEARTBEAT,
    HEARTBEAT_RESPONSE,
    TASK_REQUEST,
    TASK_RESULT,
    PEER_ADVERTISEMENT,
    PEER_DISCOVERY,
    CONNECTION_CLOSE,
    ERROR
}
