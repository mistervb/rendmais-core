package br.com.rendmais.p2p.messaging;

import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;

import java.util.HashMap;
import java.util.function.Consumer;

public class MessageRouter {
    private final HashMap<MessageType, Consumer<SignedMessage>> handlers = new HashMap<>();

    public void registerHandler(MessageType type, Consumer<SignedMessage> handler) {
        handlers.put(type, handler);
    }

    public void route(SignedMessage message) {
        if (handlers.containsKey(message.getType())) {
            handlers.get(message.getType()).accept(message);
        }
    }
}
