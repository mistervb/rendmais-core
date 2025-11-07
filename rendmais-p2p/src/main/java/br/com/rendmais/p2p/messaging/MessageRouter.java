package br.com.rendmais.p2p.messaging;

import java.util.HashMap;
import java.util.function.Consumer;

public class MessageRouter {
    private final HashMap<MessageType, Consumer<Message>> handlers = new HashMap<>();

    public void registerHandler(MessageType type, Consumer<Message> handler) {
        handlers.put(type, handler);
    }

    public void route(Message message) {
        if (handlers.containsKey(message.getType())) {
            handlers.get(message.getType()).accept(message);
        }
    }
}
