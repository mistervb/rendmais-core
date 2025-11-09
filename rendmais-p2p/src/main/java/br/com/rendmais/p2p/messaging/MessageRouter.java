package br.com.rendmais.p2p.messaging;

import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class MessageRouter {
    private static final Logger log = LoggerFactory.getLogger(MessageRouter.class);
    
    private final Map<MessageType, Consumer<SignedMessage>> handlers = new HashMap<>();
    private final CopyOnWriteArrayList<MessageInterceptor> interceptors = new CopyOnWriteArrayList<>();
    
    public interface MessageInterceptor {
        void intercept(SignedMessage message, MessageContext context);
    }
    
    public static class MessageContext {
        private final String sourceAddress;
        private final long timestamp;
        private boolean processed;
        
        public MessageContext(String sourceAddress) {
            this.sourceAddress = sourceAddress;
            this.timestamp = System.currentTimeMillis();
            this.processed = false;
        }
        
        public String getSourceAddress() { return sourceAddress; }
        public long getTimestamp() { return timestamp; }
        public boolean isProcessed() { return processed; }
        public void setProcessed(boolean processed) { this.processed = processed; }
    }

    public void registerHandler(MessageType type, Consumer<SignedMessage> handler) {
        handlers.put(type, handler);
        log.info("Registered handler for message type: {}", type);
    }

    public void unregisterHandler(MessageType type) {
        handlers.remove(type);
        log.info("Unregistered handler for message type: {}", type);
    }

    public void addInterceptor(MessageInterceptor interceptor) {
        interceptors.add(interceptor);
    }

    public void removeInterceptor(MessageInterceptor interceptor) {
        interceptors.remove(interceptor);
    }

    public void route(SignedMessage message, String sourceAddress) {
        if (message == null || message.getType() == null) {
            log.warn("Received null or invalid message");
            return;
        }

        MessageContext context = new MessageContext(sourceAddress);
        
        // Run interceptors
        for (MessageInterceptor interceptor : interceptors) {
            try {
                interceptor.intercept(message, context);
            } catch (Exception e) {
                log.error("Error in message interceptor", e);
            }
        }
        
        if (context.isProcessed()) {
            log.debug("Message processed by interceptor, skipping handlers");
            return;
        }

        Consumer<SignedMessage> handler = handlers.get(message.getType());
        if (handler != null) {
            try {
                log.debug("Routing message of type: {} from {}", message.getType(), sourceAddress);
                handler.accept(message);
            } catch (Exception e) {
                log.error("Error processing message of type: {} from {}", message.getType(), sourceAddress, e);
            }
        } else {
            log.warn("No handler registered for message type: {} from {}", message.getType(), sourceAddress);
        }
    }

    public void route(SignedMessage message) {
        route(message, "unknown");
    }

    public boolean hasHandler(MessageType type) {
        return handlers.containsKey(type);
    }

    public void clearHandlers() {
        handlers.clear();
        log.info("Cleared all message handlers");
    }

    public Map<MessageType, Consumer<SignedMessage>> getRegisteredHandlers() {
        return new HashMap<>(handlers);
    }
}
