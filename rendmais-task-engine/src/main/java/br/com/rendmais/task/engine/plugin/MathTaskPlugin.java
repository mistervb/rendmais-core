package br.com.rendmais.task.engine.plugin;

import br.com.rendmais.task.engine.model.Task;
import br.com.rendmais.task.engine.model.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class MathTaskPlugin implements TaskPlugin {
    
    private static final Logger log = LoggerFactory.getLogger(MathTaskPlugin.class);
    
    private static final String PLUGIN_ID = "math-plugin";
    private static final String PLUGIN_NAME = "Math Task Plugin";
    private static final String PLUGIN_VERSION = "1.0.0";
    
    private volatile boolean isHealthy = true;
    private Map<String, String> config;
    
    @Override
    public String getPluginId() {
        return PLUGIN_ID;
    }
    
    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }
    
    @Override
    public String getPluginVersion() {
        return PLUGIN_VERSION;
    }
    
    @Override
    public Set<String> getSupportedTaskTypes() {
        return Set.of("CALCULATE", "ADD", "MULTIPLY", "RANDOM");
    }
    
    @Override
    public TaskResult execute(Task task) {
        log.info("Executing {} task: {}", task.getType(), task.getId());
        
        try {
            String payload = task.getPayload();
            String result;
            
            switch (task.getType()) {
                case "ADD":
                    result = performAddition(payload);
                    break;
                case "MULTIPLY":
                    result = performMultiplication(payload);
                    break;
                case "RANDOM":
                    result = generateRandomNumber(payload);
                    break;
                case "CALCULATE":
                    result = performCalculation(payload);
                    break;
                default:
                    return TaskResult.failure(task.getId(), "Unsupported math operation: " + task.getType());
            }
            
            return TaskResult.success(task.getId(), result);
            
        } catch (Exception e) {
            return TaskResult.failure(task.getId(), "Math task failed: " + e.getMessage(), e);
        }
    }
    
    private String performAddition(String payload) {
        String[] numbers = payload.split(",");
        int sum = 0;
        for (String num : numbers) {
            sum += Integer.parseInt(num.trim());
        }
        return String.valueOf(sum);
    }
    
    private String performMultiplication(String payload) {
        String[] numbers = payload.split(",");
        int product = 1;
        for (String num : numbers) {
            product *= Integer.parseInt(num.trim());
        }
        return String.valueOf(product);
    }
    
    private String generateRandomNumber(String payload) {
        int max = Integer.parseInt(payload.trim());
        int random = ThreadLocalRandom.current().nextInt(max);
        return String.valueOf(random);
    }
    
    private String performCalculation(String payload) {
        // Simple calculation: evaluate basic expressions like "2+3" or "5*4"
        if (payload.contains("+")) {
            String[] parts = payload.split("\\+");
            int a = Integer.parseInt(parts[0].trim());
            int b = Integer.parseInt(parts[1].trim());
            return String.valueOf(a + b);
        } else if (payload.contains("*")) {
            String[] parts = payload.split("\\*");
            int a = Integer.parseInt(parts[0].trim());
            int b = Integer.parseInt(parts[1].trim());
            return String.valueOf(a * b);
        }
        
        throw new IllegalArgumentException("Unsupported calculation: " + payload);
    }
    
    @Override
    public void initialize(Map<String, String> config) {
        this.config = new HashMap<>(config);
        log.info("Initialized {} with config: {}", PLUGIN_NAME, config);
    }
    
    @Override
    public void shutdown() {
        log.info("Shutting down {}", PLUGIN_NAME);
        this.config = null;
    }
    
    @Override
    public boolean isHealthy() {
        return isHealthy;
    }
    
    @Override
    public boolean validateTask(Task task) {
        if (task == null || task.getPayload() == null) {
            log.warn("Invalid task: null task or payload");
            return false;
        }
        
        if (task.getPayload().trim().isEmpty()) {
            log.warn("Invalid task: empty payload");
            return false;
        }
        
        // Validate based on task type
        try {
            switch (task.getType()) {
                case "ADD":
                case "MULTIPLY":
                    String[] numbers = task.getPayload().split(",");
                    for (String num : numbers) {
                        Integer.parseInt(num.trim());
                    }
                    break;
                case "RANDOM":
                    Integer.parseInt(task.getPayload().trim());
                    break;
                case "CALCULATE":
                    // Basic validation for calculation format
                    if (!task.getPayload().matches("\\d+[+\\*]\\d+")) {
                        return false;
                    }
                    break;
            }
            return true;
        } catch (NumberFormatException e) {
            log.warn("Invalid numeric payload for task type {}: {}", task.getType(), task.getPayload());
            return false;
        }
    }
    
    @Override
    public Map<String, Object> getPluginInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("description", "Mathematical operations task plugin");
        info.put("config", config != null ? config : new HashMap<>());
        info.put("healthCheck", isHealthy);
        return info;
    }
}