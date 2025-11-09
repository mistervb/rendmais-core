package br.com.rendmais.task.engine.registry;

import br.com.rendmais.task.engine.exception.PluginException;
import br.com.rendmais.task.engine.plugin.TaskPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TaskRegistry {
    
    private static final Logger log = LoggerFactory.getLogger(TaskRegistry.class);
    
    private final Map<String, TaskPlugin> plugins = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> taskTypeToPlugins = new ConcurrentHashMap<>();
    
    public void registerPlugin(TaskPlugin plugin) throws PluginException {
        String pluginId = plugin.getPluginId();
        
        if (plugins.containsKey(pluginId)) {
            throw new PluginException("Plugin already registered: " + pluginId);
        }
        
        try {
            plugins.put(pluginId, plugin);
            
            // Map task types to plugins
            Set<String> supportedTypes = plugin.getSupportedTaskTypes();
            for (String taskType : supportedTypes) {
                taskTypeToPlugins.computeIfAbsent(taskType, k -> ConcurrentHashMap.newKeySet())
                    .add(pluginId);
            }
            
            log.info("Registered plugin: {} - {} v{}", 
                pluginId, plugin.getPluginName(), plugin.getPluginVersion());
            
        } catch (Exception e) {
            throw new PluginException("Failed to register plugin: " + pluginId, e);
        }
    }
    
    public void unregisterPlugin(String pluginId) throws PluginException {
        TaskPlugin plugin = plugins.remove(pluginId);
        if (plugin == null) {
            throw new PluginException("Plugin not found: " + pluginId);
        }
        
        try {
            plugin.shutdown();
            
            // Remove from task type mappings
            for (Set<String> pluginIds : taskTypeToPlugins.values()) {
                pluginIds.remove(pluginId);
            }
            
            // Clean up empty sets
            taskTypeToPlugins.entrySet().removeIf(entry -> entry.getValue().isEmpty());
            
            log.info("Unregistered plugin: {}", pluginId);
            
        } catch (Exception e) {
            throw new PluginException("Failed to unregister plugin: " + pluginId, e);
        }
    }
    
    public TaskPlugin getPlugin(String pluginId) {
        return plugins.get(pluginId);
    }
    
    public TaskPlugin getPluginForTaskType(String taskType) {
        Set<String> pluginIds = taskTypeToPlugins.get(taskType);
        if (pluginIds == null || pluginIds.isEmpty()) {
            return null;
        }
        
        // Return the first available plugin for this task type
        String pluginId = pluginIds.iterator().next();
        return plugins.get(pluginId);
    }
    
    public List<TaskPlugin> getPluginsForTaskType(String taskType) {
        Set<String> pluginIds = taskTypeToPlugins.get(taskType);
        if (pluginIds == null || pluginIds.isEmpty()) {
            return Collections.emptyList();
        }
        
        return pluginIds.stream()
            .map(plugins::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }
    
    public Set<String> getSupportedTaskTypes() {
        return new HashSet<>(taskTypeToPlugins.keySet());
    }
    
    public Set<String> getRegisteredPluginIds() {
        return new HashSet<>(plugins.keySet());
    }
    
    public List<TaskPlugin> getAllPlugins() {
        return new ArrayList<>(plugins.values());
    }
    
    public boolean isPluginRegistered(String pluginId) {
        return plugins.containsKey(pluginId);
    }
    
    public boolean isTaskTypeSupported(String taskType) {
        return taskTypeToPlugins.containsKey(taskType);
    }
    
    public void initializePlugin(String pluginId, Map<String, String> config) throws PluginException {
        TaskPlugin plugin = plugins.get(pluginId);
        if (plugin == null) {
            throw new PluginException("Plugin not found: " + pluginId);
        }
        
        try {
            plugin.initialize(config);
            log.info("Initialized plugin: {} with config: {}", pluginId, config);
        } catch (Exception e) {
            throw new PluginException("Failed to initialize plugin: " + pluginId, e);
        }
    }
    
    public Map<String, Object> getPluginInfo(String pluginId) {
        TaskPlugin plugin = plugins.get(pluginId);
        if (plugin == null) {
            return Collections.emptyMap();
        }
        
        Map<String, Object> info = new HashMap<>();
        info.put("pluginId", plugin.getPluginId());
        info.put("pluginName", plugin.getPluginName());
        info.put("pluginVersion", plugin.getPluginVersion());
        info.put("supportedTaskTypes", plugin.getSupportedTaskTypes());
        info.put("healthy", plugin.isHealthy());
        info.putAll(plugin.getPluginInfo());
        
        return info;
    }
    
    public Map<String, Map<String, Object>> getAllPluginsInfo() {
        Map<String, Map<String, Object>> allInfo = new HashMap<>();
        for (String pluginId : plugins.keySet()) {
            allInfo.put(pluginId, getPluginInfo(pluginId));
        }
        return allInfo;
    }
}