package br.com.rendmais.task.engine.registry;

import br.com.rendmais.task.engine.exception.PluginException;
import br.com.rendmais.task.engine.plugin.EchoTaskPlugin;
import br.com.rendmais.task.engine.plugin.TaskPlugin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class TaskRegistryTest {
    
    private TaskRegistry taskRegistry;
    
    @BeforeEach
    void setUp() {
        taskRegistry = new TaskRegistry();
    }
    
    @AfterEach
    void tearDown() throws Exception {
        // Clean up any registered plugins
        for (String pluginId : taskRegistry.getRegisteredPluginIds()) {
            taskRegistry.unregisterPlugin(pluginId);
        }
    }
    
    @Test
    void testRegisterPlugin() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        
        taskRegistry.registerPlugin(plugin);
        
        assertThat(taskRegistry.isPluginRegistered(plugin.getPluginId())).isTrue();
        assertThat(taskRegistry.getPlugin(plugin.getPluginId())).isEqualTo(plugin);
    }
    
    @Test
    void testRegisterDuplicatePlugin() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskRegistry.registerPlugin(plugin);
        
        assertThatThrownBy(() -> taskRegistry.registerPlugin(plugin))
            .isInstanceOf(PluginException.class)
            .hasMessageContaining("Plugin already registered");
    }
    
    @Test
    void testUnregisterPlugin() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskRegistry.registerPlugin(plugin);
        
        taskRegistry.unregisterPlugin(plugin.getPluginId());
        
        assertThat(taskRegistry.isPluginRegistered(plugin.getPluginId())).isFalse();
        assertThat(taskRegistry.getPlugin(plugin.getPluginId())).isNull();
    }
    
    @Test
    void testUnregisterNonExistentPlugin() {
        assertThatThrownBy(() -> taskRegistry.unregisterPlugin("non-existent"))
            .isInstanceOf(PluginException.class)
            .hasMessageContaining("Plugin not found");
    }
    
    @Test
    void testGetPluginForTaskType() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskRegistry.registerPlugin(plugin);
        
        TaskPlugin foundPlugin = taskRegistry.getPluginForTaskType("ECHO");
        
        assertThat(foundPlugin).isEqualTo(plugin);
    }
    
    @Test
    void testGetPluginForUnsupportedTaskType() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskRegistry.registerPlugin(plugin);
        
        TaskPlugin foundPlugin = taskRegistry.getPluginForTaskType("UNSUPPORTED");
        
        assertThat(foundPlugin).isNull();
    }
    
    @Test
    void testGetSupportedTaskTypes() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskRegistry.registerPlugin(plugin);
        
        assertThat(taskRegistry.getSupportedTaskTypes()).contains("ECHO", "PING", "HELLO");
    }
    
    @Test
    void testIsTaskTypeSupported() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskRegistry.registerPlugin(plugin);
        
        assertThat(taskRegistry.isTaskTypeSupported("ECHO")).isTrue();
        assertThat(taskRegistry.isTaskTypeSupported("UNSUPPORTED")).isFalse();
    }
    
    @Test
    void testGetAllPlugins() throws Exception {
        EchoTaskPlugin plugin1 = new EchoTaskPlugin();
        taskRegistry.registerPlugin(plugin1);
        
        assertThat(taskRegistry.getAllPlugins()).hasSize(1).contains(plugin1);
    }
    
    @Test
    void testGetPluginInfo() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskRegistry.registerPlugin(plugin);
        
        Map<String, Object> info = taskRegistry.getPluginInfo(plugin.getPluginId());
        
        assertThat(info).containsEntry("pluginId", plugin.getPluginId());
        assertThat(info).containsEntry("pluginName", plugin.getPluginName());
        assertThat(info).containsEntry("pluginVersion", plugin.getPluginVersion());
        assertThat(info).containsEntry("healthy", true);
    }
    
    @Test
    void testGetAllPluginsInfo() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskRegistry.registerPlugin(plugin);
        
        Map<String, Map<String, Object>> allInfo = taskRegistry.getAllPluginsInfo();
        
        assertThat(allInfo).containsKey(plugin.getPluginId());
        assertThat(allInfo.get(plugin.getPluginId())).containsEntry("pluginName", plugin.getPluginName());
    }
    
    @Test
    void testInitializePlugin() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskRegistry.registerPlugin(plugin);
        
        Map<String, String> config = Map.of("timeout", "1000", "retries", "3");
        taskRegistry.initializePlugin(plugin.getPluginId(), config);
        
        // Plugin should be initialized with the config
        Map<String, Object> info = taskRegistry.getPluginInfo(plugin.getPluginId());
        assertThat(info).containsKey("config");
    }
    
    @Test
    void testInitializeNonExistentPlugin() {
        Map<String, String> config = Map.of("timeout", "1000");
        
        assertThatThrownBy(() -> taskRegistry.initializePlugin("non-existent", config))
            .isInstanceOf(PluginException.class)
            .hasMessageContaining("Plugin not found");
    }
}