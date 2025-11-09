package br.com.rendmais.task.engine.model;

public enum TaskPriority {
    LOW(1),
    NORMAL(2),
    HIGH(3),
    CRITICAL(4);

    private final int level;

    TaskPriority(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    public static TaskPriority fromLevel(int level) {
        for (TaskPriority priority : values()) {
            if (priority.level == level) {
                return priority;
            }
        }
        return NORMAL;
    }
}