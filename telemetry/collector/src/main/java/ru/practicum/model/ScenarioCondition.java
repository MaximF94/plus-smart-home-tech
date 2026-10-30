package ru.practicum.model;

import ru.practicum.enums.ConditionOperation;
import ru.practicum.enums.ConditionType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ScenarioCondition {

    private String sensorId;
    private ConditionType type;
    private ConditionOperation operation;
    private Object value;

    public void setValue(Object value) {
        if (value != null && !(value instanceof Integer) && !(value instanceof Boolean)) {
            throw new IllegalArgumentException(
                    "value должно быть null, Integer или Boolean, получено: " + value.getClass().getName());
        }
        this.value = value;
    }
}
