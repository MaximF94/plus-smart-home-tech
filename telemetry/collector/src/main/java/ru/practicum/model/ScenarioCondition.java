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
    private Integer value;
}
