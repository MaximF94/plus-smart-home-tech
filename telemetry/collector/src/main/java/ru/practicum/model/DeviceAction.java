package ru.practicum.model;

import ru.practicum.enums.ActionType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class DeviceAction {

    private String sensorId;
    private ActionType type;
    private Integer value;
}
