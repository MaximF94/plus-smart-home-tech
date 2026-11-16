package ru.yandex.practicum.analyzer.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;

@Getter
@Setter
public class ScenarioActionId implements Serializable {

    private Long scenario;
    private String sensor;
    private Long action;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ScenarioActionId that = (ScenarioActionId) o;
        return Objects.equals(scenario, that.scenario) && Objects.equals(sensor, that.sensor) && Objects.equals(action, that.action);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scenario, sensor, action);
    }
}
