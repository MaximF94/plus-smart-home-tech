package ru.yandex.practicum.analyzer.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;

@Getter
@Setter
public class ScenarioConditionId implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long scenario;
    private String sensor;
    private Long condition;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScenarioConditionId that = (ScenarioConditionId) o;
        return Objects.equals(scenario, that.scenario)
                && Objects.equals(sensor, that.sensor)
                && Objects.equals(condition, that.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scenario, sensor, condition);
    }
}
