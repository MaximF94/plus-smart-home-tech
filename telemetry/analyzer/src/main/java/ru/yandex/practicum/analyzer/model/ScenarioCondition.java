package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Entity
@Table(name = "scenario_conditions")
@IdClass(ScenarioConditionId.class)
@Getter
@Setter
public class ScenarioCondition {

    @Id
    @ManyToOne
    @JoinColumn(name = "scenario_id")
    private Scenario scenario;

    @Id
    @ManyToOne
    @JoinColumn(name = "sensor_id")
    private Sensor sensor;

    @Id
    @ManyToOne
    @JoinColumn(name = "condition_id")
    private Condition condition;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ScenarioCondition that = (ScenarioCondition) o;
        return Objects.equals(scenario, that.scenario) && Objects.equals(sensor, that.sensor) && Objects.equals(condition, that.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scenario, sensor, condition);
    }
}
