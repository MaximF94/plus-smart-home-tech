package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Entity
@Table(name = "scenario_actions")
@IdClass(ScenarioActionId.class)
@Getter
@Setter
public class ScenarioAction {

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
    @JoinColumn(name = "action_id")
    private Action action;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ScenarioAction that = (ScenarioAction) o;
        return Objects.equals(scenario, that.scenario) && Objects.equals(sensor, that.sensor) && Objects.equals(action, that.action);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scenario, sensor, action);
    }
}
