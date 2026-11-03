package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.enums.ActionType;
import ru.yandex.practicum.analyzer.enums.ConditionOperation;
import ru.yandex.practicum.analyzer.enums.ConditionType;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;

@Service
@RequiredArgsConstructor
public class ScenarioService {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Transactional
    public void saveScenario(String hubId, ScenarioAddedEventAvro event) {
        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .orElseGet(() -> {
                    Scenario s = new Scenario();
                    s.setHubId(hubId);
                    s.setName(event.getName());
                    return s;
                });

        scenario.getConditions().clear();
        scenario.getActions().clear();

        for (ScenarioConditionAvro conditionAvro : event.getConditions()) {
            Sensor sensor = sensorRepository.findById(conditionAvro.getSensorId()).orElse(null);
            if (sensor == null) {
                continue;
            }

            ConditionType type = ConditionType.valueOf(conditionAvro.getType().name());
            ConditionOperation operation = ConditionOperation.valueOf(conditionAvro.getOperation().name());
            Integer value = mapValue(conditionAvro.getValue());

            Condition condition = conditionRepository
                    .findByTypeAndOperationAndValue(type, operation, value)
                    .orElseGet(() -> {
                        Condition c = new Condition();
                        c.setType(type);
                        c.setOperation(operation);
                        c.setValue(value);
                        return conditionRepository.save(c);
                    });

            ScenarioCondition scenarioCondition = new ScenarioCondition();
            scenarioCondition.setScenario(scenario);
            scenarioCondition.setSensor(sensor);
            scenarioCondition.setCondition(condition);
            scenario.getConditions().add(scenarioCondition);
        }

        for (DeviceActionAvro actionAvro : event.getActions()) {
            Sensor sensor = sensorRepository.findById(actionAvro.getSensorId()).orElse(null);
            if (sensor == null) {
                continue;
            }

            ActionType type = ActionType.valueOf(actionAvro.getType().name());
            Integer value = mapValue(actionAvro.getValue());

            Action action = actionRepository
                    .findByTypeAndValue(type, value)
                    .orElseGet(() -> {
                        Action a = new Action();
                        a.setType(type);
                        a.setValue(value);
                        return actionRepository.save(a);
                    });

            ScenarioAction scenarioAction = new ScenarioAction();
            scenarioAction.setScenario(scenario);
            scenarioAction.setSensor(sensor);
            scenarioAction.setAction(action);
            scenario.getActions().add(scenarioAction);
        }

        scenarioRepository.save(scenario);
    }

    @Transactional
    public void removeScenario(String hubId, String name) {
        scenarioRepository.findByHubIdAndName(hubId, name)
                .ifPresent(scenarioRepository::delete);
    }

    private Integer mapValue(Object value) {
        if (value == null) return null;
        if (value instanceof Boolean b) return b ? 1 : 0;
        if (value instanceof Integer i) return i;
        if (value instanceof Long l) return l.intValue();
        if (value instanceof Number n) return n.intValue();
        throw new IllegalArgumentException("Неожиданный тип value: " + value.getClass());
    }
}
