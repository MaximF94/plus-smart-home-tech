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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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

        HashSet<String> sensorIds = new HashSet<>();
        event.getConditions().forEach(c -> sensorIds.add(c.getSensorId()));
        event.getActions().forEach(a -> sensorIds.add(a.getSensorId()));

        Map<String, Sensor> sensorsById = sensorRepository.findAllById(sensorIds).stream()
                .collect(Collectors.toMap(Sensor::getId, Function.identity()));

        Set<ConditionType> condTypes = new HashSet<>();
        Set<ConditionOperation> condOps = new HashSet<>();
        Set<Integer> condValues = new HashSet<>();
        for (ScenarioConditionAvro c : event.getConditions()) {
            condTypes.add(ConditionType.valueOf(c.getType().name()));
            condOps.add(ConditionOperation.valueOf(c.getOperation().name()));
            condValues.add(mapValue(c.getValue()));
        }
        Map<String, Condition> conditionsByKey = conditionRepository
                .findByTypeInAndOperationInAndValueIn(condTypes, condOps, condValues).stream()
                .collect(Collectors.toMap(
                        c -> conditionKey(c.getType(), c.getOperation(), c.getValue()),
                        Function.identity()));

        Set<ActionType> actTypes = new HashSet<>();
        Set<Integer> actValues = new HashSet<>();
        for (DeviceActionAvro a : event.getActions()) {
            actTypes.add(ActionType.valueOf(a.getType().name()));
            actValues.add(mapValue(a.getValue()));
        }
        Map<String, Action> actionsByKey = actionRepository
                .findByTypeInAndValueIn(actTypes, actValues).stream()
                .collect(Collectors.toMap(
                        a -> actionKey(a.getType(), a.getValue()),
                        Function.identity()));

        for (ScenarioConditionAvro conditionAvro : event.getConditions()) {
            Sensor sensor = sensorsById.get(conditionAvro.getSensorId());
            if (sensor == null) continue;

            ConditionType type = ConditionType.valueOf(conditionAvro.getType().name());
            ConditionOperation operation = ConditionOperation.valueOf(conditionAvro.getOperation().name());
            Integer value = mapValue(conditionAvro.getValue());
            String key = conditionKey(type, operation, value);

            Condition condition = conditionsByKey.get(key);
            if (condition == null) {
                condition = new Condition();
                condition.setType(type);
                condition.setOperation(operation);
                condition.setValue(value);
                condition = conditionRepository.save(condition);
                conditionsByKey.put(key, condition);
            }

            ScenarioCondition scenarioCondition = new ScenarioCondition();
            scenarioCondition.setScenario(scenario);
            scenarioCondition.setSensor(sensor);
            scenarioCondition.setCondition(condition);
            scenario.getConditions().add(scenarioCondition);
        }
        
        for (DeviceActionAvro actionAvro : event.getActions()) {
            Sensor sensor = sensorsById.get(actionAvro.getSensorId());
            if (sensor == null) continue;

            ActionType type = ActionType.valueOf(actionAvro.getType().name());
            Integer value = mapValue(actionAvro.getValue());
            String key = actionKey(type, value);

            Action action = actionsByKey.get(key);
            if (action == null) {
                action = new Action();
                action.setType(type);
                action.setValue(value);
                action = actionRepository.save(action);
                actionsByKey.put(key, action);
            }

            ScenarioAction scenarioAction = new ScenarioAction();
            scenarioAction.setScenario(scenario);
            scenarioAction.setSensor(sensor);
            scenarioAction.setAction(action);
            scenario.getActions().add(scenarioAction);
        }

        scenarioRepository.save(scenario);
    }

    private String conditionKey(ConditionType type, ConditionOperation op, Integer value) {
        return type + "|" + op + "|" + value;
    }

    private String actionKey(ActionType type, Integer value) {
        return type + "|" + value;
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
