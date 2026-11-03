package ru.yandex.practicum.analyzer.processor;


import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.enums.ConditionOperation;
import ru.yandex.practicum.analyzer.enums.ConditionType;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Duration;
import java.util.List;


@Slf4j
@Component
public class SnapshotProcessor {

    private static final String SNAPSHOTS_TOPIC = "telemetry.snapshots.v1";

    private final KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer;
    private final ScenarioRepository scenarioRepository;
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public SnapshotProcessor(
            KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer,
            ScenarioRepository scenarioRepository,
            @GrpcClient("hub-router") HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.snapshotConsumer = snapshotConsumer;
        this.scenarioRepository = scenarioRepository;
        this.hubRouterClient = hubRouterClient;
    }

    public void start() {
        try {
            snapshotConsumer.subscribe(List.of(SNAPSHOTS_TOPIC));

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        snapshotConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    try {
                        handleSnapshot(record.value());
                    } catch (Exception e) {
                        log.error("Ошибка обработки снапшота offset={}, partition={}",
                                record.offset(), record.partition(), e);
                    }
                }

                if (!records.isEmpty()) {
                    snapshotConsumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {

        } catch (Exception e) {
            log.error("Фатальная ошибка консьюмера снапшотов", e);
        } finally {
            try {
                snapshotConsumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер снапшотов");
                snapshotConsumer.close();
            }
        }
    }

    private void handleSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);

        if (scenarios.isEmpty()) {
            log.debug("Для хаба {} нет сценариев", hubId);
            return;
        }

        for (Scenario scenario : scenarios) {
            boolean allMet = true;

            for (ScenarioCondition scenarioCondition : scenario.getConditions()) {
                if (!isConditionMet(scenarioCondition, snapshot)) {
                    allMet = false;
                    break;
                }
            }

            if (allMet) {
                log.info("Сценарий '{}' для хаба {} выполнен", scenario.getName(), hubId);
                executeActions(scenario, snapshot);
            }
        }
    }

    private boolean isConditionMet(ScenarioCondition scenarioCondition, SensorsSnapshotAvro snapshot) {
        String sensorId = scenarioCondition.getSensor().getId();
        SensorStateAvro state = snapshot.getSensorsState().get(sensorId);

        if (state == null) {
            log.debug("Датчик {} отсутствует в снапшоте", sensorId);
            return false;
        }

        Object data = state.getData();
        Condition condition = scenarioCondition.getCondition();
        Integer expected = condition.getValue();

        if (expected == null) {
            return false;
        }

        Integer actual = extractValue(data, condition.getType());
        if (actual == null) {
            return false;
        }

        boolean result = compare(actual, expected, condition.getOperation());
        log.debug("CHECK sensor={} type={} op={} expected={} actual={} -> {}",
                sensorId, condition.getType(), condition.getOperation(), expected, actual, result);
        return result;
    }

    private Integer extractValue(Object data, ConditionType type) {
        if (data instanceof SwitchSensorAvro switchSensor) {
            return switchSensor.getState() ? 1 : 0;
        }
        if (data instanceof MotionSensorAvro motionSensor) {
            return motionSensor.getMotion() ? 1 : 0;
        }
        if (data instanceof ClimateSensorAvro climateSensor) {
            return switch (type) {
                case TEMPERATURE -> climateSensor.getTemperatureC();
                case HUMIDITY -> climateSensor.getHumidity();
                case CO2LEVEL -> climateSensor.getCo2Level();
                default -> null;
            };
        }
        if (data instanceof LightSensorAvro lightSensor) {
            return type == ConditionType.LUMINOSITY ? lightSensor.getLuminosity() : null;
        }
        if (data instanceof TemperatureSensorAvro temperatureSensor) {
            return type == ConditionType.TEMPERATURE ? temperatureSensor.getTemperatureC() : null;
        }
        return null;
    }

    private void executeActions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        for (ScenarioAction scenarioAction : scenario.getActions()) {
            Action action = scenarioAction.getAction();

            DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                    .setSensorId(scenarioAction.getSensor().getId())
                    .setType(ActionTypeProto.valueOf(action.getType().name()));

            if (action.getValue() != null) {
                actionBuilder.setValue(action.getValue());
            }

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(snapshot.getHubId())
                    .setScenarioName(scenario.getName())
                    .setAction(actionBuilder.build())
                    .setTimestamp(Timestamp.newBuilder()
                            .setSeconds(snapshot.getTimestamp().getEpochSecond())
                            .setNanos(snapshot.getTimestamp().getNano())
                            .build())
                    .build();

            try {
                if (hubRouterClient == null) {
                    log.error("hubRouterClient is null! gRPC client not initialized");
                    return;
                }
                log.info("Отправляю gRPC-запрос: {}", request);
                hubRouterClient.handleDeviceAction(request);
                log.info("Отправлено действие {} для датчика {}",
                        action.getType(), scenarioAction.getSensor().getId());
            } catch (Exception e) {
                log.error("Ошибка отправки действия sensor={}", scenarioAction.getSensor().getId(), e);
            }
        }
    }

    private boolean compare(Integer actual, Integer expected, ConditionOperation operation) {
        return switch (operation) {
            case EQUALS -> actual.equals(expected);
            case GREATER_THAN -> actual > expected;
            case LOWER_THAN -> actual < expected;
        };
    }
}
