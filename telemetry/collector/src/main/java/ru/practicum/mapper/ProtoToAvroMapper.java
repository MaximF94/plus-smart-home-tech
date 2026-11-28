package ru.practicum.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

@Component
public class ProtoToAvroMapper {

    public SensorEventAvro toAvro(SensorEventProto proto) {
        Instant timestamp = Instant.ofEpochSecond(
                proto.getTimestamp().getSeconds(),
                proto.getTimestamp().getNanos()
        );

        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(proto.getId())
                .setHubId(proto.getHubId())
                .setTimestamp(timestamp);


        switch (proto.getPayloadCase()) {
            case SWITCH_SENSOR:
                builder.setPayload(SwitchSensorAvro.newBuilder()
                        .setState(proto.getSwitchSensor().getState())
                        .build());
                break;
            case MOTION_SENSOR:
                builder.setPayload(MotionSensorAvro.newBuilder()
                        .setLinkQuality(proto.getMotionSensor().getLinkQuality())
                        .setMotion(proto.getMotionSensor().getMotion())
                        .setVoltage(proto.getMotionSensor().getVoltage())
                        .build());
                break;

            case LIGHT_SENSOR:
                builder.setPayload(LightSensorAvro.newBuilder()
                        .setLinkQuality(proto.getLightSensor().getLinkQuality())
                        .setLuminosity(proto.getLightSensor().getLuminosity())
                        .build());
                break;

            case CLIMATE_SENSOR:
                builder.setPayload(ClimateSensorAvro.newBuilder()
                        .setTemperatureC(proto.getClimateSensor().getTemperatureC())
                        .setHumidity(proto.getClimateSensor().getHumidity())
                        .setCo2Level(proto.getClimateSensor().getCo2Level())
                        .build());
                break;

            case TEMPERATURE_SENSOR:
                builder.setPayload(TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(proto.getTemperatureSensor().getTemperatureC())
                        .setTemperatureF(proto.getTemperatureSensor().getTemperatureF())
                        .build());
                break;
            default:
                throw new IllegalArgumentException("Неизвестный тип payload: " + proto.getPayloadCase());
        }

        return builder.build();
    }

    public HubEventAvro toAvro(HubEventProto proto) {
        Instant timestamp = Instant.ofEpochSecond(
                proto.getTimestamp().getSeconds(),
                proto.getTimestamp().getNanos()
        );

        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(proto.getHubId())
                .setTimestamp(timestamp);

        switch (proto.getPayloadCase()) {
            case DEVICE_REMOVED:
                builder.setPayload(DeviceRemovedEventAvro.newBuilder()
                        .setId(proto.getDeviceRemoved().getId())
                        .build());
                break;

            case SCENARIO_REMOVED:
                builder.setPayload(ScenarioRemovedEventAvro.newBuilder()
                        .setName(proto.getScenarioRemoved().getName())
                        .build());
                break;

            case DEVICE_ADDED:
                builder.setPayload(DeviceAddedEventAvro.newBuilder()
                        .setId(proto.getDeviceAdded().getId())
                        .setType(DeviceTypeAvro.valueOf(proto.getDeviceAdded().getType().name()))
                        .build());
                break;

            case SCENARIO_ADDED:
                ScenarioAddedEventProto scenario = proto.getScenarioAdded();
                builder.setPayload(ScenarioAddedEventAvro.newBuilder()
                        .setName(scenario.getName())
                        .setConditions(scenario.getConditionList().stream()
                                .map(this::toAvro)
                                .toList())
                        .setActions(scenario.getActionList().stream()
                                .map(this::toAvro)
                                .toList())
                        .build());
                break;

            default:
                throw new IllegalArgumentException("Неизвестный тип payload: " + proto.getPayloadCase());
        }

        return builder.build();
    }

    private ScenarioConditionAvro toAvro(ScenarioConditionProto proto) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(proto.getSensorId())
                .setType(ConditionTypeAvro.valueOf(proto.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(proto.getOperation().name()));

        switch (proto.getValueCase()) {
            case BOOL_VALUE:
                builder.setValue(proto.getBoolValue());
                break;
            case INT_VALUE:
                builder.setValue(proto.getIntValue());
                break;
            default:
                builder.setValue(null);
        }

        return builder.build();
    }

    private DeviceActionAvro toAvro(DeviceActionProto proto) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(proto.getSensorId())
                .setType(ActionTypeAvro.valueOf(proto.getType().name()))
                .setValue(proto.hasValue() ? proto.getValue() : null)
                .build();
    }

}
