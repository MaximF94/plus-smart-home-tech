package ru.practicum.mapper;

import org.springframework.stereotype.Component;
import ru.practicum.model.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

@Component
public class EventMapper {

    public SensorEventAvro toAvro(SensorEvent event) {

        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp());

        if (event instanceof LightSensorEvent light) {
            builder.setPayload(LightSensorAvro.newBuilder()
                    .setLinkQuality(light.getLinkQuality())
                    .setLuminosity(light.getLuminosity())
                    .build());
        } else if (event instanceof SwitchSensorEvent sw) {
            builder.setPayload(SwitchSensorAvro.newBuilder()
                    .setState(sw.isState())
                    .build());
        } else if (event instanceof ClimateSensorEvent climate) {
            builder.setPayload(ClimateSensorAvro.newBuilder()
                    .setTemperatureC(climate.getTemperatureC())
                    .setHumidity(climate.getHumidity())
                    .setCo2Level(climate.getCo2Level())
                    .build());
        } else if (event instanceof MotionSensorEvent motion) {
            builder.setPayload(MotionSensorAvro.newBuilder()
                    .setLinkQuality(motion.getLinkQuality())
                    .setMotion(motion.isMotion())
                    .setVoltage(motion.getVoltage())
                    .build());
        } else if (event instanceof TemperatureSensorEvent temperature) {
            builder.setPayload(TemperatureSensorAvro.newBuilder()
                    .setId(temperature.getId())
                    .setHubId(temperature.getHubId())
                    .setTimestamp(temperature.getTimestamp())
                    .setTemperatureC(temperature.getTemperatureC())
                    .setTemperatureF(temperature.getTemperatureF())
                    .build());
        }

        return builder.build();
    }

    public HubEventAvro toAvro(HubEvent event) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp());

        if (event instanceof DeviceAddedEvent added) {
            builder.setPayload(DeviceAddedEventAvro.newBuilder()
                    .setId(added.getId())
                    .setType(DeviceTypeAvro.valueOf(added.getDeviceType().name()))
                    .build());

        } else if (event instanceof DeviceRemovedEvent removed) {
            builder.setPayload(DeviceRemovedEventAvro.newBuilder()
                    .setId(removed.getId())
                    .build());

        } else if (event instanceof ScenarioAddedEvent scenario) {
            List<ScenarioConditionAvro> conditions = scenario.getConditions().stream()
                    .map(this::toAvro)
                    .toList();

            List<DeviceActionAvro> actions = scenario.getActions().stream()
                    .map(this::toAvro)
                    .toList();

            builder.setPayload(ScenarioAddedEventAvro.newBuilder()
                    .setName(scenario.getName())
                    .setConditions(conditions)
                    .setActions(actions)
                    .build());

        } else if (event instanceof ScenarioRemovedEvent scenario) {
            builder.setPayload(ScenarioRemovedEventAvro.newBuilder()
                    .setName(scenario.getName())
                    .build());
        }

        return builder.build();
    }

    private ScenarioConditionAvro toAvro(ScenarioCondition condition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .setValue(condition.getValue())
                .build();
    }

    private DeviceActionAvro toAvro(DeviceAction action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .setValue(action.getValue())
                .build();
    }
}
