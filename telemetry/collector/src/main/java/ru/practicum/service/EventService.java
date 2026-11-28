package ru.practicum.service;

import ru.practicum.model.HubEvent;
import ru.practicum.model.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

public interface EventService {

    void processSensorEvent(SensorEvent sensorEvent);

    void processHubEvent(HubEvent hubEvent);

    void sendSensorEvent(SensorEventAvro avro, String hubId);

    void sendHubEvent(HubEventAvro avro, String hubId);
}
