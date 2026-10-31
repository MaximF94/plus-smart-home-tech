package ru.practicum.service;

import ru.practicum.model.HubEvent;
import ru.practicum.model.SensorEvent;

public interface EventService {

    void processSensorEvent(SensorEvent sensorEvent);

    void processHubEvent(HubEvent hubEvent);
}
