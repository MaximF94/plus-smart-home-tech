package ru.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import ru.practicum.mapper.EventMapper;
import ru.practicum.model.HubEvent;
import ru.practicum.model.SensorEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventServiceImpl implements EventService {

    private static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    private static final String HUBS_TOPIC = "telemetry.hubs.v1";

    private final EventMapper eventMapper;
    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;

    @Override
    public void processSensorEvent(SensorEvent sensorEvent) {
        SensorEventAvro avro = eventMapper.toAvro(sensorEvent);
        kafkaTemplate.send(SENSORS_TOPIC, sensorEvent.getHubId(), avro)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Не удалось отправить событие датчика для хаба {}", sensorEvent.getHubId(), ex);
                    } else {
                        log.info("Событие датчика отправлено для хаба {}", sensorEvent.getHubId());
                    }
                });
    }

    @Override
    public void processHubEvent(HubEvent hubEvent) {
        HubEventAvro avro = eventMapper.toAvro(hubEvent);
        kafkaTemplate.send(HUBS_TOPIC, hubEvent.getHubId(), avro)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Не удалось отправить событие хаба, hubId={}", hubEvent.getHubId(), ex);
                    } else {
                        log.info("Событие отправлено для хаба {}", hubEvent.getHubId());
                    }
                });
    }
}
