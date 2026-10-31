package ru.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.practicum.mapper.EventMapper;
import ru.practicum.model.HubEvent;
import ru.practicum.model.SensorEvent;
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
    private final Producer<String, SpecificRecord> kafkaProducer;

    @Override
    public void processSensorEvent(SensorEvent sensorEvent) {
        SensorEventAvro avro = eventMapper.toAvro(sensorEvent);
        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>(
                SENSORS_TOPIC,
                null,
                sensorEvent.getTimestamp().toEpochMilli(),
                sensorEvent.getHubId(),
                avro
        );
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить событие датчика для хаба {}", sensorEvent.getHubId(), exception);
            } else {
                log.info("Событие датчика отправлено для хаба {}", sensorEvent.getHubId());
            }
        });
    }

    @Override
    public void processHubEvent(HubEvent hubEvent) {
        HubEventAvro avro = eventMapper.toAvro(hubEvent);
        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>(
                HUBS_TOPIC,
                null,
                hubEvent.getTimestamp().toEpochMilli(),
                hubEvent.getHubId(),
                avro
        );
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить событие хаба {}", hubEvent.getHubId(), exception);
            } else {
                log.info("Событие отправлено для хаба {}", hubEvent.getHubId());
            }
        });
    }
}
