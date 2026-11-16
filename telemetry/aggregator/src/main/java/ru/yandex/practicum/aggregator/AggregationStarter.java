package ru.yandex.practicum.aggregator;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    private static final String SNAPSHOTS_TOPIC = "telemetry.snapshots.v1";

    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final Producer<String, SpecificRecord> producer;
    private final SnapshotAggregator snapshotAggregator;

    public void start() {
        try {
            consumer.subscribe(List.of(SENSORS_TOPIC));

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    SensorEventAvro event = record.value();
                    Optional<SensorsSnapshotAvro> updated = snapshotAggregator.updateState(event);

                    updated.ifPresent(snapshot -> {
                        ProducerRecord<String, SpecificRecord> producerRecord = new ProducerRecord<>(
                                SNAPSHOTS_TOPIC,
                                null,
                                snapshot.getTimestamp().toEpochMilli(),
                                snapshot.getHubId(),
                                snapshot
                        );
                        producer.send(producerRecord);
                        log.info("Отправлен снапшот для хаба {}", snapshot.getHubId());
                    });
                }

                consumer.commitSync();
            }
        } catch (WakeupException ignored) {

        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }
}
