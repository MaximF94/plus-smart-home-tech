package ru.yandex.practicum.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.KafkaProperties;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.analyzer.service.ScenarioService;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> hubEventConsumer;
    private final SensorRepository sensorRepository;
    private final ScenarioService scenarioService;
    private final KafkaProperties kafkaProperties;

    @Override
    public void run() {
        String topic = kafkaProperties.getTopics().getHubs();
        try {
            hubEventConsumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<String, HubEventAvro> records =
                        hubEventConsumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    continue;
                }

                boolean batchProcessed = true;
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    try {
                        handleEvent(record.value());
                    } catch (Exception e) {
                        log.error("Ошибка обработки hub-события offset={}, partition={} — "
                                        + "пакет не будет зафиксирован",
                                record.offset(), record.partition(), e);
                        batchProcessed = false;
                        break;
                    }
                }

                if (batchProcessed) {
                    hubEventConsumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {

        } catch (Exception e) {
            log.error("Ошибка консьюмера hub-событий", e);
        } finally {
            try {
                hubEventConsumer.commitSync();
            } catch (WakeupException | IllegalStateException e) {
                log.warn("Не удалось зафиксировать смещения при остановке hubEventConsumer", e);
            } finally {
                try {
                    hubEventConsumer.close();
                } catch (Exception e) {
                    log.warn("Ошибка закрытия hubEventConsumer", e);
                }
                log.info("Консьюмер hub-событий закрыт");
            }
        }
    }

    private void handleEvent(HubEventAvro event) {
        Object payload = event.getPayload();
        String hubId = event.getHubId();

        if (payload instanceof DeviceAddedEventAvro deviceAdded) {
            handleDeviceAdded(hubId, deviceAdded);
        } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
            handleDeviceRemoved(deviceRemoved);
        } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
            scenarioService.saveScenario(hubId, scenarioAdded);
        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            scenarioService.removeScenario(hubId, scenarioRemoved.getName());
        } else {
            log.warn("Неизвестный тип hub-события: {}", payload.getClass());
        }

        log.info("Получено hub-событие: {}", event.getPayload());
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        if (sensorRepository.findById(event.getId()).isEmpty()) {
            ru.yandex.practicum.analyzer.model.Sensor sensor =
                    new ru.yandex.practicum.analyzer.model.Sensor();
            sensor.setId(event.getId());
            sensor.setHubId(hubId);
            sensorRepository.save(sensor);
            log.info("Сохранён датчик {} для хаба {}", event.getId(), hubId);
        } else {
            log.info("Датчик {} уже существует, пропускаем", event.getId());
        }
    }

    private void handleDeviceRemoved(DeviceRemovedEventAvro event) {
        sensorRepository.deleteById(event.getId());
        log.info("Удалён датчик {}", event.getId());
    }
}
