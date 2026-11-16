package ru.yandex.practicum.aggregator.config;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.util.Properties;

@Configuration
public class KafkaConsumerConfig {

    private KafkaConsumer<String, SensorEventAvro> consumer;

    @Bean
    public KafkaConsumer<String, SensorEventAvro> sensorEventConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "aggregator-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "ru.yandex.practicum.deserializer.SensorEventDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(props);
        return this.consumer;
    }

    @PreDestroy
    public void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

}
