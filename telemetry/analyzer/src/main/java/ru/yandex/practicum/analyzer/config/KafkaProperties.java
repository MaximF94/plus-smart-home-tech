package ru.yandex.practicum.analyzer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {
    private String bootstrapServers;
    private Consumer consumer = new Consumer();
    private Topics topics = new Topics();

    @Getter
    @Setter
    public static class Consumer {
        private String snapshotsGroupId;
        private String hubEventsGroupId;
        private String autoOffsetReset;
        private boolean enableAutoCommit;
        private String keyDeserializer;
        private String snapshotValueDeserializer;
        private String hubEventValueDeserializer;
        private int maxPollRecords = 500;
        private int fetchMaxWaitMs = 500;
    }

    @Getter
    @Setter
    public static class Topics {
        private String snapshots;
        private String hubs;
    }
}
