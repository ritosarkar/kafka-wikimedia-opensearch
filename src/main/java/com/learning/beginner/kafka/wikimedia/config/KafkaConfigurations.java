package com.learning.beginner.kafka.wikimedia.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Data
@Configuration
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaConfigurations {
    private String bootstrapServers;
    private String autoOffsetResetConfig;
    private String enableAutoCommitConfig;
    private String groupId;
    private List<String> topic;
}
