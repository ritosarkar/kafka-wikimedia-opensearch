package com.learning.beginner.kafka.wikimedia.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class Topics {
    private final KafkaConfigurations appConfiguration;
    private final String wikimedia;


    @Autowired
    public Topics(KafkaConfigurations appConfiguration) {
        this.appConfiguration = appConfiguration;
        wikimedia = appConfiguration.getTopic().getFirst(); //wikimedia.recentchange
    }

}
