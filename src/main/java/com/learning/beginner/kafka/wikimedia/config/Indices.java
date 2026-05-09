package com.learning.beginner.kafka.wikimedia.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class Indices {
    private final OpenSearchConfigurations openSearchConfigurations;
    private final String wikimedia;

    @Autowired
    public Indices(OpenSearchConfigurations openSearchConfigurations){
        this.openSearchConfigurations = openSearchConfigurations;
        wikimedia =  openSearchConfigurations.getIndices().getFirst();
    }
}
