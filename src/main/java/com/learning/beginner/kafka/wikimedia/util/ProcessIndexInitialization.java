package com.learning.beginner.kafka.wikimedia.util;

import com.learning.beginner.kafka.wikimedia.config.OpenSearchConfigurations;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProcessIndexInitialization {
    private final OpenSearchConfigurations openSearchConfigurations;
    private final RestHighLevelClient restHighLevelClient;

    @PostConstruct
    public void createOpenSearchIndex(){
        openSearchConfigurations.getIndices().forEach(index->{
            try {
                if(!restHighLevelClient.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)){
                    CreateIndexRequest createIndexRequest=new CreateIndexRequest(index);
                    restHighLevelClient.indices().create(createIndexRequest,RequestOptions.DEFAULT);
                    log.info("Index {} has been created!!",index);
                }else {
                    log.info("The {} is already present!!",index);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
