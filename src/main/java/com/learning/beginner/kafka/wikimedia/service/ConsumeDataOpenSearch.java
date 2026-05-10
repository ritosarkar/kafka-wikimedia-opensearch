package com.learning.beginner.kafka.wikimedia.service;

import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.learning.beginner.kafka.wikimedia.config.Indices;
import com.learning.beginner.kafka.wikimedia.config.OpenSearchConfigurations;
import com.learning.beginner.kafka.wikimedia.config.Topics;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;

@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumeDataOpenSearch {
    private final RestHighLevelClient restHighLevelClient;
    private final KafkaConsumer<String,String> kafkaConsumer;
    private final Indices indices;

    public void consumeData() throws IOException {
        while (true){
            ConsumerRecords<String,String> records=kafkaConsumer.poll(Duration.ofMillis(3000));
            log.info("Processing {} records -",records.count());
            for(ConsumerRecord<String,String> record:records){
                try {
                    IndexRequest indexRequest=new IndexRequest(indices.getWikimedia())
                            .source(record.value(), XContentType.JSON)
                            .id(extractId(record.value()));
                    //Send record to open search
                    IndexResponse indexResponse= restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                    // log.info("<<<<<Successfully processed a record:::===\n {}",record.value());
                    log.info("Fetch < {} > to see more....",indexResponse.getId());
                } catch (IOException e) {
                    log.info("Error occurred!!");
                }
            }
        }
    }


    //Strategy 1
    //Define Id using kafka record
    //String id= record.topic()+"_"+record.partition()+"_"+record.offset()
    //===================================================================//
    //Strategy 2
    //Extract id from JSON value

    private static String extractId(String json){
       //gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}
