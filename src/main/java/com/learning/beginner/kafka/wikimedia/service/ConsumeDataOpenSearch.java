package com.learning.beginner.kafka.wikimedia.service;


import com.google.gson.JsonParser;
import com.learning.beginner.kafka.wikimedia.config.Indices;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumeDataOpenSearch {
    private final RestHighLevelClient restHighLevelClient;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Indices indices;

    public void consumeData() throws InterruptedException, IOException {
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
            log.info("Processing {} records -", records.count());
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                var recordId = extractId(record.value());
                try {
                    IndexRequest indexRequest = new IndexRequest(indices.getWikimedia())
                            .source(record.value(), XContentType.JSON)
                            .id(recordId);
                    bulkRequest.add(indexRequest);
                    log.info("Record < {} > added in the bulk request....", recordId);
                    /*Send each record to open search
                    IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                    log.info("<<<<<Successfully processed a record:::===\n {}",record.value());
                    log.info("Fetch < {} > to see more....", indexResponse.getId());*/
                } catch (Exception e) {
                    log.error("Error occurred for {} !!", recordId);
                }
            }
            if (bulkRequest.numberOfActions() > 0) {
                BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                log.info("Inserted {} record(s).", bulkResponse.getItems().length);
                /*When enable.auto.commit = false; we have to manually commit the offset
                let's say here we will be pushing after every batch has been processed*/
                kafkaConsumer.commitSync();
                log.info("Offsets has been commited!! Zero records time-out.....");
                TimeUnit.MILLISECONDS.sleep(5);
            }

        }
    }


    //Strategy 1
    //Define id using kafka record
    //String id= record.topic()+"_"+record.partition()+"_"+record.offset()
    //===================================================================//
    //Strategy 2
    //Extract id from JSON value

    private static String extractId(String json) {
        //gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}
