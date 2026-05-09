package com.learning.beginner.kafka.wikimedia.config;

import lombok.RequiredArgsConstructor;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.DefaultConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.kafka.clients.CommonClientConfigs;


import java.net.URI;
import java.util.Collections;
import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class ApplicationClientConfigurations {
    private final OpenSearchConfigurations openSearchConfigurations;
    private final KafkaConfigurations kafkaConfigurations;
    private final Topics topics;

    @Bean
    public RestHighLevelClient createOpenSearchClient() {
        URI connectionUri = URI.create(openSearchConfigurations.getConnectionUri());
        String userInfo = connectionUri.getUserInfo();
        if (userInfo == null) {
            //Rest client without security
            return new RestHighLevelClient(RestClient.builder(
                    new HttpHost("http", connectionUri.getHost(), connectionUri.getPort())
            ));
        } else {
            String[] auth = userInfo.split(":");
            BasicCredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(new AuthScope(
                    new HttpHost(connectionUri.getScheme(), connectionUri.getHost(), connectionUri.getPort())
            ), new UsernamePasswordCredentials(auth[0], auth[1].toCharArray()));
            return new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connectionUri.getScheme(), connectionUri.getHost(), connectionUri.getPort()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        }
    }

    @Bean
    public KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigurations.getBootstrapServers());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfigurations.getGroupId());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfigurations.getAutoOffsetResetConfig());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaConfigurations.getEnableAutoCommitConfig());

        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(topics.getWikimedia()));
        return kafkaConsumer;
    }
}
