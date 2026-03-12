package com.hethongtrongbanking.nienluancosonganh;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * ================================================================
 * KAFKA CONSUMER CONFIG
 * ================================================================
 * Cấu hình ConsumerFactory và KafkaListenerContainerFactory
 * để FraudTransactionConsumer có thể deserialize Payment object
 * từ JSON message trong Kafka.
 *
 * Tại sao cần class này?
 *   application.properties chỉ cấu hình được consumer mặc định.
 *   Khi cần nhiều group-id hoặc deserialize thành object cụ thể (Payment),
 *   cần định nghĩa factory rõ ràng qua @Configuration.
 * ================================================================
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * consumerFactory(): định nghĩa cách tạo Kafka consumer.
     *
     * JsonDeserializer<Payment>: tự động parse JSON → Payment object.
     * addTrustedPackages("*"): cho phép deserialize class từ bất kỳ package nào.
     * (Cần thiết vì producer và consumer có thể ở package khác nhau)
     */
    @Bean
    public ConsumerFactory<String, Payment> consumerFactory() {
        JsonDeserializer<Payment> deserializer = new JsonDeserializer<>(Payment.class);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeHeaders(false); // không dùng type header, parse thẳng vào Payment

        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "fraud-detection-group");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), deserializer);
    }

    /**
     * kafkaListenerContainerFactory(): factory được @KafkaListener tham chiếu.
     *
     * setConcurrency(3): chạy 3 luồng consumer song song.
     * → 3 message được xử lý đồng thời, tăng throughput khi tải cao.
     * → Phù hợp với Virtual Threads đã bật trong application.properties.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Payment> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Payment> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3); // 3 luồng xử lý song song
        return factory;
    }
}