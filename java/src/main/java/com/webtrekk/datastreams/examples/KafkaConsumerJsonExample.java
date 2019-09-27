package com.webtrekk.datastreams.examples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.ResourceBundle;

import static com.webtrekk.datastreams.examples.config.ConfigKeys.*;

public class KafkaConsumerJsonExample {

    public static void main(String[] args) {
        MyKafkaConsumerFactory kafkaConsumerFactory = new MyKafkaConsumerFactory();
        KafkaConsumer<Long, String> consumer = kafkaConsumerFactory.getConsumer();

        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Long, String> record : records)
                System.out.printf("offset = %d, key = %d, value = %s%n", record.offset(), record.key(), record.value());
        }

        // Close the consumer when necessary
        // consumer.close();

    }

    private static class MyKafkaConsumerFactory {
        private String keyDeserializer = LongDeserializer.class.getCanonicalName();
        private String valueDeserializer = StringDeserializer.class.getCanonicalName();

        public KafkaConsumer<Long, String> getConsumer() {
            ResourceBundle config = ResourceBundle.getBundle("application");
            KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(getProperties(config));
            consumer.subscribe(Collections.singletonList(config.getString(Topic)));
            return consumer;
        }

        private Properties getProperties(ResourceBundle config) {
            Properties props = new Properties();
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getString(ClientId));
            props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString(GroupId));
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(Endpoints));
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString(AutoOffsetResetPolicy));
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getString(SecurityProtocol));
            props.put(SaslConfigs.SASL_MECHANISM, config.getString(SecuritySaslMechanism));
            props.put(SaslConfigs.SASL_JAAS_CONFIG, getJaasConfig(config));
            return props;
        }

        private String getJaasConfig(ResourceBundle config) {
            String scramUser = config.getString(SecurityScramUsername);
            String scramPassword = config.getString(SecurityScramPassword);
            return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + scramUser + "\" password=\"" + scramPassword + "\";";
        }

    }

}