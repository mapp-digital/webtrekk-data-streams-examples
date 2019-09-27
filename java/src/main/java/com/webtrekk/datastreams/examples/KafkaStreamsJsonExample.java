package com.webtrekk.datastreams.examples;

import com.webtrekk.datastreams.examples.config.ConfigKeys;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.ResourceBundle;

import static java.lang.Thread.UncaughtExceptionHandler;

class KafkaStreamsJsonExample {

    public static void main(String[] args) {
        MyKafkaStreamsFactory myKafkaStreamsFactory = new MyKafkaStreamsFactory();
        KafkaStreams kafkaStreams = myKafkaStreamsFactory.getKafkaStreams();
        kafkaStreams.setUncaughtExceptionHandler(myKafkaStreamsFactory.getUncaughtExceptionHandler());
        kafkaStreams.start();

        // Close the Stream when necessary
        // kafkaStreams.close()
    }

    private static class MyKafkaStreamsFactory {

        private String keySerde = "org.apache.kafka.common.serialization.Serdes$LongSerde";
        private String valueSerde = "org.apache.kafka.common.serialization.Serdes$StringSerde";

        private KafkaStreams getKafkaStreams() {
            ResourceBundle config = ResourceBundle.getBundle("application");
            StreamsBuilder streamBuilder = new StreamsBuilder();
            KStream<Long, String> stream = streamBuilder.stream(config.getString(ConfigKeys.Topic), Consumed.with(Serdes.Long(), Serdes.String()));
            stream.foreach((key, value) -> System.out.printf("key = %d, value = %s%n", key, value));

            return new KafkaStreams(streamBuilder.build(), getProperties(config));
        }

        private Properties getProperties(ResourceBundle config) {
            Properties props = new Properties();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString(ConfigKeys.GroupId));
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString(ConfigKeys.AutoOffsetResetPolicy));
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerde);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerde);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString(ConfigKeys.GroupId));
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(ConfigKeys.Endpoints));
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, config.getString(ConfigKeys.Streams.NumOfThreads));
            props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, config.getString(ConfigKeys.SecurityProtocol));
            props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, config.getString(ConfigKeys.Streams.DeserializationExceptionHandler));
            props.put(SaslConfigs.SASL_MECHANISM, config.getString(ConfigKeys.SecuritySaslMechanism));
            props.put(SaslConfigs.SASL_JAAS_CONFIG, getJaasConfig(config));
            return props;
        }

        private String getJaasConfig(ResourceBundle config) {
            String scramUser = config.getString(ConfigKeys.SecurityScramUsername);
            String scramPassword = config.getString(ConfigKeys.SecurityScramPassword);
            return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + scramUser + "\" password=\"" + scramPassword + "\";";
        }

        private UncaughtExceptionHandler getUncaughtExceptionHandler() {
            return (thread, exception) -> System.out.println("Exception running the Stream " + exception.getMessage());
        }

    }

}