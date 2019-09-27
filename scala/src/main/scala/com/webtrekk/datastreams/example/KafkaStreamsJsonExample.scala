package com.webtrekk.datastreams.example

import java.lang.Thread.UncaughtExceptionHandler
import java.util.{Properties, ResourceBundle}

import com.webtrekk.datastreams.example.config.ConfigKeys.{Streams, _}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object KafkaStreamsJsonExample {

  private val keySerde = "org.apache.kafka.common.serialization.Serdes$LongSerde"
  private val valueSerde = "org.apache.kafka.common.serialization.Serdes$StringSerde"

  private def getProperties(config: ResourceBundle): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString(GroupId))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString(AutoOffsetResetPolicy))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerde)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerde)
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString(GroupId))
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(Endpoints))
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, config.getString(Streams.NumOfThreads))
    props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, config.getString(Streams.DeserializationExceptionHandler))
    props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, config.getString(SecurityProtocol))
    props.put(SaslConfigs.SASL_MECHANISM, config.getString(SecuritySaslMechanism))
    props.put(SaslConfigs.SASL_JAAS_CONFIG, getJaasConfig(config))
    props
  }

  private def getJaasConfig(config: ResourceBundle): String = {
    val scramUser = config.getString(SecurityScramUsername)
    val scramPassword = config.getString(SecurityScramPassword)
    s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$scramUser" password="$scramPassword";"""
  }

  private val getUncaughtExceptionHandler: UncaughtExceptionHandler = {
    case (_, ex) =>
      println(s"Exception running the Stream $ex")
      ()
  }

  def main(args: Array[String]): Unit = {
    val kafkaStreams = getKafkaStreams
    kafkaStreams.setUncaughtExceptionHandler(getUncaughtExceptionHandler)
    kafkaStreams.start()

    // Close the Stream when necessary
    // kafkaStreams.close()
  }

  private def getKafkaStreams: KafkaStreams = {
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.Serdes._

    val config = ResourceBundle.getBundle("application")
    val streamBuilder = new StreamsBuilder()

    streamBuilder
      .stream[Long, String](config.getString(Topic))
      .foreach {
        case (key, value) =>
          println(s"key = $key, value = $value")
      }

    new KafkaStreams(streamBuilder.build(), getProperties(config))
  }

}
