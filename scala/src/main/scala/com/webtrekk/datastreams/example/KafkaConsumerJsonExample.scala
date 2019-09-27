package com.webtrekk.datastreams.example

import java.time.Duration
import java.util.{Properties, ResourceBundle}

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import scala.collection.JavaConverters._
import com.webtrekk.datastreams.example.config.ConfigKeys._

object KafkaConsumerJsonExample {

  private val keyDeserializer: String = classOf[LongDeserializer].getCanonicalName
  private val valueDeserializer: String = classOf[StringDeserializer].getCanonicalName

  private def getProperties(config: ResourceBundle): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getString(ClientId))
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString(GroupId))
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(Endpoints))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString(AutoOffsetResetPolicy))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getString(SecurityProtocol))
    props.put(SaslConfigs.SASL_MECHANISM, config.getString(SecuritySaslMechanism))
    props.put(SaslConfigs.SASL_JAAS_CONFIG, getJaasConfig(config))
    props
  }

  private def getJaasConfig(config: ResourceBundle): String = {
    val scramUser = config.getString(SecurityScramUsername)
    val scramPassword = config.getString(SecurityScramPassword)
    s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$scramUser" password="$scramPassword";"""
  }

  def main(args: Array[String]): Unit = {
    val config = ResourceBundle.getBundle("application")
    val consumer = new KafkaConsumer[Long, String](getProperties(config))
    consumer.subscribe(List(config.getString(Topic)).asJava)

    while (true) {
      val records = consumer.poll(Duration.ofMillis(100))
      records.iterator().forEachRemaining { record =>
        println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}")
      }
    }
    // Close the Consumer when necessary
    // consumer.close()
  }

}
