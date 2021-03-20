package com.afterpay

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.Properties

class SimpleConsumer {
    companion object {

        @JvmStatic
        fun main(args: Array<String>) {
            val consumer = getKafkaConsumer()

            consumer.subscribe(listOf("gps_locations"))
            val records = consumer.poll(Duration.ofSeconds(1))


            val producer = getKafkaProducer()
            records.forEach { record ->
                val producerRecord = ProducerRecord("processed_gps_locations", record.key(), record.value())
                producer.send(producerRecord)
            }
        }

        private fun getKafkaConsumer(): KafkaConsumer<String, String> {
            val properties = Properties()
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-group")
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

            return KafkaConsumer<String, String>(properties)
        }

        private fun getKafkaProducer(): KafkaProducer<String, String> {
            val properties = Properties()
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
            return KafkaProducer<String, String>(properties)
        }
    }
}
