package com.afterpay.parallelconsumer

import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class ParallelConsumerApplication {
    companion object {
        private const val SOURCE_TOPIC = "gps_locations"
        private const val DESTINATION_TOPIC = "processed_gps_locations"

        @JvmStatic
        fun main(args: Array<String>) {
            val kafkaConsumer: Consumer<String, String> = getKafkaConsumer()
            val kafkaProducer: Producer<String, String> = getKafkaProducer()

            val options = ParallelConsumerOptions.builder<String, String>()
                .ordering(ParallelConsumerOptions.ProcessingOrder.PARTITION)
                .maxConcurrency(100)
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build()

            val parallelStreamProcessor = ParallelStreamProcessor.createEosStreamProcessor(options)

            parallelStreamProcessor.subscribe(listOf(SOURCE_TOPIC))

            parallelStreamProcessor.pollAndProduce { record ->
                println("Concurrently processing a record: $record")
                Thread.sleep(5000)

                ProducerRecord(DESTINATION_TOPIC, record.key(), record.value())
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
            return KafkaProducer<String, String>(properties)
        }
    }
}
