package com.afterpay

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*

class GpsCoordinatesProducer() {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val producer: KafkaProducer<String, String>

    init {
        val properties = getKafkaProperties()
        producer = KafkaProducer<String, String>(properties)
    }

    fun run() {
        val numOfRecordsToProduce = 100
        val noOfTimes = 5
        val sleepTimeMs = 50L

        logger.info("Producing ${numOfRecordsToProduce * noOfTimes} records")

        (1..noOfTimes).forEach { batch ->
            logger.info("Starting batch $batch.")
            (1..numOfRecordsToProduce).forEach {
                val truckId = "TRUCK-${(0..10).random()}"
                val gpsCoordinates =  """
                   ${ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)} : ${ (0..100000000).random() }
                """.trimIndent()
                produceRecord(truckId, gpsCoordinates)
            }
            producer.flush()
            logger.info("Done with batch $batch. Sleeping")
            Thread.sleep(sleepTimeMs)
        }
    }

    private fun getKafkaProperties(): Properties {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
        return properties
    }

    private fun produceRecord(key: String, value: String) {
        val topicName = "gps_locations"
        val record = ProducerRecord(topicName, key, value)
        producer.send(record)
    }
}
