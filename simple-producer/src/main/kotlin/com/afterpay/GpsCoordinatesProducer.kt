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

class GpsCoordinatesProducer {
    fun run() {
        val numOfRecordsToProduce = 100

        val producer = KafkaProducer<String, String>(getKafkaProperties())

        (1..numOfRecordsToProduce).forEach { _ ->
            val (truckId, gpsCoordinates) = generateGpsCoordinates()
            val topicName = "gps_locations"
            val record = ProducerRecord(topicName, truckId, gpsCoordinates)
            producer.send(record)
        }
        producer.flush()
    }

    private fun generateGpsCoordinates(): Pair<String, String> {
        val timestamp = ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val fakeGpsLocation = (0..100000000).random()
        val gpsCoordinates = "$timestamp : $fakeGpsLocation"

        val truckId = "TRUCK-${(0..10).random()}"

        return Pair(truckId, gpsCoordinates)
    }

    private fun getKafkaProperties(): Properties {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        return properties
    }
}
