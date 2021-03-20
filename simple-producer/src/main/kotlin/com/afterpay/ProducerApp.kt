package com.afterpay

class ProducerApp {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val coordinatesProducer = GpsCoordinatesProducer()
            coordinatesProducer.run()
        }
    }
}
