plugins {
    java
    kotlin("jvm")
}

group = "com.afterpay"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.apache.kafka:kafka-clients:2.7.0")
    implementation("org.slf4j:slf4j-simple:1.7.30")
}
