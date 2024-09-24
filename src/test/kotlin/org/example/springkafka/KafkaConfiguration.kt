package org.example.springkafka

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import java.lang.Exception

@Configuration
class KafkaConfiguration {
    fun kafkaProducerConfigs(): Map<String, Any> =
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
        )

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<Int, String> =
        ConcurrentKafkaListenerContainerFactory<Int, String>().apply {
            consumerFactory = DefaultKafkaConsumerFactory<Any, Any>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
                    ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS to StringDeserializer::class.java
                )
            )
            setConcurrency(3)
            setCommonErrorHandler(object : CommonErrorHandler {
                override fun handleOne(
                    thrownException: Exception,
                    record: ConsumerRecord<*, *>,
                    consumer: Consumer<*, *>,
                    container: MessageListenerContainer
                ): Boolean {
                    println("에러처리")
                    return true
                }
            })
        }

    @Bean
    fun myKafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(DefaultKafkaProducerFactory(kafkaProducerConfigs()))
    }
}
