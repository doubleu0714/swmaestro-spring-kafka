package org.example.springkafka

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.fail

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = ["dev.phote.question.v1"],
    brokerProperties = ["listeners=PLAINTEXT://localhost:9092"],
    kraft = false
)
class KafkaListenerTest @Autowired constructor(
    private val myKafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
) {
    private val countDownLatch = CountDownLatch(1)

    @KafkaListener(
        containerFactory = "kafkaListenerContainerFactory",
        topics = ["dev.phote.question.v1"],
        groupId = "dev.phote.question.v1"
    )
    fun listen(record: ConsumerRecord<String, String>) {
        countDownLatch.countDown()
    }

    @Test
    fun `메시지를 수신한다`() {
        // given
        val message = QuestionMessage(id = 1, name = "테스트")

        // when
        myKafkaTemplate.send("dev.phote.question.v1", objectMapper.writeValueAsString(message)).get()

        // then
        if(!countDownLatch.await(1, TimeUnit.SECONDS)) {
            fail("메시지 수신 실패")
        }
    }
}

data class QuestionMessage(
    val id: Long,
    val name: String,
)
