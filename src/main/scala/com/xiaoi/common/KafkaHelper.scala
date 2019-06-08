package com.xiaoi.common

import java.util.Properties

/**
 * KafkaHelper
 * Kafka工具类
 *
 * Created by ligz on 16/2/17.
 */
object KafkaHelper {


  /**
   * 获取KafkaProducer
   * @param brokers
   * @return
   */
  def getKafkaProducer(brokers: String): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "1")
    props.put("retries", "0")
    new KafkaProducer[String, String](props)
  }


  /**
   * 构建json字符串格式的Kafka的消息
   * @param topic
   * @param jsonMsg
   * @return
   */
  def buildRecord(topic: String, jsonMsg: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, jsonMsg)
  }

  /**
   * 保存到Kafka中
   * @param producer
   * @param record
   */
  def saveToKafka(producer: KafkaProducer[String, String],
                  record: ProducerRecord[String, String]): Unit ={
    producer.send(record)
  }

  /**
   * 从Kafka中获取Direct InputDStream（No receiver）
   * @param ssc
   * @param params
   * @return
   */
  def getInputStreamFromKafka(ssc: StreamingContext,
                              params: Params): InputDStream[(String, String)] = {

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> params.kafkaBrokers,
      "group.id" -> params.kafkaConsumerGroupName,
      "zookeeper.connect" -> params.zkHosts
    )
    val kafkaDStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
     kafkaParams, Set(params.kafkaInTopic))
//    kafkaDStream.checkpoint(Seconds(30))
    kafkaDStream

  }

}
