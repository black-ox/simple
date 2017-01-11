package com.ox.bigdata.util.sparkstreaming

import com.ox.bigdata.util.log.LogSupport
import com.ox.bigdata.util.kafka.KafkaZkUtils
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

object InputStreams extends LogSupport {

  def kafkaDirectStreamWithoutOffset(topic_set: Set[String],
                                     kafka_param: Map[String, String],
                                     streaming_context: StreamingContext): InputDStream[(String, String)] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streaming_context, kafka_param, topic_set)
  }

  def kafkaDirectStreamUsingOffset(zk_hosts: String,
                                   offset_zk_path: String,
                                   partition_num: Int,
                                   topic: String,
                                   kafka_param: Map[String, String],
                                   streaming_context: StreamingContext): InputDStream[(String, String)] = {

    val last_offsets = KafkaZkUtils.readOffset(zk_hosts, offset_zk_path, kafka_param.get("group.id").get, topic, partition_num)

    var from_offsets = Map[TopicAndPartition, Long]()

    for (offset <- last_offsets) {
      from_offsets += (TopicAndPartition(offset.topic, offset.partition) -> offset.untilOffset)
      LOG.info("start from, topic = " + offset.topic + ", partition = " + offset.partition + ", start_offset = " + offset.untilOffset)
    }

    val kafka_stream = if (from_offsets.nonEmpty && last_offsets.length == partition_num) {
      val msg_handler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streaming_context, kafka_param, from_offsets, msg_handler)
    } else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streaming_context, kafka_param, Set(topic))
    }

    save_offset(kafka_stream, zk_hosts, offset_zk_path, kafka_param.get("group.id").get)

    kafka_stream
  }

  protected def save_offset(kafka_stream: InputDStream[(String, String)],
                            zk_list: String,
                            offset_zk_path: String,
                            consumer_group: String) {
    kafka_stream.foreachRDD(rdd => {
      val offsetArr = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (offset <- offsetArr) {
        val ret = KafkaZkUtils.writeOffset(zk_list, offset_zk_path, consumer_group, offset)
        if (!ret) {
          LOG.warn("Unable to write offset : group_id = " + consumer_group + ", offset = " + offset)
        } else {
          LOG.info("Write offset : group_id = " + consumer_group + ", offset = " + offset)
        }
      }
    })
  }

}