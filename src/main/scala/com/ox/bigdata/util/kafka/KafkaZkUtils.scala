package com.ox.bigdata.util.kafka

import java.util

import com.ox.bigdata.util.log.LogSupport
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.zookeeper.ZooKeeper

import scala.collection.Seq


object KafkaZkUtils extends LogSupport {

  def usingZooKeeper(zkHosts: String)(op: ZooKeeper => Unit): Unit = {
    val zk = ZkUtils.connect(zkHosts, 30000, null)
    try {
      op(zk)
    } catch {
      case e: Exception => LOG.error("ZooKeeper actions failed ！" + e.printStackTrace())
    } finally {
      ZkUtils.close(zk)
    }
  }

  def writeOffset(zk_hosts: String,
                  base_path: String,
                  group: String,
                  offset: OffsetRange): Boolean = {
    var ret = false
    usingZooKeeper(zk_hosts) {
      zk =>
        if (zk != null) {
          val offset_path = base_path + "/" + group + "/" + offset.topic + "_" + offset.partition
          val value = offset.fromOffset + "_" + offset.untilOffset
          if (zk.exists(offset_path, false) == null) ZkUtils.createNodes(zk, offset_path, true)
          zk.setData(offset_path, value.getBytes, -1)
          ret = true
        }
    }
    ret
  }

  def getChildren(zk_hosts: String, path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    var result: Seq[String] = Nil
    usingZooKeeper(zk_hosts) {
      zk =>
        result = zk.getChildren(path, false)
    }
    result
  }

  def readOffset(zk_hosts: String,
                 base_path: String,
                 group: String,
                 topic: String,
                 partition_num: Int): List[OffsetRange] = {
    var offsets = List[OffsetRange]()
    usingZooKeeper(zk_hosts) {
      zk =>
        for (partition <- 0 until partition_num) {
          val offset_path = base_path + "/" + group + "/" + topic + "_" + partition

          if (zk.exists(offset_path, false) != null) {
            val value = new String(zk.getData(offset_path, false, null))
            val arr = value.split("_")
            if (arr != null && arr.length == 2 && arr(0).toLong < arr(1).toLong) {
              offsets ::= OffsetRange.create(topic, partition, arr(0).toLong, arr(1).toLong)
            }
          }
        }
    }
    offsets
  }

  def writeValue(zk_list: String, path: String, value: String): Boolean = {
    var ret = false
    usingZooKeeper(zk_list) {
      zk =>
        if (zk != null) {
          if (zk.exists(path, false) == null) {
            ZkUtils.createNodes(zk, path, true)
          }
          zk.setData(path, value.getBytes, -1)
          ret = true
        }
    }
    ret
  }

}
