package com.ox.bigdata.utils.zk

import com.ox.bigdata.utils.zk.kafka.Json
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.zookeeper.data.Stat

import scala.collection.{Map, Seq, mutable}

object ZKClientManager {

  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val TopicConfigPath = "/config/topics"
  val TopicConfigChangesPath = "/config/changes"
  val ControllerPath = "/controller"
  val ControllerEpochPath = "/controller_epoch"
  val ReassignPartitionsPath = "/admin/reassign_partitions"
  val DeleteTopicsPath = "/admin/delete_topics"
  val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"

  def getTopicPath(topic: String): String = BrokerTopicsPath + "/" + topic

  def getTopicPartitionsPath(topic: String): String = getTopicPath(topic) + "/partitions"

  def getTopicConfigPath(topic: String): String = TopicConfigPath + "/" + topic

  def getDeleteTopicPath(topic: String): String = DeleteTopicsPath + "/" + topic

//  val zkClient = new ZkClient("192.168.52.128:2181", 1000)

  def getPartitionsForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, Seq[Int]] = {
    getPartitionAssignmentForTopics(zkClient, topics).map { topicAndPartitionMap =>
      val topic = topicAndPartitionMap._1
      val partitionMap = topicAndPartitionMap._2
      //debug("partition assignment of /brokers/topics/%s is %s".format(topic, partitionMap))
      topic -> partitionMap.keys.toSeq.sortWith((s, t) => s < t)
    }
  }

  def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
      (Some(client.readData(path, stat)), stat)
    } catch {
      case e: ZkNoNodeException =>
        (None, stat)
      case e2: Throwable => throw e2
    }
    dataAndStat
  }

  def getPartitionAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = {
    val ret = new mutable.HashMap[String, Map[Int, Seq[Int]]]()
    topics.foreach { topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
      val partitionMap = jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) =>
          Json.parseFull(jsonPartitionMap) match {
            case Some(m) => m.asInstanceOf[Map[String, Any]].get("partitions") match {
              case Some(replicaMap) =>
                val m1 = replicaMap.asInstanceOf[Map[String, Seq[Int]]]
                m1.map(p => (p._1.toInt, p._2))
              case None => Map[Int, Seq[Int]]()
            }
            case None => Map[Int, Seq[Int]]()
          }
        case None => Map[Int, Seq[Int]]()
      }
      //debug("Partition map for /brokers/topics/%s is %s".format(topic, partitionMap))
      ret += (topic -> partitionMap)
    }
    ret
  }

}
