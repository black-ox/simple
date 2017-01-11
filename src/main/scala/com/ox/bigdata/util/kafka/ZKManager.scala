package com.ox.bigdata.util.kafka

import com.ox.bigdata.util.log.LogSupport
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkException, ZkInterruptedException, ZkNoNodeException}
import org.apache.zookeeper.data.Stat

import scala.collection.{Map, Seq, mutable}

/*
copied from kafka.utils.ZKUtils
* */
object ZKManager extends LogSupport {

  protected val ConsumersPath = "/consumers"
  protected val BrokerIdsPath = "/brokers/ids"
  protected val BrokerTopicsPath = "/brokers/topics"
  protected val TopicConfigPath = "/config/topics"
  protected val TopicConfigChangesPath = "/config/changes"
  protected val ControllerPath = "/controller"
  protected val ControllerEpochPath = "/controller_epoch"
  protected val ReassignPartitionsPath = "/admin/reassign_partitions"
  protected val DeleteTopicsPath = "/admin/delete_topics"
  protected val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"

  def getTopicPath(topic: String): String = BrokerTopicsPath + "/" + topic

  def getTopicPartitionsPath(topic: String): String = getTopicPath(topic) + "/partitions"

  def getTopicConfigPath(topic: String): String = TopicConfigPath + "/" + topic

  def getDeleteTopicPath(topic: String): String = DeleteTopicsPath + "/" + topic

  def usingZkClient(zk_hosts: String)(op: ZkClient => Unit): Unit = {
    val zk = new ZkClient(zk_hosts, 6000)
    try {
      op(zk)
    } catch {
      case e: Exception => LOG.error(s"ZooKeeper actions failed ！zookeeper hosts => $zk_hosts" + e.printStackTrace())
    } finally {
      zk.close()
    }
  }

  /**
    * Create a persistent node and set its ACLs.
    *
    * @param path
    * @param createParents
    * if true all parent dirs are created as well and no { @link ZkNodeExistsException} is thrown in case the
    * path already exists
    * @throws ZkInterruptedException
    * if operation was interrupted, or a required reconnection got interrupted
    * @throws IllegalArgumentException
    * if called from anything except the ZooKeeper event thread
    * @throws ZkException
    * if any ZooKeeper exception occurred
    * @throws RuntimeException
    * if any other exception occurs
    */

  def createPersistent(zk_hosts: String, path: String, createParents: Boolean = true): Unit = {
    usingZkClient(zk_hosts) {
      zkClient =>
        zkClient.createPersistent(path, createParents)
    }
  }

  /**
    * Create an ephemeral node.
    *
    * @param path
    * @throws ZkInterruptedException
    * if operation was interrupted, or a required reconnection got interrupted
    * @throws IllegalArgumentException
    * if called from anything except the ZooKeeper event thread
    * @throws ZkException
    * if any ZooKeeper exception occurred
    * @throws RuntimeException
    * if any other exception occurs
    */
  def createEphemeral(zk_hosts: String, path: String): Unit = {
    usingZkClient(zk_hosts) {
      zkClient =>
        zkClient.createEphemeral(path)
    }
  }

  def getChildren(zk_hosts: String, path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    var children: Seq[String] = Nil
    usingZkClient(zk_hosts) {
      zkClient =>
        children = zkClient.getChildren(path)
    }
    children
  }

  /**
    * Set the data for the node of the given path if such a node exists and the
    * given version matches the version of the node (if the given version is
    * -1, it matches any node's versions). Return the stat of the node.
    * <p>
    * This operation, if successful, will trigger all the watches on the node
    * of the given path left by getData calls.
    * <p>
    * A KeeperException with error code KeeperException.NoNode will be thrown
    * if no node with the given path exists.
    * <p>
    * A KeeperException with error code KeeperException.BadVersion will be
    * thrown if the given version does not match the node's version.
    * <p>
    * The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
    * Arrays larger than this will cause a KeeperException to be thrown.
    *
    * @param path
    *                the path of the node
    * @param data
    *                the data to set
    * @param version
    *                the expected matching version
    * @return the state of the node
    * @throws InterruptedException If the server transaction is interrupted.
    * @throws KeeperException If the server signals an error with a non-zero error code.
    * @throws IllegalArgumentException if an invalid path is specified
    */

  def getPartitionsForTopics(zk_hosts: String, topics: Seq[String]): mutable.Map[String, Seq[Int]] = {
    val ret = new mutable.HashMap[String, Seq[Int]]()
    usingZkClient(zk_hosts) {
      zkClient =>
        getPartitionAssignmentForTopics(zkClient, topics).map { topicAndPartitionMap =>
          val topic = topicAndPartitionMap._1
          val partitionMap = topicAndPartitionMap._2
          LOG.debug("partition assignment of /brokers/topics/%s is %s".format(topic, partitionMap))
          ret += (topic -> partitionMap.keys.toSeq.sortWith((s, t) => s < t))
        }
    }
    ret
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
      LOG.debug("Partition map for /brokers/topics/%s is %s".format(topic, partitionMap))
      ret += (topic -> partitionMap)
    }
    ret
  }

  def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
      (Some(client.readData(path, stat)), stat)
    } catch {
      case e: ZkNoNodeException =>
        (None, stat)
      case e2: Exception => throw e2
    }
    dataAndStat
  }

}



