package com.ox.bigdata.utils.zk

import com.ox.bigdata.utils.log.LogSupport
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent, Watcher, ZooKeeper}


object zkutils extends LogSupport {

  private def wait(zk: ZooKeeper) {
    if (zk != null) {
      var isConnected = false
      while (!isConnected) {
        if (zk.getState ne States.CONNECTING) {
          isConnected = true
        }
        try {
          Thread.sleep(100)
        }
        catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

  private def wait(zk: ZooKeeper, timeout: Int): Boolean = {
    var ret = false
    if (zk != null) {
      var left_t = timeout
      while (0 < left_t && !ret) {
        if (zk.getState ne States.CONNECTING) {
          ret = true
        }
        try {
          Thread.sleep(100)
          left_t -= 100
        }
        catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
    ret
  }

  def connect(zk_hosts: String, timeout: Int, watcher: Watcher): ZooKeeper = {
    var zk: ZooKeeper = null
    var is_ok: Boolean = false
    try {
      zk = new ZooKeeper(zk_hosts, timeout, if (watcher != null) watcher else new EmptyWatcher)
      wait(zk)
      if (zk.getState.isConnected) {
        is_ok = true
        if (watcher != null) {
          zk.register(watcher)
        }
      }
    }
    catch {
      case e: Exception => log.warn(e.getMessage)
    }
    if (!is_ok) {
      close(zk)
      zk = null
    }
    zk
  }

  def close(zk: ZooKeeper) {
    if (zk != null) {
      try {
        zk.close()
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  def createNodes(zk: ZooKeeper, path: String, is_persistent: Boolean): Boolean = {
    var ret = false
    val paths = path.split("/").filter(!_.isEmpty)
    if (paths.length > 0) {
      {
        val dirNum = paths.length - 1
        val results = for (i <- 0 until dirNum) yield {
          {
            val curPath = paths.dropRight(dirNum - i).mkString("/", "/", "")
            try {
              zk.create(curPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
              true
            }
            catch {
              case e: Exception => {
                if (e.isInstanceOf[KeeperException.NodeExistsException]) {
                  true
                }
                else {
                  log.warn(e.getMessage)
                  false
                }
              }
            }
          }
        }
        ret = !results.contains(false)
      }
      if (ret) {
        try {
          zk.create(paths.mkString("/", "/", ""), null, Ids.OPEN_ACL_UNSAFE, if (is_persistent) CreateMode.PERSISTENT else CreateMode.EPHEMERAL)
          ret = true
        }
        catch {
          case e: Exception => {
            log.warn(e.getMessage)
            ret = false
          }
        }
      }
    }
    ret
  }

  def deleteNode(zk: ZooKeeper, path: String): Boolean = {
    var res = false
    if (zk != null && path != null) {
      try {
        zk.delete(path, -1)
        res = true
      }
      catch {
        case e: Exception => {
          log.warn(e.getMessage)
          res = false
        }
      }
    }
    res
  }

  def deleteNode(zk_hosts: String, path: String): Boolean = {
    var res = false
    val zk = connect(zk_hosts, 30 * 1000, null)
    if (zk != null) {
      res = deleteNode(zk, path)
      close(zk)
    }
    res
  }

  def deleteNodes(zk: ZooKeeper, path: String): Boolean = {
    var res = false
    val paths: Array[String] = path.split("/").filter(!_.isEmpty)
    if (zk != null && paths.length > 0) {
      val results = for (i <- 0 until paths.length) yield {
        val curPath = paths.dropRight(i).mkString("/", "/", "")
        deleteNode(zk, curPath)
      }
      res = !results.contains(false)
    }
    res
  }



}

class EmptyWatcher extends Watcher {

  def process(event: WatchedEvent): Unit = {

  }
}
