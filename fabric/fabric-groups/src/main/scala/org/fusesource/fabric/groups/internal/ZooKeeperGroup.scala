/*
 * Copyright (C) FuseSource, Inc.
 * http://fusesource.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fusesource.fabric.groups.internal

import org.apache.zookeeper._
import java.lang.String
import org.linkedin.zookeeper.tracker._
import org.fusesource.fabric.groups.{Singleton, Member, NodeState, Group}
import org.linkedin.zookeeper.client.LifecycleListener
import collection.JavaConversions._
import org.apache.zookeeper.KeeperException.{ConnectionLossException, NoNodeException}
import org.fusesource.fabric.zookeeper.IZKClient
import java.util
import collection.mutable
import org.slf4j.{LoggerFactory, Logger}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ZooKeeperGroup {

  val LOG: Logger = LoggerFactory.getLogger(classOf[ZooKeeperGroup])

  def members(zk: IZKClient, path: String):util.LinkedHashMap[String, Array[Byte]] = {
    val rc = new util.LinkedHashMap[String, Array[Byte]]
    (zk.getAllChildren(path) sortWith ((a, b) => a < b)).foreach { node =>
      try {
        if( node.matches("""0\d+""") ) {
          rc.put(node, zk.getData(path+"/"+node))
        } else {
          None
        }
      } catch {
        case e:Throwable =>
          LOG.warn("Error decoding group members", e)
      }
    }
    rc

  }


}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ZooKeeperGroup(val zk: IZKClient, val root: String) extends BaseGroup with LifecycleListener with ChangeListenerSupport {

  val tree = new ZooKeeperTreeTracker[Array[Byte]](zk, new ZKByteArrayDataReader, root, 1)
  val joins = mutable.HashMap[String, Int]()

  var members = new util.LinkedHashMap[String, Array[Byte]]

  private def member_path_prefix = root + "/0"

  zk.registerListener(this)

  create(root)
  tree.track(new NodeEventsListener[Array[Byte]]() {
    def onEvents(events: util.Collection[NodeEvent[Array[Byte]]]) {
      fire_cluster_change()
    }
  })
  fire_cluster_change()


  def close() {
    this.synchronized {
      joins.foreach {
        case (path, version) =>
          try {
            zk.delete(member_path_prefix + path, version)
          } catch {
            case x: NoNodeException => // Already deleted.
          }
      }
      joins.clear()
      tree.destroy()
      zk.removeListener(this)
    }
  }

  def connected = zk.isConnected
  def onConnected() {
    fireConnected()
  }
  def onDisconnected() {
    fireDisconnected()
  }

  def join(data:Array[Byte]=null): String = this.synchronized {
    val id = zk.create(member_path_prefix, data, CreateMode.EPHEMERAL_SEQUENTIAL).stripPrefix(member_path_prefix)
    joins.put(id, 0)
    id
  }

  def update(path:String, data:Array[Byte]=null) {
    this.synchronized {
      joins.get(path) match {
        case Some(ver) =>
          val stat = zk.setData(member_path_prefix + path, data, ver)
          joins.put(path, stat.getVersion)
        case None => throw new IllegalArgumentException("Has not joined locally: " + path)
      }
    }
  }

  def leave(path:String) {
    this.synchronized {
      joins.remove(path).foreach {
        case version =>
          try {
            zk.delete(member_path_prefix + path, version)
          } catch {
            case x: NoNodeException => // Already deleted.
            case x: ConnectionLossException => // disconnected
          }
      }
    }
  }


  def createSingleton[T <: NodeState](clazz: Class[T]): Singleton[T] = {
    val singleton = new ClusteredSingleton[T](clazz)
    singleton.start(this)
    singleton
  }

  def createMember[T <: NodeState](clazz: Class[T]): Member[T] = createSingleton(clazz)

  private def fire_cluster_change() {
    this.synchronized {
      val t = tree.getTree.toList.filterNot { x =>
      // don't include the root node, or nodes that don't match our naming convention.
        (x._1 == root) || !x._1.stripPrefix(root).matches("""/0\d+""")
      }

      this.members = new util.LinkedHashMap()
      t.sortWith((a,b)=> a._1 < b._1 ).foreach { x=>
        this.members.put(x._1.stripPrefix(member_path_prefix), x._2.getData)
      }
    }
    fireChanged()
  }

  private def create(path: String, count : java.lang.Integer = 0) {
    try {
      if (zk.exists(path, false) != null) {
        return
      }
      try {
        // try create given path in persistent mode
        zk.createOrSetWithParents(path, "", CreateMode.PERSISTENT)
      } catch {
        case ignore: KeeperException.NodeExistsException =>
      }
    } catch {
      case ignore : KeeperException.SessionExpiredException => {
        if (count > 20) {
          // we tried enought number of times
          throw new IllegalStateException("Cannot create path " + path, ignore)
        }
        // try to create path with increased counter value
        create(path, count + 1)
      }
    }
  }

}