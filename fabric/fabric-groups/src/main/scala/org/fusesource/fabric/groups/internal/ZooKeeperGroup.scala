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
import org.fusesource.fabric.groups.Group
import scala.collection.mutable.HashMap
import collection.JavaConversions._
import java.util.LinkedHashMap
import org.apache.zookeeper.KeeperException.{ConnectionLossException, NoNodeException}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{ChildData, PathChildrenCacheEvent, PathChildrenCacheListener, PathChildrenCache}
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ZooKeeperGroup {
  def members(curator: CuratorFramework, path: String):LinkedHashMap[String, Array[Byte]] = {
    var rc = new LinkedHashMap[String, Array[Byte]]
    curator.getChildren.forPath(path).sortWith((a,b)=> a < b).foreach { node =>
      try {
        if( node.matches("""0\d+""") ) {
          rc.put(node, curator.getData().forPath(path+"/"+node))
        } else {
          None
        }
      } catch {
        case e:Throwable =>
          e.printStackTrace
      }
    }
    rc

  }


}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ZooKeeperGroup(val curator: CuratorFramework, val root: String) extends Group with ConnectionStateListener with PathChildrenCacheListener with ChangeListenerSupport {

  val cache = new PathChildrenCache(curator, root, true)
  val joins = HashMap[String, Int]()

  var members = new LinkedHashMap[String, Array[Byte]]

  private def member_path_prefix = root + "/0"

  curator.getConnectionStateListenable.addListener(this)

  create(root)
  cache.getListenable.addListener(this)

  fire_cluster_change

  def childEvent(client:CuratorFramework, event:PathChildrenCacheEvent) {
    fire_cluster_change
  }

  def close = this.synchronized {
    joins.foreach { case (path, version) =>
      try {
        curator.delete().withVersion(version).forPath(member_path_prefix + path)
      } catch {
        case x:NoNodeException => // Already deleted.
      }
    }
    joins.clear
    curator.getConnectionStateListenable.removeListener(this)
    cache.close()
  }

  def connected = curator.getZookeeperClient.isConnected

  def stateChanged(curator:CuratorFramework, state:ConnectionState) {
    state match {
      case ConnectionState.CONNECTED => fireConnected()
      case ConnectionState.RECONNECTED => fireConnected()
      case _ => fireDisconnected()
    }
  }

  def join(data:Array[Byte]=null): String = this.synchronized {
    val id = curator.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(member_path_prefix, data).stripPrefix(member_path_prefix)
    joins.put(id, 0)
    id
  }

  def update(path:String, data:Array[Byte]=null): Unit = this.synchronized {
    joins.get(path) match {
      case Some(ver) =>
        val stat = curator.setData().withVersion(ver).forPath(member_path_prefix+path, data)
        joins.put(path, stat.getVersion)
      case None => throw new IllegalArgumentException("Has not joined locally: "+path)
    }
  }

  def leave(path:String): Unit = this.synchronized {
    joins.remove(path).foreach {
      case version =>
          try {
            curator.delete().withVersion(version).forPath(member_path_prefix + path)
          } catch {
            case x: NoNodeException => // Already deleted.
            case x: ConnectionLossException => // disconnected
          }
    }
  }

  private def fire_cluster_change: Unit = {
    this.synchronized {
      val t = cache.getCurrentData.filterNot { x:ChildData =>
        x.getPath == root || !x.getPath.stripPrefix(root).matches("""/0\d+""")
      }

      this.members = new LinkedHashMap()
      t.sortWith((a,b)=> a.getPath < b.getPath ).foreach { x=>
        this.members.put(x.getPath.stripPrefix(member_path_prefix), x.getData)
      }
    }
    fireChanged()
  }

  private def create(path: String, count : java.lang.Integer = 0): Unit = {
    try {
      if (curator.checkExists().forPath(path) != null) {
        return
      }
      try {
        // try create given path in persistent mode
        curator.create().creatingParentsIfNeeded().forPath(path)
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