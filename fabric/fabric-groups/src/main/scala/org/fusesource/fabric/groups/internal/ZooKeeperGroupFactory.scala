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

import org.fusesource.fabric.zookeeper.{ZkPath, IZKClient}
import java.util
import org.fusesource.fabric.groups.{Singleton, NodeState, Group, GroupFactory}

/**
 * <p>
 * </p>
 *
 * @author Guillaume Nodet
 */
class ZooKeeperGroupFactory(val zk: IZKClient) extends GroupFactory {
  def createGroup(name: String): Group = new ZooKeeperGroup(zk, ZkPath.CLUSTER.getPath(name))

  def createSingleton[T <: NodeState](clazz: Class[T]): Singleton[T] = new ClusteredSingleton[T](clazz)

  def createMember[T <: NodeState](clazz: Class[T]): Singleton[T] = createSingleton(clazz)
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ZooKeeperGroupFactory {

  def create(zk: IZKClient, path: String):Group = new ZooKeeperGroup(zk, path)
  def members(zk: IZKClient, path: String):util.LinkedHashMap[String, Array[Byte]] = ZooKeeperGroup.members(zk, path)
}



