/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.state.{StateStore, UnsafeRowPair}
import org.apache.spark.sql.streaming.MapState

class MapStateImpl[K, V](store: StateStore,
                         stateName: String) extends MapState[K, V] with Logging {
  /** Whether state exists or not. */
  override def exists(): Boolean = {
    true
  }

  /** Get the state value if it exists */
  override def getValue(key: K): V = {
    val encodedGroupingKey = StateEncoder.encodeKey(stateName) // unsafe rows of grouping key
    val encodeUserKey = StateEncoder.encodeUserKey(key)
    val unsafeRowValue = store.get(encodedGroupingKey, encodeUserKey, stateName)
    if (unsafeRowValue == null) {
      throw new UnsupportedOperationException("Cannot get value on empty key")
    }
    StateEncoder.decode(unsafeRowValue)
  }

  /** Check if the user key is contained in the map */
  override def containsKey(key: K): Boolean = {
    try {
      getValue(key) != null
    } catch {
      case _: Exception => false
    }
  }

  /** Update value for given user key */
  override def updateValue(key: K, value: V): Unit = {
    val encodedValue = StateEncoder.encodeValue(value)
    val encodedKey = StateEncoder.encodeKey(stateName)
    val encodedUserKey = StateEncoder.encodeUserKey(key)
    store.putWithMultipleKeys(encodedKey, encodedUserKey, encodedValue, stateName)
  }

  /** Get the map associated with grouping key */
  override def getMap(): Map[K, V] = {
    val encodedGroupingKey = StateEncoder.encodeKey(stateName)
    store.prefixScan(encodedGroupingKey, stateName)
      .map {
      case iter: UnsafeRowPair =>
        (StateEncoder.decode(iter.key), StateEncoder.decode(iter.value))
    }.toMap
  }

  /** Get the list of keys present in map associated with grouping key */
  override def getKeys(): Iterator[K] = {
    getMap().keys.iterator
  }

  /** Get the list of values present in map associated with grouping key */
  override def getValues(): Iterator[V] = {
    getMap().values.iterator
  }

  /** Remove user key from map state */
  override def removeKey(key: K): Unit = {
    val encodedKey = StateEncoder.encodeKey(stateName)
    val encodedUserKey = StateEncoder.encodeUserKey(key)
    store.removeWithMultipleKeys(encodedKey, encodedUserKey, stateName)
  }

  /** Remove this state. */
  override def remove(): Unit = {
    getKeys().foreach { itr =>
      removeKey(itr)
    }
  }
}


