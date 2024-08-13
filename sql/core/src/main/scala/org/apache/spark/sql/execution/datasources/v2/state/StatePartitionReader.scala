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
package org.apache.spark.sql.execution.datasources.v2.state

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.{StateVariableType, TransformWithStateVariableInfo}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.streaming.state.RecordType.{getRecordTypeAsString, RecordType}
import org.apache.spark.sql.types.{MapType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{NextIterator, SerializableConfiguration}

/**
 * An implementation of [[PartitionReaderFactory]] for State data source. This is used to support
 * general read from a state store instance, rather than specific to the operator.
 */
class StatePartitionReaderFactory(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo])
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val stateStoreInputPartition = partition.asInstanceOf[StateStoreInputPartition]
    if (stateStoreInputPartition.sourceOptions.readChangeFeed) {
      new StateStoreChangeDataPartitionReader(storeConf, hadoopConf,
        stateStoreInputPartition, schema, keyStateEncoderSpec, stateVariableInfoOpt)
    } else {
      new StatePartitionReader(storeConf, hadoopConf,
        stateStoreInputPartition, schema, keyStateEncoderSpec, stateVariableInfoOpt)
    }
  }
}

/**
 * An implementation of [[PartitionReader]] for State data source. This is used to support
 * general read from a state store instance, rather than specific to the operator.
 */
abstract class StatePartitionReaderBase(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo])
  extends PartitionReader[InternalRow] with Logging {
  protected val userKeySchema: Option[StructType] = {
    try {
      Option(
        SchemaUtil.getSchemaAsDataType(schema, "value").asInstanceOf[MapType]
      .keyType.asInstanceOf[StructType])
    } catch {
      case _: Exception =>
        None
    }
  }
  protected val keySchema = {
    val groupingKey = SchemaUtil.getSchemaAsDataType(
      schema, "key").asInstanceOf[StructType]
    new StructType()
      .add("key", groupingKey)
      .add("userKey", userKeySchema.get)
  }

  println("schema here: " + schema)
  println("key schema here: " + keySchema)
  protected val valueSchema = SchemaUtil.getSchemaAsDataType(
    schema, "value").asInstanceOf[MapType].valueType.asInstanceOf[StructType]
  println("value schema here: " + valueSchema)

  protected lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)

    val useColFamilies = if (stateVariableInfoOpt.isDefined) {
      true
    } else {
      false
    }

    val provider = StateStoreProvider.createAndInit(
      stateStoreProviderId, keySchema, valueSchema, keyStateEncoderSpec,
      useColumnFamilies = useColFamilies, storeConf, hadoopConf.value,
      useMultipleValuesPerKey = false)

    if (useColFamilies) {
      val store = provider.getStore(partition.sourceOptions.batchId + 1)
      require(stateVariableInfoOpt.isDefined)
      store.createColFamilyIfAbsent(stateVariableInfoOpt.get.stateName,
        keySchema, valueSchema, keyStateEncoderSpec, useMultipleValuesPerKey = false)
    }
    provider
  }

  protected val iter: Iterator[InternalRow]

  private var current: InternalRow = _

  override def next(): Boolean = {
    if (iter.hasNext) {
      current = iter.next()
      true
    } else {
      current = null
      false
    }
  }

  override def get(): InternalRow = current

  override def close(): Unit = {
    current = null
    provider.close()
  }
}

/**
 * An implementation of [[StatePartitionReaderBase]] for the normal mode of State Data
 * Source. It reads the the state at a particular batchId.
 */
class StatePartitionReader(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo])
  extends StatePartitionReaderBase(storeConf, hadoopConf, partition, schema,
    keyStateEncoderSpec, stateVariableInfoOpt) {

  private lazy val store: ReadStateStore = {
    partition.sourceOptions.fromSnapshotOptions match {
      case None => provider.getReadStore(partition.sourceOptions.batchId + 1)

      case Some(fromSnapshotOptions) =>
        if (!provider.isInstanceOf[SupportsFineGrainedReplay]) {
          throw StateStoreErrors.stateStoreProviderDoesNotSupportFineGrainedReplay(
            provider.getClass.toString)
        }
        provider.asInstanceOf[SupportsFineGrainedReplay]
          .replayReadStateFromSnapshot(
            fromSnapshotOptions.snapshotStartBatchId + 1,
            partition.sourceOptions.batchId + 1)
    }
  }

  override lazy val iter: Iterator[InternalRow] = {

    val stateVarName = stateVariableInfoOpt
      .map(_.stateName).getOrElse(StateStore.DEFAULT_COL_FAMILY_NAME)

    if (stateVariableInfoOpt.isDefined &&
      stateVariableInfoOpt.get.stateVariableType == StateVariableType.MapState) {
      // store.iterator will return Row((key.groupingKey, key.userKey), (value, (TTL)))
      // we need to return:
      // InternalRow(groupingKey, Map(userKey-value-TTL))

      val allRowsIter = store.iterator(stateVarName)
      println("allRowsIter hasNext: " + allRowsIter.hasNext)

      val pairs = allRowsIter.toSeq
      println("pairs.size here: " + pairs.size)

      val groupingKeySchema = SchemaUtil.getSchemaAsDataType(
        keySchema, "key").asInstanceOf[StructType]
      println("groupingKeySchema: " + groupingKeySchema)
      if (!pairs.isEmpty) {
        val previousGroupingKey = pairs.head.key.get(0, groupingKeySchema)
        val previousGroupingKeyContent = pairs.head.key.getString(0)
        println("previousGrouping key content: " + pairs.head.key.getString(0))
        var curMap = Map.empty[Any, Any]
        // this filter is sus

        val firstIter =
          pairs.filter(p => p.key.getString(0) == previousGroupingKeyContent)
        firstIter.foreach { p =>
          curMap = curMap + (p.key.get(1,
            userKeySchema.get) -> p.value)
        }
        println("i am here, previousGroupingKey: " + previousGroupingKey)
        println("user key content: " + firstIter.head.key.getString(1))
        println("value content: " + firstIter.head.value.getString(0))
        println("i am here, curMap: " + curMap.size)

        val mapKeysArrayData = ArrayData.toArrayData(curMap.keys.toArray)
        val mapValuesArrayData = ArrayData.toArrayData(curMap.values.toArray)
        val mapData = new ArrayBasedMapData(mapKeysArrayData, mapValuesArrayData)

        println("I am here, mapData: " + mapData)
        val row = new GenericInternalRow(3)
        row.update(0, previousGroupingKey)
        row.update(1, mapData)
        row.update(2, partition.partition)

        new NextIterator[InternalRow] {
          var seenFirst = false
          override protected def getNext(): InternalRow = {
            if (seenFirst) {
              finished = true
              null.asInstanceOf[InternalRow]
            } else {
              seenFirst = true
              row
            }
          }

          override protected def close(): Unit = {}
        }
      } else new Iterator[InternalRow] {
        override def hasNext: Boolean = false

        override def next(): InternalRow = null.asInstanceOf[InternalRow]
      }
    } else {
      store
        .iterator(stateVarName)
        .map(pair =>
          stateVariableInfoOpt match {
            case Some(stateVarInfo) =>
              val stateVarType = stateVarInfo.stateVariableType
              val hasTTLEnabled = stateVarInfo.ttlEnabled

              stateVarType match {
                case StateVariableType.ValueState =>
                  if (hasTTLEnabled) {
                    StateSchemaUtils.unifyStateRowPairWithTTL((pair.key, pair.value), valueSchema,
                      partition.partition)
                  } else {
                    StateSchemaUtils.unifyStateRowPair((pair.key, pair.value), partition.partition)
                  }

                case _ =>
                  println("I am here, pair is not empty")
                  throw new IllegalStateException(
                    s"Unsupported state variable type: $stateVarType")
              }

            case None =>
              StateSchemaUtils.unifyStateRowPair((pair.key, pair.value), partition.partition)
          }
        )
    }
  }

  override def close(): Unit = {
    store.abort()
    super.close()
  }
}

/**
 * An implementation of [[StatePartitionReaderBase]] for the readChangeFeed mode of State Data
 * Source. It reads the change of state over batches of a particular partition.
 */
class StateStoreChangeDataPartitionReader(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo])
  extends StatePartitionReaderBase(storeConf, hadoopConf, partition, schema,
    keyStateEncoderSpec, stateVariableInfoOpt) {

  private lazy val changeDataReader:
    NextIterator[(RecordType.Value, UnsafeRow, UnsafeRow, Long)] = {
    if (!provider.isInstanceOf[SupportsFineGrainedReplay]) {
      throw StateStoreErrors.stateStoreProviderDoesNotSupportFineGrainedReplay(
        provider.getClass.toString)
    }
    provider.asInstanceOf[SupportsFineGrainedReplay]
      .getStateStoreChangeDataReader(
        partition.sourceOptions.readChangeFeedOptions.get.changeStartBatchId + 1,
        partition.sourceOptions.readChangeFeedOptions.get.changeEndBatchId + 1)
  }

  override lazy val iter: Iterator[InternalRow] = {
    changeDataReader.iterator.map(unifyStateChangeDataRow)
  }

  override def close(): Unit = {
    changeDataReader.closeIfNeeded()
    super.close()
  }

  private def unifyStateChangeDataRow(row: (RecordType, UnsafeRow, UnsafeRow, Long)):
    InternalRow = {
    val result = new GenericInternalRow(5)
    result.update(0, row._4)
    result.update(1, UTF8String.fromString(getRecordTypeAsString(row._1)))
    result.update(2, row._2)
    result.update(3, row._3)
    result.update(4, partition.partition)
    result
  }
}
