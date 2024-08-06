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
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.StateVarType
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.StateVarType.StateVarType
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.streaming.state.RecordType.{getRecordTypeAsString, RecordType}
import org.apache.spark.sql.types.StructType
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
    stateVarName: String,
    stateVarType: StateVarType) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val stateStoreInputPartition = partition.asInstanceOf[StateStoreInputPartition]
    if (stateStoreInputPartition.sourceOptions.readChangeFeed) {
      new StateStoreChangeDataPartitionReader(storeConf, hadoopConf,
        stateStoreInputPartition, schema, keyStateEncoderSpec, stateVarName,
        stateVarType)
    } else {
      new StatePartitionReader(storeConf, hadoopConf,
        stateStoreInputPartition, schema, keyStateEncoderSpec, stateVarName,
        stateVarType)
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
    stateVarName: String,
    stateVarType: StateVarType)
  extends PartitionReader[InternalRow] with Logging {
  protected val keySchema = SchemaUtil.getSchemaAsDataType(
    schema, "key").asInstanceOf[StructType]
  protected val valueSchema = SchemaUtil.getSchemaAsDataType(
    schema, "value").asInstanceOf[StructType]

  protected lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)

    val useColFamilies = if (stateVarName != StateStore.DEFAULT_COL_FAMILY_NAME) {
      true
    } else {
      false
    }

    val useMultipleValuesPerKey = if (stateVarType == StateVarType.listType) {
      true
    } else {
      false
    }

    val provider = StateStoreProvider.createAndInit(
      stateStoreProviderId, keySchema, valueSchema, keyStateEncoderSpec,
      useColumnFamilies = useColFamilies, storeConf, hadoopConf.value,
      useMultipleValuesPerKey = useMultipleValuesPerKey)

    if (useColFamilies) {
      val store = provider.getStore(partition.sourceOptions.batchId + 1)
      store.createColFamilyIfAbsent(stateVarName, keySchema, valueSchema, keyStateEncoderSpec,
        useMultipleValuesPerKey = useMultipleValuesPerKey)
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
    stateVarName: String,
    stateVarType: StateVarType)
  extends StatePartitionReaderBase(storeConf, hadoopConf, partition, schema,
    keyStateEncoderSpec, stateVarName, stateVarType) {

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
    if (stateVarType == StateVarType.listType) {
      logWarning(s"TEST: received value schema as $valueSchema")
      val encoderNew = ExpressionEncoder.apply(valueSchema)
      val encoder = Encoders.STRING
      val valExprEnc = encoderFor(encoder)
      val objDeserializer = encoderNew.resolveAndBind().createDeserializer()
      val pairIter = store.iterator(stateVarName)
      pairIter.map { pair =>
        val key = pair.key
        val result = store.valuesIterator(key, stateVarName)

/*        result.foreach { entry =>
          val res: String = objDeserializer.apply(entry)
          logWarning(s"TEST: received value=$res")
        } */

        var unsafeRowArr: Seq[UnsafeRow] = Seq.empty

        result.foreach { entry =>
          unsafeRowArr = unsafeRowArr :+ entry.copy()
        }

/*        var idx = 0
        var op: Seq[Any] = Seq.empty
        while (result.hasNext) {
          logWarning(s"TEST: entering here")
          val entry = result.next()
          op = op :+ objDeserializer.apply(entry)
//          arrData.update(idx, entry)
          idx = idx + 1
//          val res: String = objDeserializer.apply(entry)
  //        logWarning(s"TESTRES: received value=$res")
        } */

        val arrData = new GenericArrayData(unsafeRowArr.toArray)
        logWarning(s"TEST: size of arr data=${arrData.numElements()}")
//        val genRowWithSchema = new GenericRowWithSchema(op.toArray, valueSchema)

 //       val checkArr = arrData.toArray(IntegerType)
 //       logWarning(s"TEST: converted array=$checkArr")

/*        arrData.array.foreach { entry =>
          val res: String = objDeserializer.apply(entry.asInstanceOf[UnsafeRow])
          logWarning(s"TEST: received value=$res")
        } */

/*        val arrData = new GenericArrayData(result.size)
        result.zipWithIndex.foreach { case (entry, index) =>
     //     val procEntry = objDeserializer.apply(entry)
     //     val entryStr = procEntry.asInstanceOf[String]
       //   logWarning(s"TEST: received entryStr=$entryStr")
          arrData.update(index, entry.getBaseObject)
        } */
/*        val arrData = new Array[Any](result.size)
        result.zipWithIndex.foreach { case (entry, index) =>
          arrData.update(index, entry)
        } */
        unifyStateRowPairWithMultValues((pair.key, arrData))
      }
    } else {
      store.iterator(stateVarName).map(pair => unifyStateRowPair((pair.key, pair.value)))
    }
  }

  override def close(): Unit = {
    store.abort()
    super.close()
  }

  private def unifyStateRowPairWithMultValues(pair: (UnsafeRow,
    GenericArrayData)): InternalRow = {
    val row = new GenericInternalRow(4)
    row.update(0, pair._1)
    row.setNullAt(1)
    row.update(2, pair._2)
    row.update(3, partition.partition)
    row
  }

  private def unifyStateRowPair(pair: (UnsafeRow, UnsafeRow)): InternalRow = {
    val row = new GenericInternalRow(3)
    row.update(0, pair._1)
    row.update(1, pair._2)
    row.update(2, partition.partition)
    row
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
    stateVarName: String,
    stateVarType: StateVarType)
  extends StatePartitionReaderBase(storeConf, hadoopConf, partition, schema,
    keyStateEncoderSpec, stateVarName, stateVarType) {

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
