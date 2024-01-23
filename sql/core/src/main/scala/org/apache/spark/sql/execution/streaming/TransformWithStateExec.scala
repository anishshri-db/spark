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

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalGroupState}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.StateStoreAwareZipPartitionsHelper
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StatefulProcessor, StatefulProcessorWithInitialState, TimeoutMode}
import org.apache.spark.sql.types._
import org.apache.spark.util.{CompletionIterator, SerializableConfiguration}

/**
 * Physical operator for executing `TransformWithState`
 *
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 * @param groupingAttributes used to group the data
 * @param dataAttributes used to read the data
 * @param statefulProcessor processor methods called on underlying data
 * @param timeoutMode defines the timeout mode
 * @param outputMode defines the output mode for the statefulProcessor
 * @param outputObjAttr Defines the output object
 * @param batchTimestampMs processing timestamp of the current batch.
 * @param eventTimeWatermarkForLateEvents event time watermark for filtering late events
 * @param eventTimeWatermarkForEviction event time watermark for state eviction
 * @param child the physical plan for the underlying data
 */
case class TransformWithStateExec(
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    initialStateDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    initialStateGroupingAttributes: Seq[Attribute],
    initialStateDataAttributes: Seq[Attribute],
    statefulProcessor: StatefulProcessor[Any, Any, Any],
    timeoutMode: TimeoutMode,
    outputMode: OutputMode,
    outputObjAttr: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo],
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    child: SparkPlan,
    hasInitialState: Boolean,
    initialState: SparkPlan)
  extends BinaryExecNode with StateStoreWriter with WatermarkSupport with ObjectProducerExec {

  override def shortName: String = "transformWithStateExec"

  // TODO: update this to run no-data batches when timer support is added
  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = false

  override def left: SparkPlan = child

  override def right: SparkPlan = initialState
  override protected def withNewChildrenInternal(
    newLeft: SparkPlan, newRight: SparkPlan): TransformWithStateExec =
    copy(child = newLeft, initialState = newRight)

  override def keyExpressions: Seq[Attribute] = groupingAttributes

  protected val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)

  protected val schemaForValueRow: StructType = new StructType().add("value", BinaryType)

  /**
   * Distribute by grouping attributes - We need the underlying data and the initial state data
   * to have the same grouping so that the data are co-lacated on the same task.
   */
  override def requiredChildDistribution: Seq[Distribution] = {
    StatefulOperatorPartitioning.getCompatibleDistribution(groupingAttributes,
      getStateInfo, conf) ::
      StatefulOperatorPartitioning.getCompatibleDistribution(initialStateGroupingAttributes,
        getStateInfo, conf) ::
      Nil
  }

  /**
   * Ordering needed for using GroupingIterator.
   * We need the initial state to also use the ordering as the data so that we can co-locate the
   * keys from the underlying data and the initial state.
   */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(
    groupingAttributes.map(SortOrder(_, Ascending)),
    initialStateGroupingAttributes.map(SortOrder(_, Ascending))
  )

  private def handleInputRows(keyRow: UnsafeRow, valueRowIter: Iterator[InternalRow]):
    Iterator[InternalRow] = {
    val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)

    val getValueObj =
      ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)

    val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjectType)

    val keyObj = getKeyObj(keyRow)  // convert key to objects
    ImplicitGroupingKeyTracker.setImplicitKey(keyObj)
    val valueObjIter = valueRowIter.map(getValueObj.apply)
    val mappedIterator = statefulProcessor.handleInputRows(keyObj, valueObjIter,
      new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForLateEvents)).map { obj =>
        getOutputRow(obj)
    }
    ImplicitGroupingKeyTracker.removeImplicitKey()
    mappedIterator
  }

  private def processInitialStateRows(keyRow: UnsafeRow, initStateIter: Iterator[InternalRow]):
    Unit = {
    val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)

    val getStateValueObj =
      ObjectOperator.deserializeRowToObject(initialStateDeserializer, initialStateDataAttributes)

    val keyObj = getKeyObj(keyRow) // convert key to objects
    ImplicitKeyTracker.setImplicitKey(keyObj)
    val initStateObjIter = initStateIter.map(getStateValueObj.apply)

    initStateObjIter.foreach { initState =>
      statefulProcessor
        .asInstanceOf[StatefulProcessorWithInitialState[Any, Any, Any, Any]]
        .handleInitialState(keyObj, initState)
    }
    ImplicitKeyTracker.removeImplicitKey()
  }

  private def processNewData(dataIter: Iterator[InternalRow]): Iterator[InternalRow] = {
    val groupedIter = GroupedIterator(dataIter, groupingAttributes, child.output)
    groupedIter.flatMap { case (keyRow, valueRowIter) =>
      val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
      handleInputRows(keyUnsafeRow, valueRowIter)
    }
  }

  private def processDataWithPartition(
      iter: Iterator[InternalRow],
      store: StateStore,
      processorHandle: StatefulProcessorHandleImpl,
      initialStateIterOption: Option[Iterator[InternalRow]] = None):
    CompletionIterator[InternalRow, Iterator[InternalRow]] = {
    val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
    val commitTimeMs = longMetric("commitTimeMs")

    val currentTimeNs = System.nanoTime
    val updatesStartTimeNs = currentTimeNs

    // If timeout is based on event time, then filter late data based on watermark
    val filteredIter = watermarkPredicateForDataForLateEvents match {
      case Some(predicate) =>
        applyRemovingRowsOlderThanWatermark(iter, predicate)
      case _ =>
        iter
    }

    val outputIterator = initialStateIterOption match {
      case Some(initStateIter) if initStateIter.hasNext =>
        processNewDataWithInitialState(filteredIter, initStateIter)
      case _ => processNewData(filteredIter)
    }

    processorHandle.setHandleState(StatefulProcessorHandleState.DATA_PROCESSED)
    // Return an iterator of all the rows generated by all the keys, such that when fully
    // consumed, all the state updates will be committed by the state store
    CompletionIterator[InternalRow, Iterator[InternalRow]](outputIterator, {
      // Note: Due to the iterator lazy execution, this metric also captures the time taken
      // by the upstream (consumer) operators in addition to the processing in this operator.
      allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)
      commitTimeMs += timeTakenMs {
        store.commit()
      }
      setStoreMetrics(store)
      setOperatorMetrics()
      statefulProcessor.close()
      processorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
    })
  }

  private def processNewDataWithInitialState(dataIter: Iterator[InternalRow],
     initStateIter: Iterator[InternalRow]): Iterator[InternalRow] = {

    val groupedChildDataIter = GroupedIterator(dataIter, groupingAttributes, child.output)
    val groupedInitialStateIter =
      GroupedIterator(initStateIter, initialStateGroupingAttributes, initialState.output)

    groupedChildDataIter.flatMap { case (keyRow, valueRowIter) =>
      val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
      handleInputRows(keyUnsafeRow, valueRowIter)
    }

    // Create a CoGroupedIterator that will group the two iterators together for every key group.
    new CoGroupedIterator(
      groupedChildDataIter, groupedInitialStateIter, groupingAttributes).flatMap {
      case (keyRow, valueRowIter, initialStateRowIter) =>
        // TODO in design doc: trying to re-initialize state for the same
        // grouping key will result in an error?
        val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
        handleInputRows(keyUnsafeRow, valueRowIter)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    if (hasInitialState) {
      // If the user provided initial state we need to have the initial state and the
      // data in the same partition so that we can still have just one commit at the end.
      val storeConf = new StateStoreConf(session.sqlContext.sessionState.conf)
      val hadoopConfBroadcast = sparkContext.broadcast(
        new SerializableConfiguration(session.sqlContext.sessionState.newHadoopConf()))
      child.execute().stateStoreAwareZipPartitions(
        initialState.execute(),
        getStateInfo,
        storeNames = Seq(),
        session.sqlContext.streams.stateStoreCoordinator) {
        // The state store aware zip partitions will provide us with two iterators,
        // child data iterator and the initial state iterator per partition.
        case (partitionId, childDataIterator, initStateIterator) =>
          val stateStoreId = StateStoreId(stateInfo.get.checkpointLocation,
            stateInfo.get.operatorId, partitionId)
          val storeProviderId = StateStoreProviderId(stateStoreId, stateInfo.get.queryRunId)
          val store = StateStore.get(
            storeProviderId,
            schemaForKeyRow,
            schemaForValueRow,
            0,
            stateInfo.get.storeVersion,
            useColumnFamilies = true,
            storeConf, hadoopConfBroadcast.value.value
          )
          val processorHandle = new StatefulProcessorHandleImpl(store, getStateInfo.queryRunId)
          assert(processorHandle.getHandleState == StatefulProcessorHandleState.CREATED)
          statefulProcessor.init(processorHandle, outputMode)
          processorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)

          // Check if is first batch
          // Only process initial states for first batch
          if (processorHandle.getQueryInfo().getBatchId == 0) {
            val groupedInitialStateIter =
              GroupedIterator(initStateIterator,
                initialStateGroupingAttributes, initialState.output)
            groupedInitialStateIter.foreach {
              case (keyRow, valueRowIter) =>
                processInitialStateRows(keyRow.asInstanceOf[UnsafeRow],
                  valueRowIter)
            }
          }

          val result = processDataWithPartition(childDataIterator, store,
            processorHandle, Option(initStateIterator))
          result
      }
    } else {
      child.execute().mapPartitionsWithStateStore[InternalRow](
        getStateInfo,
        schemaForKeyRow,
        schemaForValueRow,
        numColsPrefixKey = 0,
        session.sqlContext.sessionState,
        Some(session.sqlContext.streams.stateStoreCoordinator),
        useColumnFamilies = true
      ) {
        case (store: StateStore, singleIterator: Iterator[InternalRow]) =>
          val processorHandle = new StatefulProcessorHandleImpl(store, getStateInfo.queryRunId)
          assert(processorHandle.getHandleState == StatefulProcessorHandleState.CREATED)
          statefulProcessor.init(processorHandle, outputMode)
          processorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
          val result = processDataWithPartition(singleIterator, store, processorHandle)
          result
      }
    }
  }
}

object TransformWithStateExec {

  def foundDuplicateInitialKeyException(): Exception = {
    throw new IllegalArgumentException("The initial state provided contained " +
      "multiple rows(state) with the same key. Make sure to de-duplicate the " +
      "initial state before passing it.")
  }

  /**
   * Plan logical flatmapGroupsWIthState for batch queries
   * If the initial state is provided, we create an instance of the CoGroupExec, if the initial
   * state is not provided we create an instance of the MapGroupsExec
   */
  // scalastyle:off argcount
  def generateSparkPlanForBatchQueries(
      statefulProcessor: StatefulProcessor[Any, Any, Any],
      keyDeserializer: Expression,
      valueDeserializer: Expression,
      initialStateDeserializer: Expression,
      groupingAttributes: Seq[Attribute],
      initialStateGroupAttrs: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      initialStateDataAttrs: Seq[Attribute],
      outputObjAttr: Attribute,
      timeoutMode: TimeoutMode,
      hasInitialState: Boolean,
      initialState: SparkPlan,
      child: SparkPlan): SparkPlan = {
    // how to deal with initial state?
    if (hasInitialState) {
      val watermarkPresent = child.output.exists {
        case a: Attribute if a.metadata.contains(EventTimeWatermark.delayKey) => true
        case _ => false
      }
      val func = (keyRow: Any, values: Iterator[Any], states: Iterator[Any]) => {
        // Check if there is only one state for every key.
        var foundInitialStateForKey = false
        val optionalStates = states.map { stateValue =>
          if (foundInitialStateForKey) {
            foundDuplicateInitialKeyException()
          }
          foundInitialStateForKey = true
          stateValue
        }.toArray

        // Create group state object
        val groupState = GroupStateImpl.createForStreaming(
          optionalStates.headOption,
          System.currentTimeMillis,
          GroupStateImpl.NO_TIMESTAMP,
          GroupStateTimeout.NoTimeout(),
          hasTimedOut = false,
          watermarkPresent)

         val currentTimestampMs = Some(groupState.getCurrentProcessingTimeMs())
         val currentWatermarkMs = Some(groupState.getCurrentWatermarkMs())
        // Call user function with the state and values for this key
        statefulProcessor.handleInputRows(keyRow, values,
          new TimerValuesImpl(currentTimestampMs, currentWatermarkMs))
      }
      CoGroupExec(
        func, keyDeserializer, valueDeserializer, initialStateDeserializer, groupingAttributes,
        initialStateGroupAttrs, dataAttributes, initialStateDataAttrs, Seq.empty, Seq.empty,
        outputObjAttr, child, initialState)
    } else {
      // what to pass in to MapGroupsExec?
      MapGroupsExec(
        statefulProcessor.handleInputRows, keyDeserializer, valueDeserializer, groupingAttributes,
        dataAttributes, Seq.empty, outputObjAttr, GroupStateTimeout.NoTimeout(), child)
    }
  }
}