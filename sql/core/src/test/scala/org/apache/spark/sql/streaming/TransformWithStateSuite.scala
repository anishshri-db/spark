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

package org.apache.spark.sql.streaming

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.{AlsoTestWithChangelogCheckpointingEnabled, RocksDBStateStoreProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

object TransformWithStateSuiteUtils {
  val NUM_SHUFFLE_PARTITIONS = 5
}

// Class to verify stateful processor usage without timers
class RunningCountStatefulProcessor extends StatefulProcessor[String, String, (String, String)]
  with Logging {
  @transient var _countState: ValueState[Long] = _
  @transient var _processorHandle: StatefulProcessorHandle = _

  override def init(
      handle: StatefulProcessorHandle,
      outputMode: OutputMode) : Unit = {
    _processorHandle = handle
    assert(handle.getQueryInfo().getBatchId >= 0)
    assert(handle.getQueryInfo().getOperatorId == 0)
    assert(handle.getQueryInfo().getPartitionId >= 0 &&
      handle.getQueryInfo().getPartitionId < TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS)
    _countState = _processorHandle.getValueState[Long]("countState")
  }

  override def handleInputRow(
      key: String,
      inputRow: String,
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + 1
    if (count == 3) {
      _countState.remove()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }

  override def close(): Unit = {}
}

// Class to verify stateful processor usage with adding processing time timers
class RunningCountStatefulProcessorWithProcTimeTimer extends RunningCountStatefulProcessor {
  override def handleInputRow(
      key: String,
      inputRow: String,
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currCount = _countState.getOption().getOrElse(0L)
    if (currCount == 0 && (key == "a" || key == "c")) {
      _processorHandle.registerProcessingTimeTimer(timerValues.getCurrentProcessingTimeInMs()
        + 5000)
    }

    val count = currCount + 1
    if (count == 3) {
      _countState.remove()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }

  override def handleProcessingTimeTimers(
      key: String,
      expiryTimestampMs: Long,
      timerValues: TimerValues): Iterator[(String, String)] = {
    _countState.remove()
    Iterator((key, "-1"))
  }
}

// Class to verify stateful processor usage with adding/deleting processing time timers
class RunningCountStatefulProcessorWithAddRemoveProcTimeTimer
  extends RunningCountStatefulProcessor {
  @transient private var _timerState: ValueState[Long] = _

  override def init(
      handle: StatefulProcessorHandle,
      outputMode: OutputMode) : Unit = {
    super.init(handle, outputMode)
    _timerState = _processorHandle.getValueState[Long]("timerState")
  }

  override def handleInputRow(
      key: String,
      inputRows: String,
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currCount = _countState.getOption().getOrElse(0L)
    val count = currCount + inputRows.size
    _countState.update(count)
    if (key == "a") {
      var nextTimerTs: Long = 0L
      if (currCount == 0) {
        nextTimerTs = timerValues.getCurrentProcessingTimeInMs() + 5000
        _processorHandle.registerProcessingTimeTimer(nextTimerTs)
        _timerState.update(nextTimerTs)
      } else if (currCount == 1) {
        _processorHandle.deleteProcessingTimeTimer(_timerState.get())
        nextTimerTs = timerValues.getCurrentProcessingTimeInMs() + 7500
        _processorHandle.registerProcessingTimeTimer(nextTimerTs)
        _timerState.update(nextTimerTs)
      }
    }
    Iterator((key, count.toString))
  }

  override def handleProcessingTimeTimers(
      key: String,
      expiryTimestampMs: Long,
      timerValues: TimerValues): Iterator[(String, String)] = {
    _timerState.remove()
    Iterator((key, "-1"))
  }
}

// Class to verify stateful processor usage with adding event time timers
class RunningCountStatefulProcessorWithEventTimeTimer
  extends StatefulProcessor[String, (String, java.sql.Timestamp), (String, String)] {

  @transient var _countState: ValueState[Long] = _
  @transient var _processorHandle: StatefulProcessorHandle = _

  override def init(
       handle: StatefulProcessorHandle,
       outputMode: OutputMode): Unit = {
    _processorHandle = handle
    assert(handle.getQueryInfo().getBatchId >= 0)
    assert(handle.getQueryInfo().getOperatorId == 0)
    assert(handle.getQueryInfo().getPartitionId >= 0 && handle.getQueryInfo().getPartitionId < 5)
    _countState = _processorHandle.getValueState[Long]("countState")
  }

  override def close(): Unit = {}

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, java.sql.Timestamp)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currCount = _countState.getOption().getOrElse(0L)
    if (currCount == 0 && (key == "a" || key == "c")) {
      _processorHandle.registerEventTimeTimer(timerValues.getCurrentWatermarkInMs()
        + 5000)
    }

    val count = currCount + inputRows.size
    if (count == 3) {
      _countState.remove()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }

  override def handleEventTimeTimers(
     key: String,
     expiryTimestampMs: Long,
     timerValues: TimerValues): Iterator[(String, String)] = {
    _countState.remove()
    Iterator((key, "-1"))
  }
}

// Class to verify stateful processor usage with adding/deleting processing time timers
class RunningCountStatefulProcessorWithAddRemoveEventTimeTimer
  extends RunningCountStatefulProcessorWithEventTimeTimer {
  @transient private var _timerState: ValueState[Long] = _

  override def init(
     handle: StatefulProcessorHandle,
     outputMode: OutputMode) : Unit = {
    super.init(handle, outputMode)
    _timerState = _processorHandle.getValueState[Long]("timerState")
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, java.sql.Timestamp)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currCount = _countState.getOption().getOrElse(0L)
    val count = currCount + inputRows.size
    _countState.update(count)
    if (key == "a") {
      var nextTimerTs: Long = 0L
      if (currCount == 0) {
        nextTimerTs = timerValues.getCurrentWatermarkInMs() + 5000
        _processorHandle.registerEventTimeTimer(nextTimerTs)
        _timerState.update(nextTimerTs)
      } else if (currCount == 1) {
        _processorHandle.deleteEventTimeTimer(_timerState.get())
        nextTimerTs = timerValues.getCurrentWatermarkInMs() + 7500
        _processorHandle.registerEventTimeTimer(nextTimerTs)
        _timerState.update(nextTimerTs)
      }
    }
    Iterator((key, count.toString))
  }

  override def handleEventTimeTimers(
     key: String,
     expiryTimestampMs: Long,
     timerValues: TimerValues): Iterator[(String, String)] = {
    _timerState.remove()
    Iterator((key, "-1"))
  }
}

// Class to verify incorrect usage of stateful processor
class RunningCountStatefulProcessorWithError extends RunningCountStatefulProcessor {
  @transient private var _tempState: ValueState[Long] = _

  override def handleInputRow(
      key: String,
      inputRow: String,
      timerValues: TimerValues): Iterator[(String, String)] = {
    // Trying to create value state here should fail
    _tempState = _processorHandle.getValueState[Long]("tempState")
    Iterator.empty
  }
}

/**
 * Class that adds tests for transformWithState stateful streaming operator
 */
class TransformWithStateSuite extends StateStoreMetricsTest
  with AlsoTestWithChangelogCheckpointingEnabled {

  import testImplicits._

  test("transformWithState - streaming with rocksdb and invalid processor should fail") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
      TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessorWithError(),
          TimeoutMode.NoTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[SparkException] {
          (t: Throwable) => { assert(t.getCause
            .getMessage.contains("Cannot create state variable")) }
        }
      )
    }
  }

  test("transformWithState - streaming with rocksdb should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
      TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(),
          TimeoutMode.NoTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        CheckNewAnswer(("a", "1")),
        AddData(inputData, "a", "b"),
        CheckNewAnswer(("a", "2"), ("b", "1")),
        StopStream,
        StartStream(),
        AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
        CheckNewAnswer(("b", "2")),
        StopStream,
        StartStream(),
        AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
        CheckNewAnswer(("a", "1"), ("c", "1"))
      )
    }
  }

  test("transformWithState - streaming with rocksdb and processing time timer " +
   "should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessorWithProcTimeTimer(),
          TimeoutMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("a", "1")),

        AddData(inputData, "b"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("b", "1")),

        AddData(inputData, "b"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("a", "-1"), ("b", "2")),

        StopStream,
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "b"),
        AddData(inputData, "c"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("c", "1")),
        AddData(inputData, "d"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("c", "-1"), ("d", "1")),
        StopStream
      )
    }
  }

  test("transformWithState - streaming with rocksdb and processing time timer " +
   "and add/remove timers should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(
          new RunningCountStatefulProcessorWithAddRemoveProcTimeTimer(),
          TimeoutMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("a", "1")),

        AddData(inputData, "a"),
        AdvanceManualClock(2 * 1000),
        CheckNewAnswer(("a", "2")),
        StopStream,

        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "d"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("a", "-1"), ("d", "1")),
        StopStream
      )
    }
  }

  test("transformWithState - streaming with rocksdb and event time timer " +
    "should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      import java.sql.Timestamp
      val inputData = MemoryStream[(String, Timestamp)]
      val result = inputData.toDS()
        .select($"_1".as("value"), $"_2".as("eventTime"))
        .withWatermark("eventTime", "1 second")
        .as[(String, Timestamp)]
        .groupByKey(x => x._1)
        .transformWithState(new RunningCountStatefulProcessorWithEventTimeTimer(),
          TimeoutMode.EventTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(),
        AddData(inputData, ("a",
          Timestamp.valueOf("2023-08-01 00:00:00"))),

        AddData(inputData, ("b",
          Timestamp.valueOf("2023-08-02 00:00:00"))),
        CheckNewAnswer(("a", "1"), ("a", "-1"), ("b", "1")),

        AddData(inputData, ("b", Timestamp.valueOf("2023-08-03 00:00:00"))),
        CheckNewAnswer(("b", "2")), // watermark: 3rd august, timer t1 should have fired,

        AddData(inputData, ("b", Timestamp.valueOf("2023-08-04 00:00:00"))),
        AddData(inputData, ("c", Timestamp.valueOf("2023-08-04 00:00:00"))),
        CheckNewAnswer(("c", "-1"), ("c", "1")),
        AddData(inputData, ("d", Timestamp.valueOf("2023-08-06 00:00:00"))),
        CheckNewAnswer(("d", "1")),
        StopStream
      )
    }
  }

  test("transformWithState - streaming with rocksdb and event time timer " +
    "and add/remove timers should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      import java.sql.Timestamp
      val inputData = MemoryStream[(String, Timestamp)]
      val result = inputData.toDS()
        .select($"_1".as("value"), $"_2".as("eventTime"))
        .withWatermark("eventTime", "1 second")
        .as[(String, Timestamp)]
        .groupByKey(x => x._1)
        .transformWithState(new RunningCountStatefulProcessorWithAddRemoveEventTimeTimer(),
          TimeoutMode.EventTime(),
          OutputMode.Update())


      testStream(result, OutputMode.Update())(
        StartStream(),
        AddData(inputData, ("a", Timestamp.valueOf("2023-08-01 00:00:00"))),
        CheckNewAnswer(("a", "1"), ("a", "-1")),

        AddData(inputData, ("a", Timestamp.valueOf("2023-08-02 00:00:00"))),
        CheckNewAnswer(("a", "2"), ("a", "-1")),

        AddData(inputData, ("d", Timestamp.valueOf("2023-08-03 00:00:00"))),
        CheckNewAnswer(("d", "1")),
        StopStream
      )
    }
  }
}

class TransformWithStateValidationSuite extends StateStoreMetricsTest {
  import testImplicits._

  test("transformWithState - batch should fail") {
    val ex = intercept[Exception] {
      val df = Seq("a", "a", "b").toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor,
          TimeoutMode.NoTimeouts(),
          OutputMode.Append())
        .write
        .format("noop")
        .mode(SaveMode.Append)
        .save()
    }
    assert(ex.isInstanceOf[AnalysisException])
    assert(ex.getMessage.contains("not supported"))
  }

  test("transformWithState - streaming with hdfsStateStoreProvider should fail") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS()
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(),
        TimeoutMode.NoTimeouts(),
        OutputMode.Update())

    testStream(result, OutputMode.Update())(
      AddData(inputData, "a"),
      ExpectFailure[SparkException] {
        (t: Throwable) => { assert(t.getCause.getMessage.contains("not supported")) }
      }
    )
  }
}
