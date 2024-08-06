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

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{AlsoTestWithChangelogCheckpointingEnabled, RocksDBStateStoreProvider}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.internal.SQLConf

case class InputRow(key: String, action: String, value: String)

class TestListStateProcessor
  extends StatefulProcessor[String, InputRow, (String, String)] {

  @transient var _listState: ListState[String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _listState = getHandle.getListState("testListState", Encoders.STRING)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[InputRow],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {

    var output = List[(String, String)]()

    for (row <- rows) {
      if (row.action == "emit") {
        output = (key, row.value) :: output
      } else if (row.action == "emitAllInState") {
        _listState.get().foreach { v =>
          output = (key, v) :: output
        }
        _listState.clear()
      } else if (row.action == "append") {
        _listState.appendValue(row.value)
      } else if (row.action == "appendAll") {
        _listState.appendList(row.value.split(","))
      } else if (row.action == "put") {
        _listState.put(row.value.split(","))
      } else if (row.action == "remove") {
        _listState.clear()
      } else if (row.action == "tryAppendingNull") {
        _listState.appendValue(null)
      } else if (row.action == "tryAppendingNullValueInList") {
        _listState.appendList(Array(null))
      } else if (row.action == "tryAppendingNullList") {
        _listState.appendList(null)
      } else if (row.action == "tryPutNullList") {
        _listState.put(null)
      } else if (row.action == "tryPuttingNullInList") {
        _listState.put(Array(null))
      } else if (row.action == "tryPutEmptyList") {
        _listState.put(Array())
      } else if (row.action == "tryAppendingEmptyList") {
        _listState.appendList(Array())
      }
    }

    output.iterator
  }
}

class ToggleSaveAndEmitProcessor
  extends StatefulProcessor[String, String, String] {

  @transient var _listState: ListState[String] = _
  @transient var _valueState: ValueState[Boolean] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _listState = getHandle.getListState("testListState", Encoders.STRING)
    _valueState = getHandle.getValueState("testValueState", Encoders.scalaBoolean)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[String] = {
    val valueStateOption = _valueState.getOption()

    if (valueStateOption.isEmpty || !valueStateOption.get) {
      _listState.appendList(rows.toArray)
      _valueState.update(true)
      Seq().iterator
    } else {
      _valueState.clear()
      val storedValues = _listState.get()
      _listState.clear()

      new Iterator[String] {
        override def hasNext: Boolean = {
          rows.hasNext || storedValues.hasNext
        }

        override def next(): String = {
          if (rows.hasNext) {
            rows.next()
          } else {
            storedValues.next()
          }
        }
      }
    }
  }
}

class StatefulProcessorWithSingleListState extends
  StatefulProcessor[String, (String, String), String] with Logging {
  @transient private var _listState: ListState[String] = _
  @transient private var _tempState: ListState[Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _listState = getHandle.getListState("testListState", Encoders.STRING)
    _tempState = getHandle.getListState("tempState", Encoders.scalaInt)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[String] = {
    logWarning(s"TEST: processing key=$key")
    inputRows.foreach { inputRow =>
      logWarning(s"TEST: Adding value=${inputRow._2} for key=$key")
      _listState.appendValue(inputRow._2)
      _tempState.appendValue(inputRow._2.size)
    }

    _listState.get().foreach { item =>
      logWarning(s"TEST: current content of list=$item")
    }
    logWarning(s"TEST: done processing key=$key")

    Iterator.empty
  }
}

class TransformWithListStateSuite extends StreamTest
  with AlsoTestWithChangelogCheckpointingEnabled {
  import testImplicits._

  test("test appending null value in list state throw exception") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update()) (
        AddData(inputData, InputRow("k1", "tryAppendingNull", "")),
        ExpectFailure[SparkIllegalArgumentException](e => {
          assert(e.getMessage.contains("ILLEGAL_STATE_STORE_VALUE.NULL_VALUE"))
        })
      )
    }
  }

  test("test putting null value in list state throw exception") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputRow("k1", "tryPuttingNullInList", "")),
        ExpectFailure[SparkIllegalArgumentException](e => {
          assert(e.getMessage.contains("ILLEGAL_STATE_STORE_VALUE.NULL_VALUE"))
        })
      )
    }
  }

  test("test putting null list in list state throw exception") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputRow("k1", "tryPutNullList", "")),
        ExpectFailure[SparkIllegalArgumentException](e => {
          assert(e.getMessage.contains("ILLEGAL_STATE_STORE_VALUE.NULL_VALUE"))
        })
      )
    }
  }

  test("test appending null list in list state throw exception") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputRow("k1", "tryAppendingNullList", "")),
        ExpectFailure[SparkIllegalArgumentException](e => {
          assert(e.getMessage.contains("ILLEGAL_STATE_STORE_VALUE.NULL_VALUE"))
        })
      )
    }
  }

  test("test putting empty list in list state throw exception") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputRow("k1", "tryPutEmptyList", "")),
        ExpectFailure[SparkIllegalArgumentException](e => {
          assert(e.getMessage.contains("ILLEGAL_STATE_STORE_VALUE.EMPTY_LIST_VALUE"))
        })
      )
    }
  }

  test("test appending empty list in list state throw exception") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputRow("k1", "tryAppendingEmptyList", "")),
        ExpectFailure[SparkIllegalArgumentException](e => {
          assert(e.getMessage.contains("ILLEGAL_STATE_STORE_VALUE.EMPTY_LIST_VALUE"))
        })
      )
    }
  }

  test("test list state correctness") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update()) (
        // no interaction test
        AddData(inputData, InputRow("k1", "emit", "v1")),
        CheckNewAnswer(("k1", "v1")),
        // check simple append
        AddData(inputData, InputRow("k1", "append", "v2")),
        AddData(inputData, InputRow("k1", "emitAllInState", "")),
        CheckNewAnswer(("k1", "v2")),
        // multiple appends are correctly stored and emitted
        AddData(inputData, InputRow("k2", "append", "v1")),
        AddData(inputData, InputRow("k1", "append", "v4")),
        AddData(inputData, InputRow("k2", "append", "v2")),
        AddData(inputData, InputRow("k1", "emit", "v5")),
        AddData(inputData, InputRow("k2", "emit", "v3")),
        CheckNewAnswer(("k1", "v5"), ("k2", "v3")),
        AddData(inputData, InputRow("k1", "emitAllInState", "")),
        AddData(inputData, InputRow("k2", "emitAllInState", "")),
        CheckNewAnswer(("k2", "v1"), ("k2", "v2"), ("k1", "v4")),
        // check appendAll with append
        AddData(inputData, InputRow("k3", "appendAll", "v1,v2,v3")),
        AddData(inputData, InputRow("k3", "emit", "v4")),
        AddData(inputData, InputRow("k3", "append", "v5")),
        CheckNewAnswer(("k3", "v4")),
        AddData(inputData, InputRow("k3", "emitAllInState", "")),
        CheckNewAnswer(("k3", "v1"), ("k3", "v2"), ("k3", "v3"), ("k3", "v5")),
        // check removal cleans up all data in state
        AddData(inputData, InputRow("k4", "append", "v2")),
        AddData(inputData, InputRow("k4", "appendList", "v3,v4")),
        AddData(inputData, InputRow("k4", "remove", "")),
        AddData(inputData, InputRow("k4", "emitAllInState", "")),
        CheckNewAnswer(),
        // check put cleans up previous state and adds new state
        AddData(inputData, InputRow("k5", "appendAll", "v1,v2,v3")),
        AddData(inputData, InputRow("k5", "append", "v4")),
        AddData(inputData, InputRow("k5", "put", "v5,v6")),
        AddData(inputData, InputRow("k5", "emitAllInState", "")),
        CheckNewAnswer(("k5", "v5"), ("k5", "v6")),
        Execute { q =>
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numListStateVars") > 0)
        }
      )
    }
  }

  test("test ValueState And ListState in Processor") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new ToggleSaveAndEmitProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "k1"),
        AddData(inputData, "k2"),
        CheckNewAnswer(),
        AddData(inputData, "k1"),
        AddData(inputData, "k2"),
        CheckNewAnswer("k1", "k1", "k2", "k2")
      )
    }
  }

  test("test data source integration") {
    withTempDir { tempDir =>
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName) {

        val inputData = MemoryStream[(String, String)]
        val result = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new StatefulProcessorWithSingleListState(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, ("k1", "v1")),
          AddData(inputData, ("k1", "v400")),
//          AddData(inputData, ("k2", "v2")),
//          CheckNewAnswer(),
          AddData(inputData, ("k1", "v20")),
          AddData(inputData, ("k1", "v609024")),
          AddData(inputData, ("k1", "x")),
  //        CheckNewAnswer(),
  //        AddData(inputData, ("k3", "v6")),
   //        AddData(inputData, ("k3", "v7")),
          CheckNewAnswer(),
          StopStream
        )

        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "tempState")
          .load()

        val listStateDf = stateReaderDf
          .selectExpr(
      "key.value AS groupingKey",
            "value_list.value AS valueList",
            "partition_id")
          .select($"groupingKey",
            explode($"valueList"))

        checkAnswer(listStateDf,
          Seq(Row("k1", 1), Row("k1", 2), Row("k1", 3),
            Row("k1", 4), Row("k1", 7)))

        val stateReaderDf1 = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "testListState")
          .load()

        val listStrStateDf = stateReaderDf1
          .selectExpr(
      "key.value AS groupingKey",
            "value_list.value AS valueList",
            "partition_id")
          .select($"groupingKey",
            explode($"valueList"))

        checkAnswer(listStrStateDf,
          Seq(Row("k1", "v1"), Row("k1", "v20"), Row("k1", "v400"),
            Row("k1", "v609024"), Row("k1", "x")))
      }
    }
  }
}
