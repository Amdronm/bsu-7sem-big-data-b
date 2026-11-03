#!/usr/bin/env python3
"""
Apache Flink DataStream API application for real-time user event stream processing.
Features:
- Event-time processing with Watermarks (BoundedOutOfOrderness)
- Tumbling, Sliding, and Session windows
- Stateful processing: KeyedProcessFunction with ValueState for user event counts per minute
- End-to-end latency measurement and throughput metrics
"""

import argparse
import json
import os
import time

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import (
    EventTimeSessionWindows,
    SlidingEventTimeWindows,
    TumblingEventTimeWindows,
)


class EventTimestampAssigner(TimestampAssigner):
    """
    Extracts event timestamp in milliseconds from parsed JSON event.
    """

    def extract_timestamp(self, value, record_timestamp):
        # value is dict
        return value.get("event_epoch_ms", int(time.time() * 1000))


class MinuteUserStatefulCounter(KeyedProcessFunction):
    """
    Stateful KeyedProcessFunction:
    Maintains user event counts and total spend over 1-minute intervals.
    Registers event-time timers to emit results at the end of each 1-minute bucket.
    Computes end-to-end latency (System.currentTime - event.sent_epoch_ms).
    """

    def __init__(self):
        self.count_state = None
        self.amount_state = None
        self.timer_state = None

    def open(self, runtime_context: RuntimeContext):
        self.count_state = runtime_context.get_state(
            ValueStateDescriptor("count_state", Types.LONG())
        )
        self.amount_state = runtime_context.get_state(
            ValueStateDescriptor("amount_state", Types.DOUBLE())
        )
        self.timer_state = runtime_context.get_state(
            ValueStateDescriptor("timer_state", Types.LONG())
        )

    def process_element(self, value: dict, ctx: "KeyedProcessFunction.Context"):
        current_count = self.count_state.value() or 0
        current_amount = self.amount_state.value() or 0.0

        event_amount = float(value.get("amount", 0.0))
        event_epoch_ms = value.get("event_epoch_ms", int(time.time() * 1000))

        # Update state
        self.count_state.update(current_count + 1)
        self.amount_state.update(current_amount + event_amount)

        # Align timer to 1-minute event-time boundary
        minute_bucket_end = ((event_epoch_ms // 60000) + 1) * 60000
        existing_timer = self.timer_state.value()

        if existing_timer is None or existing_timer != minute_bucket_end:
            ctx.timer_service().register_event_time_timer(minute_bucket_end)
            self.timer_state.update(minute_bucket_end)

    def on_timer(self, timestamp: int, ctx: "KeyedProcessFunction.OnTimerContext"):
        user_id = ctx.get_current_key()
        total_count = self.count_state.value() or 0
        total_amount = self.amount_state.value() or 0.0

        # Processing time right now for latency estimation
        proc_time_ms = int(time.time() * 1000)
        e2e_latency_ms = max(0, proc_time_ms - timestamp)

        result = {
            "engine": "flink",
            "metric_type": "stateful_user_1m",
            "user_id": user_id,
            "window_end_epoch_ms": timestamp,
            "event_count": total_count,
            "total_amount": round(total_amount, 2),
            "approx_latency_ms": e2e_latency_ms,
            "processed_at_ms": proc_time_ms,
        }

        # Clear state for next minute bucket
        self.count_state.clear()
        self.amount_state.clear()
        self.timer_state.clear()

        yield json.dumps(result)


class WindowSummaryFunction(ProcessWindowFunction):
    """
    Emits window aggregate results with window bounds and latency.
    """

    def __init__(self, window_name: str):
        self.window_name = window_name

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: list,
    ):
        count = len(elements)
        total_amount = sum(float(e.get("amount", 0.0)) for e in elements)
        proc_time_ms = int(time.time() * 1000)

        # Calculate average end-to-end latency across elements
        sent_times = [
            e.get("sent_epoch_ms", proc_time_ms)
            for e in elements
            if "sent_epoch_ms" in e
        ]
        avg_latency = (
            sum(proc_time_ms - st for st in sent_times) / len(sent_times)
            if sent_times
            else 0.0
        )

        res = {
            "engine": "flink",
            "window_type": self.window_name,
            "group_key": key,
            "window_start_ms": context.window().start,
            "window_end_ms": context.window().end,
            "event_count": count,
            "total_amount": round(total_amount, 2),
            "avg_latency_ms": round(avg_latency, 2),
            "processed_at_ms": proc_time_ms,
        }
        yield json.dumps(res)


def main():
    parser = argparse.ArgumentParser(description="Flink DataStream API Lab 2")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--input-topic",
        default="user-events",
        help="Kafka input topic",
    )
    parser.add_argument(
        "--output-topic",
        default="flink-output",
        help="Kafka output topic",
    )
    parser.add_argument(
        "--mode",
        choices=["tumbling", "sliding", "session", "stateful", "all"],
        default="all",
        help="Processing mode",
    )
    parser.add_argument(
        "--watermark-delay-sec",
        type=int,
        default=20,
        help="Max out-of-orderness watermark delay in seconds",
    )
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Automatically add Kafka connector jar if available
    kafka_jar_path = os.environ.get(
        "FLINK_KAFKA_JAR",
        "/opt/flink/lib/flink-sql-connector-kafka.jar",
    )
    if os.path.exists(kafka_jar_path):
        env.add_jars(f"file://{kafka_jar_path}")

    print("=" * 60)
    print("Starting Apache Flink DataStream Processing")
    print(f"Bootstrap servers: {args.bootstrap_servers}")
    print(f"Input topic:       {args.input_topic}")
    print(f"Mode:              {args.mode}")
    print(f"Watermark delay:   {args.watermark_delay_sec}s")
    print("=" * 60)

    # 1. Setup Kafka Source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_topics(args.input_topic)
        .set_group_id("flink-stream-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # 2. Raw Stream & Deserialization
    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "KafkaSource",
    )

    def parse_json(raw_str: str):
        try:
            return json.loads(raw_str)
        except (json.JSONDecodeError, TypeError):
            return None

    parsed_stream = raw_stream.map(
        parse_json, output_type=Types.PICKLED_BYTE_ARRAY()
    ).filter(lambda x: x is not None and "user_id" in x)

    # 3. Watermark Assignment (Event Time)
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(args.watermark_delay_sec)
    ).with_timestamp_assigner(EventTimestampAssigner())

    event_time_stream = parsed_stream.assign_timestamps_and_watermarks(
        watermark_strategy
    )

    # 4a. Tumbling Window (30 seconds)
    if args.mode in ("tumbling", "all"):
        tumbling_stream = (
            event_time_stream.key_by(lambda x: x.get("event_type", "unknown"))
            .window(TumblingEventTimeWindows.of(Time.seconds(30)))
            .process(WindowSummaryFunction("tumbling_30s"))
        )
        tumbling_stream.print()

    # 4b. Sliding Window (60 seconds length, 15 seconds slide)
    if args.mode in ("sliding", "all"):
        sliding_stream = (
            event_time_stream.key_by(lambda x: x.get("event_type", "unknown"))
            .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(15)))
            .process(WindowSummaryFunction("sliding_60s_15s"))
        )
        sliding_stream.print()

    # 4c. Session Window (Session inactivity gap: 30 seconds per user)
    if args.mode in ("session", "all"):
        session_stream = (
            event_time_stream.key_by(lambda x: x.get("user_id", "unknown"))
            .window(EventTimeSessionWindows.with_gap(Time.seconds(30)))
            .process(WindowSummaryFunction("session_gap_30s"))
        )
        session_stream.print()

    # 4d. Stateful Stream Processing (KeyedProcessFunction with ValueState for 1-minute key counts)
    if args.mode in ("stateful", "all"):
        stateful_stream = event_time_stream.key_by(
            lambda x: x.get("user_id", "unknown")
        ).process(MinuteUserStatefulCounter())
        stateful_stream.print()

    print("Submitting Flink Job to Execution Environment...")
    env.execute("Flink-UserEvents-StreamProcessing")


if __name__ == "__main__":
    main()
