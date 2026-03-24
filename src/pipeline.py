"""
Generic config-driven CDC pipeline: MySQL → PyFlink → ClickHouse

Join topology is defined in config/pipelines.json — no code changes needed
to add new tables or new destination tables.

How it works:
  1. Every MySQL table in every pipeline config gets a Kafka source.
  2. All events are tagged with their table name and pipeline name, then unioned.
  3. A single KeyedProcessFunction handles every event:
       - Updates state for whichever table changed.
       - Re-emits the fully joined row to ClickHouse for every affected destination.
  4. If a fact row changes  → re-join using latest dimension state, emit.
     If a dimension row changes → find all fact rows referencing it, re-emit each.
  5. ClickHouse ReplacingMergeTree deduplicates by _version.

To add a new join (e.g. table_e ⋈ table_f → dest_db.table_g):
  - Add a new entry in config/pipelines.json — that's it.
"""

import base64
import json
import logging
import os
import urllib.parse
import urllib.request
from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP",    "kafka:29092")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "cdc.source_db")
CLICKHOUSE_URL     = os.getenv("CLICKHOUSE_URL",     "http://clickhouse:8123")
CONFIG_PATH        = os.getenv("CONFIG_PATH",        "/opt/flink/jobs/pipelines.json")
PARALLELISM        = int(os.getenv("PARALLELISM",    "1"))


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


_DECIMAL_COLUMNS = frozenset({
    "amount", "price", "total", "cost", "value", "discount_pct",
})


def decode_decimal(value) -> str:
    """Debezium encodes MySQL DECIMAL as base64 bytes. Decode to a plain string."""
    if not isinstance(value, str) or not value:
        return str(value) if value is not None else "0"
    try:
        decoded = base64.b64decode(value + "==")
        return str(int.from_bytes(decoded, "big") / 100)
    except Exception:
        return value


def kafka_source(topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("fetch.min.bytes", "1")
        .set_property("fetch.max.wait.ms", "50")
        .build()
    )


def clickhouse_insert(database: str, table: str, rows: list[dict]):
    """Bulk-insert rows into ClickHouse via HTTP JSONEachRow."""
    if not rows:
        return
    cols = list(rows[0].keys())
    sql = f"INSERT INTO {database}.{table} ({','.join(cols)}) FORMAT JSONEachRow"
    body = "\n".join(json.dumps(r) for r in rows).encode()
    url = f"{CLICKHOUSE_URL}/?query={urllib.parse.quote(sql)}"
    req = urllib.request.Request(url, data=body, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status not in (200, 204):
                log.error("ClickHouse insert failed [%s.%s]: %s", database, table, resp.read())
    except Exception as exc:
        log.error("ClickHouse insert error [%s.%s]: %s", database, table, exc)


# ── Core: Generic stateful join processor ────────────────────────────────────

class GenericJoinProcessor(KeyedProcessFunction):
    """
    Handles all CDC events for a single pipeline (one destination table).

    State (all keyed to partition 0 for small dimension tables):
      fact_state:   MapState[fact_pk  -> row JSON]
      dim_state_<table>: MapState[dim_pk -> row JSON]   one per dimension table

    On fact event:
      - Update fact_state
      - Look up each dimension, emit joined row

    On dimension event:
      - Update dim_state_<table>
      - Scan fact_state, re-emit every fact row that joins on this dimension row
    """

    def __init__(self, pipeline_cfg: dict):
        self.cfg = pipeline_cfg
        self.fact_table = pipeline_cfg["fact"]["mysql_table"]
        self.dimensions = pipeline_cfg["dimensions"]  # list of dim configs
        self.dest_db    = pipeline_cfg["destination"]["database"]
        self.dest_table = pipeline_cfg["destination"]["table"]
        # dim table name → dim config
        self.dim_by_table = {d["mysql_table"]: d for d in self.dimensions}

    def open(self, ctx: RuntimeContext):
        self.fact_state = ctx.get_map_state(
            MapStateDescriptor("fact_state", Types.INT(), Types.STRING())
        )
        self.dim_states = {}
        for dim in self.dimensions:
            key = dim["mysql_table"]
            self.dim_states[key] = ctx.get_map_state(
                MapStateDescriptor(f"dim_{key}", Types.INT(), Types.STRING())
            )

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        try:
            msg = json.loads(value)
            table = msg["_table"]
            row   = msg["_row"]
        except (json.JSONDecodeError, KeyError):
            log.warning("Malformed message: %s", value)
            return

        if table == self.fact_table:
            yield from self._handle_fact(row)
        elif table in self.dim_by_table:
            yield from self._handle_dim(table, row)

    def _handle_fact(self, row: dict):
        """Fact table changed → update state, emit one joined row."""
        pk = row.get("id")
        if pk is None:
            return
        self.fact_state.put(pk, json.dumps(row))
        joined = self._join_fact(row)
        if joined:
            clickhouse_insert(self.dest_db, self.dest_table, [joined])
            yield json.dumps(joined)  # pass-through for observability

    def _handle_dim(self, table: str, row: dict):
        """Dimension changed → update state, re-emit every fact row that references it."""
        dim_cfg = self.dim_by_table[table]
        pk = row.get(dim_cfg["pk"])
        if pk is None:
            return
        self.dim_states[table].put(pk, json.dumps(row))

        # Scan all fact rows that join on this dimension key
        fk_col = dim_cfg["fk_in_fact"]
        to_insert = []
        for _fact_pk, fact_json in self.fact_state.items():
            fact = json.loads(fact_json)
            if fact.get(fk_col) != pk:
                continue
            joined = self._join_fact(fact)
            if joined:
                to_insert.append(joined)
                yield json.dumps(joined)

        if to_insert:
            clickhouse_insert(self.dest_db, self.dest_table, to_insert)

    def _join_fact(self, fact: dict) -> dict | None:
        """Build the full denormalized row by joining fact with all dimensions."""
        result = {}

        # Fact columns
        for src_col, dst_col in self.cfg["fact"]["columns"].items():
            val = fact.get(src_col)
            if isinstance(val, str) and src_col in _DECIMAL_COLUMNS:
                val = decode_decimal(val)
            result[dst_col] = val

        # Dimension columns
        for dim in self.dimensions:
            fk_val = fact.get(dim["fk_in_fact"])
            nullable = dim.get("nullable", False)

            if nullable and fk_val is None:
                # LEFT JOIN with NULL FK — keep fact-set values, default others to None
                for src_col, dst_col in dim["columns"].items():
                    result.setdefault(dst_col, None)
            else:
                dim_json = self.dim_states[dim["mysql_table"]].get(fk_val) if fk_val is not None else None
                dim_row = json.loads(dim_json) if dim_json else {}
                for src_col, dst_col in dim["columns"].items():
                    val = dim_row.get(src_col)
                    if src_col == "metadata" and isinstance(val, dict):
                        val = json.dumps(val)
                    elif isinstance(val, str) and src_col in _DECIMAL_COLUMNS:
                        val = decode_decimal(val)
                    result[dst_col] = val if val is not None else ""

        # CDC metadata
        result["_op"]      = fact.get("__op", fact.get("_op", "u"))
        result["_version"] = int(fact.get("__source_ts_ms", fact.get("__ts_ms", 0)))
        return result


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    cfg = load_config(CONFIG_PATH)
    pipelines = cfg["pipelines"]

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(PARALLELISM)
    env.set_buffer_timeout(50)
    env.enable_checkpointing(10_000)

    wm = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_millis(500))
        .with_idleness(Duration.of_seconds(30))
    )

    # Collect every unique MySQL table needed across all pipelines
    all_tables: set[str] = set()
    for p in pipelines:
        all_tables.add(p["fact"]["mysql_table"])
        for dim in p["dimensions"]:
            all_tables.add(dim["mysql_table"])

    # Build one Kafka source per table, tag each message with table + pipeline names
    table_streams = {}
    for table in all_tables:
        topic = f"{KAFKA_TOPIC_PREFIX}.{table}"
        raw = env.from_source(kafka_source(topic, f"flink-cdc-{table}"), wm, f"kafka-{table}")
        # Tag: {"_table": "table_a", "_row": {...original debezium row...}}
        table_streams[table] = raw.map(
            lambda s, t=table: json.dumps({"_table": t, "_row": json.loads(s)}),
            output_type=Types.STRING()
        )

    # For each pipeline: union only its relevant table streams, run the join processor
    for p in pipelines:
        tables_in_pipeline = [p["fact"]["mysql_table"]] + [d["mysql_table"] for d in p["dimensions"]]
        streams = [table_streams[t] for t in tables_in_pipeline]

        # Union all streams, key to single partition (dimensions are small)
        combined = streams[0]
        for s in streams[1:]:
            combined = combined.union(s)

        (
            combined
            .key_by(lambda _: 0)
            .process(GenericJoinProcessor(p), output_type=Types.STRING())
        )
        log.info("Pipeline '%s' wired: %s → %s.%s",
                 p["name"], tables_in_pipeline,
                 p["destination"]["database"], p["destination"]["table"])

    log.info("Submitting job with %d pipeline(s)", len(pipelines))
    env.execute("generic-cdc-clickhouse")


if __name__ == "__main__":
    main()
