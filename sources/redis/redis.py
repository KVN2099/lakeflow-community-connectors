import redis
import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    DoubleType,
    ArrayType,
)
from datetime import datetime
import time
from typing import Dict, List, Tuple, Iterator, Any


class LakeflowConnect:
    def __init__(self, options: dict) -> None:
        """
        Initialize the Redis connector with connection parameters.

        Args:
            options: Dictionary containing:
                - host: Redis server hostname (default: localhost)
                - port: Redis server port (default: 6379)
                - password: Optional password for authentication
                - db: Database number 0-15 (default: 0)
                - ssl: Whether to use SSL (default: false)
                - key_pattern: Optional glob pattern to filter keys (default: *)
                - batch_size: Number of keys to scan per batch (default: 1000)
        """
        self.host = options.get("host", "localhost")
        self.port = int(options.get("port", 6379))
        self.password = options.get("password", None)
        self.db = int(options.get("db", 0))
        self.ssl = options.get("ssl", "false").lower() == "true"
        self.key_pattern = options.get("key_pattern", "*")
        self.batch_size = int(options.get("batch_size", 1000))

        # Initialize Redis client
        self._client = redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password,
            db=self.db,
            ssl=self.ssl,
            decode_responses=True,
        )

        # Cache for schemas
        self._schema_cache = {}

        # Centralized object metadata configuration
        self._object_config = {
            "keys": {
                "primary_keys": ["key_name"],
                "cursor_field": "scanned_at",
                "ingestion_type": "snapshot",
            },
            "strings": {
                "primary_keys": ["key_name"],
                "cursor_field": "scanned_at",
                "ingestion_type": "snapshot",
            },
            "hashes": {
                "primary_keys": ["key_name", "field"],
                "cursor_field": "scanned_at",
                "ingestion_type": "snapshot",
            },
            "lists": {
                "primary_keys": ["key_name", "list_index"],
                "cursor_field": "scanned_at",
                "ingestion_type": "snapshot",
            },
            "sets": {
                "primary_keys": ["key_name", "member"],
                "cursor_field": "scanned_at",
                "ingestion_type": "snapshot",
            },
            "sorted_sets": {
                "primary_keys": ["key_name", "member"],
                "cursor_field": "scanned_at",
                "ingestion_type": "snapshot",
            },
            "streams": {
                "primary_keys": ["key_name", "entry_id"],
                "cursor_field": "entry_id",
                "ingestion_type": "append",
            },
            "server_info": {
                "primary_keys": ["section", "property"],
                "cursor_field": "collected_at",
                "ingestion_type": "snapshot",
            },
        }

        # Centralized schema configuration
        self._schema_config = {
            "keys": StructType(
                [
                    StructField("key_name", StringType(), False),
                    StructField("key_type", StringType(), True),
                    StructField("ttl_seconds", LongType(), True),
                    StructField("memory_bytes", LongType(), True),
                    StructField("encoding", StringType(), True),
                    StructField("refcount", LongType(), True),
                    StructField("idle_time_seconds", LongType(), True),
                    StructField("scanned_at", LongType(), True),
                ]
            ),
            "strings": StructType(
                [
                    StructField("key_name", StringType(), False),
                    StructField("value", StringType(), True),
                    StructField("value_length", LongType(), True),
                    StructField("ttl_seconds", LongType(), True),
                    StructField("scanned_at", LongType(), True),
                ]
            ),
            "hashes": StructType(
                [
                    StructField("key_name", StringType(), False),
                    StructField("field", StringType(), False),
                    StructField("value", StringType(), True),
                    StructField("ttl_seconds", LongType(), True),
                    StructField("scanned_at", LongType(), True),
                ]
            ),
            "lists": StructType(
                [
                    StructField("key_name", StringType(), False),
                    StructField("list_index", LongType(), False),
                    StructField("value", StringType(), True),
                    StructField("list_length", LongType(), True),
                    StructField("ttl_seconds", LongType(), True),
                    StructField("scanned_at", LongType(), True),
                ]
            ),
            "sets": StructType(
                [
                    StructField("key_name", StringType(), False),
                    StructField("member", StringType(), False),
                    StructField("set_cardinality", LongType(), True),
                    StructField("ttl_seconds", LongType(), True),
                    StructField("scanned_at", LongType(), True),
                ]
            ),
            "sorted_sets": StructType(
                [
                    StructField("key_name", StringType(), False),
                    StructField("member", StringType(), False),
                    StructField("score", DoubleType(), True),
                    StructField("rank", LongType(), True),
                    StructField("zset_cardinality", LongType(), True),
                    StructField("ttl_seconds", LongType(), True),
                    StructField("scanned_at", LongType(), True),
                ]
            ),
            "streams": StructType(
                [
                    StructField("key_name", StringType(), False),
                    StructField("entry_id", StringType(), False),
                    StructField("fields", StringType(), True),
                    StructField("timestamp_ms", LongType(), True),
                    StructField("sequence", LongType(), True),
                    StructField("ttl_seconds", LongType(), True),
                ]
            ),
            "server_info": StructType(
                [
                    StructField("section", StringType(), False),
                    StructField("property", StringType(), False),
                    StructField("value", StringType(), True),
                    StructField("collected_at", LongType(), True),
                ]
            ),
        }

    def list_tables(self) -> list[str]:
        """
        List available Redis tables/data types.

        Returns:
            List of supported table names
        """
        return [
            "keys",
            "strings",
            "hashes",
            "lists",
            "sets",
            "sorted_sets",
            "streams",
            "server_info",
        ]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get the Spark schema for a Redis table.

        Args:
            table_name: Name of the table
            table_options: Additional options (key_pattern can override default)

        Returns:
            StructType representing the table schema
        """
        if table_name not in self._schema_config:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.list_tables()}"
            )
        return self._schema_config[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Get metadata for a Redis table.

        Args:
            table_name: Name of the table
            table_options: Additional options

        Returns:
            Dictionary with primary_keys, cursor_field, and ingestion_type
        """
        if table_name not in self._object_config:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.list_tables()}"
            )
        config = self._object_config[table_name]
        return {
            "primary_keys": config["primary_keys"],
            "cursor_field": config["cursor_field"],
            "ingestion_type": config["ingestion_type"],
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[List[Dict], Dict]:
        """
        Read data from a Redis table.

        Args:
            table_name: Name of the table to read
            start_offset: Dictionary containing cursor information for incremental reads
            table_options: Additional options (key_pattern can filter keys)

        Returns:
            Tuple of (records, new_offset)
        """
        if table_name not in self._object_config:
            raise ValueError(f"Unsupported table: {table_name}")

        # Get key pattern from table_options or use default
        key_pattern = table_options.get("key_pattern", self.key_pattern)

        # Route to appropriate reader
        reader_map = {
            "keys": self._read_keys,
            "strings": self._read_strings,
            "hashes": self._read_hashes,
            "lists": self._read_lists,
            "sets": self._read_sets,
            "sorted_sets": self._read_sorted_sets,
            "streams": self._read_streams,
            "server_info": self._read_server_info,
        }

        reader = reader_map.get(table_name)
        if reader:
            return reader(key_pattern, start_offset)
        else:
            raise ValueError(f"No reader implemented for table: {table_name}")

    def _scan_keys(self, pattern: str, key_type: str = None) -> Iterator[str]:
        """
        Scan Redis keys matching pattern and optionally filter by type.

        Args:
            pattern: Glob pattern for key matching
            key_type: Optional type filter (string, hash, list, set, zset, stream)

        Yields:
            Key names matching the pattern and type
        """
        cursor = 0
        while True:
            cursor, keys = self._client.scan(
                cursor=cursor, match=pattern, count=self.batch_size
            )
            for key in keys:
                if key_type is None:
                    yield key
                else:
                    actual_type = self._client.type(key)
                    if actual_type == key_type:
                        yield key
            if cursor == 0:
                break

    def _get_ttl(self, key: str) -> int:
        """Get TTL for a key, returns -1 if no expiry, -2 if key doesn't exist."""
        return self._client.ttl(key)

    def _read_keys(
        self, key_pattern: str, start_offset: dict
    ) -> Tuple[List[Dict], Dict]:
        """
        Read metadata about all keys matching the pattern.

        Returns key name, type, TTL, memory usage, and other metadata.
        """
        records = []
        current_time = int(time.time())

        for key in self._scan_keys(key_pattern):
            try:
                key_type = self._client.type(key)
                ttl = self._get_ttl(key)

                # Get debug object info (may not be available on all Redis versions)
                try:
                    debug_info = self._client.debug_object(key)
                    encoding = debug_info.get("encoding", None)
                    refcount = debug_info.get("refcount", None)
                    idle_time = debug_info.get("idletime", None)
                except Exception:
                    encoding = None
                    refcount = None
                    idle_time = None

                # Get memory usage (Redis 4.0+)
                try:
                    memory = self._client.memory_usage(key)
                except Exception:
                    memory = None

                record = {
                    "key_name": key,
                    "key_type": key_type,
                    "ttl_seconds": ttl if ttl >= 0 else None,
                    "memory_bytes": memory,
                    "encoding": encoding,
                    "refcount": refcount,
                    "idle_time_seconds": idle_time,
                    "scanned_at": current_time,
                }
                records.append(record)
            except Exception as e:
                # Skip keys that are deleted during scan
                continue

        return records, {"scanned_at": current_time}

    def _read_strings(
        self, key_pattern: str, start_offset: dict
    ) -> Tuple[List[Dict], Dict]:
        """
        Read all string key-value pairs matching the pattern.
        """
        records = []
        current_time = int(time.time())

        for key in self._scan_keys(key_pattern, key_type="string"):
            try:
                value = self._client.get(key)
                ttl = self._get_ttl(key)

                record = {
                    "key_name": key,
                    "value": value,
                    "value_length": len(value) if value else 0,
                    "ttl_seconds": ttl if ttl >= 0 else None,
                    "scanned_at": current_time,
                }
                records.append(record)
            except Exception:
                continue

        return records, {"scanned_at": current_time}

    def _read_hashes(
        self, key_pattern: str, start_offset: dict
    ) -> Tuple[List[Dict], Dict]:
        """
        Read all hash fields from keys matching the pattern.

        Each hash field becomes a separate record.
        """
        records = []
        current_time = int(time.time())

        for key in self._scan_keys(key_pattern, key_type="hash"):
            try:
                hash_data = self._client.hgetall(key)
                ttl = self._get_ttl(key)

                for field, value in hash_data.items():
                    record = {
                        "key_name": key,
                        "field": field,
                        "value": value,
                        "ttl_seconds": ttl if ttl >= 0 else None,
                        "scanned_at": current_time,
                    }
                    records.append(record)
            except Exception:
                continue

        return records, {"scanned_at": current_time}

    def _read_lists(
        self, key_pattern: str, start_offset: dict
    ) -> Tuple[List[Dict], Dict]:
        """
        Read all list elements from keys matching the pattern.

        Each list element becomes a separate record with its index.
        """
        records = []
        current_time = int(time.time())

        for key in self._scan_keys(key_pattern, key_type="list"):
            try:
                list_length = self._client.llen(key)
                list_data = self._client.lrange(key, 0, -1)
                ttl = self._get_ttl(key)

                for idx, value in enumerate(list_data):
                    record = {
                        "key_name": key,
                        "list_index": idx,
                        "value": value,
                        "list_length": list_length,
                        "ttl_seconds": ttl if ttl >= 0 else None,
                        "scanned_at": current_time,
                    }
                    records.append(record)
            except Exception:
                continue

        return records, {"scanned_at": current_time}

    def _read_sets(
        self, key_pattern: str, start_offset: dict
    ) -> Tuple[List[Dict], Dict]:
        """
        Read all set members from keys matching the pattern.

        Each set member becomes a separate record.
        """
        records = []
        current_time = int(time.time())

        for key in self._scan_keys(key_pattern, key_type="set"):
            try:
                members = self._client.smembers(key)
                cardinality = len(members)
                ttl = self._get_ttl(key)

                for member in members:
                    record = {
                        "key_name": key,
                        "member": member,
                        "set_cardinality": cardinality,
                        "ttl_seconds": ttl if ttl >= 0 else None,
                        "scanned_at": current_time,
                    }
                    records.append(record)
            except Exception:
                continue

        return records, {"scanned_at": current_time}

    def _read_sorted_sets(
        self, key_pattern: str, start_offset: dict
    ) -> Tuple[List[Dict], Dict]:
        """
        Read all sorted set members from keys matching the pattern.

        Each member becomes a separate record with its score and rank.
        """
        records = []
        current_time = int(time.time())

        for key in self._scan_keys(key_pattern, key_type="zset"):
            try:
                # Get all members with scores
                members_with_scores = self._client.zrange(
                    key, 0, -1, withscores=True
                )
                cardinality = len(members_with_scores)
                ttl = self._get_ttl(key)

                for rank, (member, score) in enumerate(members_with_scores):
                    record = {
                        "key_name": key,
                        "member": member,
                        "score": score,
                        "rank": rank,
                        "zset_cardinality": cardinality,
                        "ttl_seconds": ttl if ttl >= 0 else None,
                        "scanned_at": current_time,
                    }
                    records.append(record)
            except Exception:
                continue

        return records, {"scanned_at": current_time}

    def _read_streams(
        self, key_pattern: str, start_offset: dict
    ) -> Tuple[List[Dict], Dict]:
        """
        Read stream entries from keys matching the pattern.

        Supports incremental reading using stream entry IDs.
        For full refresh, reads all entries from the beginning.
        For incremental, reads entries after the last processed ID.
        """
        records = []
        latest_entry_id = start_offset.get("entry_id", "0-0") if start_offset else "0-0"
        new_latest_id = latest_entry_id

        for key in self._scan_keys(key_pattern, key_type="stream"):
            try:
                ttl = self._get_ttl(key)

                # Read entries after the start offset
                # Use XREAD with > syntax for incremental
                entries = self._client.xrange(key, min=f"({latest_entry_id}", max="+")

                for entry_id, fields in entries:
                    # Parse timestamp and sequence from entry ID
                    parts = entry_id.split("-")
                    timestamp_ms = int(parts[0]) if len(parts) > 0 else 0
                    sequence = int(parts[1]) if len(parts) > 1 else 0

                    record = {
                        "key_name": key,
                        "entry_id": entry_id,
                        "fields": json.dumps(fields),
                        "timestamp_ms": timestamp_ms,
                        "sequence": sequence,
                        "ttl_seconds": ttl if ttl >= 0 else None,
                    }
                    records.append(record)

                    # Track the latest entry ID for offset
                    if entry_id > new_latest_id:
                        new_latest_id = entry_id
            except Exception:
                continue

        return records, {"entry_id": new_latest_id}

    def _read_server_info(
        self, key_pattern: str, start_offset: dict
    ) -> Tuple[List[Dict], Dict]:
        """
        Read Redis server information and statistics.

        Returns server info as key-value pairs organized by section.
        """
        records = []
        current_time = int(time.time())

        try:
            info = self._client.info()

            for key, value in info.items():
                # Determine section based on key patterns
                section = self._categorize_info_key(key)

                record = {
                    "section": section,
                    "property": key,
                    "value": str(value),
                    "collected_at": current_time,
                }
                records.append(record)
        except Exception as e:
            raise Exception(f"Failed to read server info: {str(e)}")

        return records, {"collected_at": current_time}

    def _categorize_info_key(self, key: str) -> str:
        """Categorize an INFO key into its section."""
        section_prefixes = {
            "redis_": "server",
            "os": "server",
            "arch": "server",
            "process": "server",
            "tcp_": "server",
            "uptime": "server",
            "hz": "server",
            "lru": "server",
            "executable": "server",
            "config_file": "server",
            "connected_clients": "clients",
            "client_": "clients",
            "blocked_clients": "clients",
            "tracking_clients": "clients",
            "used_memory": "memory",
            "mem_": "memory",
            "total_system_memory": "memory",
            "maxmemory": "memory",
            "allocator": "memory",
            "lazyfree": "memory",
            "active_defrag": "memory",
            "loading": "persistence",
            "rdb_": "persistence",
            "aof_": "persistence",
            "current_": "persistence",
            "total_connections": "stats",
            "total_commands": "stats",
            "instantaneous": "stats",
            "rejected_connections": "stats",
            "sync_": "stats",
            "expired_": "stats",
            "evicted_": "stats",
            "keyspace_": "stats",
            "pubsub_": "stats",
            "latest_fork_usec": "stats",
            "migrate_cached_sockets": "stats",
            "slave_": "replication",
            "master_": "replication",
            "connected_slaves": "replication",
            "repl_": "replication",
            "second_repl_offset": "replication",
            "used_cpu": "cpu",
            "cluster_": "cluster",
            "db": "keyspace",
        }

        for prefix, section in section_prefixes.items():
            if key.startswith(prefix):
                return section

        return "other"

    def test_connection(self) -> dict:
        """
        Test the connection to Redis server.

        Returns:
            Dictionary with status and message
        """
        try:
            # Try to ping the server
            response = self._client.ping()

            if response:
                # Get basic server info
                info = self._client.info("server")
                redis_version = info.get("redis_version", "unknown")

                return {
                    "status": "success",
                    "message": f"Connected to Redis server v{redis_version}",
                }
            else:
                return {
                    "status": "error",
                    "message": "Server did not respond to PING",
                }
        except redis.ConnectionError as e:
            return {
                "status": "error",
                "message": f"Connection failed: {str(e)}",
            }
        except redis.AuthenticationError as e:
            return {
                "status": "error",
                "message": f"Authentication failed: {str(e)}",
            }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}

