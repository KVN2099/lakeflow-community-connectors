# Redis Community Connector

This connector enables data ingestion from Redis databases into Databricks using Lakeflow Connect.

## Overview

The Redis connector extracts data from all Redis data types and exposes them as structured tables suitable for analytics and data processing.

## Supported Tables

| Table | Description | Primary Keys | Ingestion Type |
|-------|-------------|--------------|----------------|
| `keys` | Metadata about all keys (name, type, TTL, memory usage) | `key_name` | Snapshot |
| `strings` | String key-value pairs | `key_name` | Snapshot |
| `hashes` | Hash fields (each field becomes a row) | `key_name`, `field` | Snapshot |
| `lists` | List elements (each element becomes a row with its index) | `key_name`, `list_index` | Snapshot |
| `sets` | Set members (each member becomes a row) | `key_name`, `member` | Snapshot |
| `sorted_sets` | Sorted set members with scores and ranks | `key_name`, `member` | Snapshot |
| `streams` | Stream entries for event processing | `key_name`, `entry_id` | Append |
| `server_info` | Redis server information and statistics | `section`, `property` | Snapshot |

## Connection Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `host` | string | Yes | - | Redis server hostname or IP address |
| `port` | string | No | `6379` | Redis server port number |
| `password` | string | No | - | Password for Redis AUTH (if required) |
| `db` | string | No | `0` | Database number (0-15) |
| `ssl` | string | No | `false` | Enable SSL/TLS encryption |

## Table Options

These options can be specified per-table to customize data extraction:

| Option | Default | Description |
|--------|---------|-------------|
| `key_pattern` | `*` | Glob pattern to filter keys (e.g., `user:*`, `cache:*`) |
| `batch_size` | `1000` | Number of keys to scan per batch |

## Schema Details

### keys

Provides metadata about all keys in the database:

```
key_name: STRING (PK)     - The key name
key_type: STRING          - Type (string, hash, list, set, zset, stream)
ttl_seconds: LONG         - Time-to-live in seconds (null if no expiry)
memory_bytes: LONG        - Memory usage in bytes (Redis 4.0+)
encoding: STRING          - Internal encoding
refcount: LONG            - Reference count
idle_time_seconds: LONG   - Idle time since last access
scanned_at: LONG          - Unix timestamp when scanned
```

### strings

String key-value pairs:

```
key_name: STRING (PK)     - The key name
value: STRING             - The string value
value_length: LONG        - Length of the value
ttl_seconds: LONG         - Time-to-live in seconds
scanned_at: LONG          - Unix timestamp when scanned
```

### hashes

Hash data with each field as a separate row:

```
key_name: STRING (PK)     - The hash key name
field: STRING (PK)        - The field name within the hash
value: STRING             - The field value
ttl_seconds: LONG         - TTL of the hash key
scanned_at: LONG          - Unix timestamp when scanned
```

### lists

List elements with positional index:

```
key_name: STRING (PK)     - The list key name
list_index: LONG (PK)     - Zero-based index in the list
value: STRING             - The element value
list_length: LONG         - Total length of the list
ttl_seconds: LONG         - TTL of the list key
scanned_at: LONG          - Unix timestamp when scanned
```

### sets

Set members:

```
key_name: STRING (PK)     - The set key name
member: STRING (PK)       - The set member
set_cardinality: LONG     - Total number of members in the set
ttl_seconds: LONG         - TTL of the set key
scanned_at: LONG          - Unix timestamp when scanned
```

### sorted_sets

Sorted set members with scores:

```
key_name: STRING (PK)     - The sorted set key name
member: STRING (PK)       - The member value
score: DOUBLE             - The member's score
rank: LONG                - Zero-based rank (by score ascending)
zset_cardinality: LONG    - Total members in the sorted set
ttl_seconds: LONG         - TTL of the sorted set key
scanned_at: LONG          - Unix timestamp when scanned
```

### streams

Stream entries for event processing (supports incremental append):

```
key_name: STRING (PK)     - The stream key name
entry_id: STRING (PK)     - Stream entry ID (timestamp-sequence)
fields: STRING            - JSON-encoded field-value pairs
timestamp_ms: LONG        - Timestamp portion of entry ID
sequence: LONG            - Sequence portion of entry ID
ttl_seconds: LONG         - TTL of the stream key
```

### server_info

Redis server information and statistics:

```
section: STRING (PK)      - Info section (server, clients, memory, etc.)
property: STRING (PK)     - Property name
value: STRING             - Property value
collected_at: LONG        - Unix timestamp when collected
```

## Usage Example

### Basic Setup

```python
from sources.redis.redis import LakeflowConnect

# Initialize connector
connector = LakeflowConnect({
    "host": "redis.example.com",
    "port": "6379",
    "password": "your-password",
    "db": "0",
    "ssl": "true"
})

# Test connection
result = connector.test_connection()
print(result)

# List available tables
tables = connector.list_tables()
print(tables)

# Read all string keys
records, offset = connector.read_table("strings", None, {})
```

### Using Key Patterns

```python
# Read only user-related hashes
records, offset = connector.read_table(
    "hashes",
    None,
    {"key_pattern": "user:*"}
)

# Read only cache keys
records, offset = connector.read_table(
    "strings",
    None,
    {"key_pattern": "cache:*"}
)
```

### Incremental Stream Reading

```python
# Initial read - get all stream entries
records, offset = connector.read_table("streams", None, {})
print(f"Read {len(records)} entries, offset: {offset}")

# Subsequent reads - only get new entries
new_records, new_offset = connector.read_table("streams", offset, {})
print(f"Read {len(new_records)} new entries")
```

## Requirements

- Redis server 4.0+ (for memory usage statistics)
- Python package: `redis>=4.0.0`

## Security Considerations

1. **Authentication**: Always use password authentication in production
2. **SSL/TLS**: Enable SSL for connections over public networks
3. **Network**: Use Redis in a private network or VPC when possible
4. **Access Control**: Consider using Redis ACLs (Redis 6.0+) for fine-grained access

## Limitations

- The connector uses `SCAN` for key iteration, which is safe for production but may not capture keys created/deleted during scanning
- Large lists, sets, or sorted sets are fully loaded into memory
- `DEBUG OBJECT` command may be restricted on some Redis deployments (hosted services)
- Stream reading is incremental but other data types use snapshot mode

## Troubleshooting

### Connection Errors

```
Connection refused
```
- Verify Redis server is running and accessible
- Check host and port configuration
- Ensure firewall allows the connection

### Authentication Errors

```
Authentication failed
```
- Verify the password is correct
- Check if Redis requires authentication (`requirepass` config)

### Permission Errors

```
Operation not permitted
```
- Some commands (like `DEBUG OBJECT`) may be disabled
- Check Redis ACLs if using Redis 6.0+

