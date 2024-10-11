# CassandraCRUD Library

CassandraCRUD is a Python library that provides a simple and flexible interface for interacting with Apache Cassandra databases. It wraps the DataStax Cassandra driver and offers additional utility functions for common database operations.

## Features

- Easy connection management
- Flexible query execution (synchronous and asynchronous)
- Batch operations support
- Customizable consistency levels
- Built-in CRUD operations
- Table management utilities
- Configurable through environment variables or direct parameters

## Installation

To install CassandraCRUD, you can use pip:

```
pip install cassandracrud
```

## Usage

Here's a basic example of how to use CassandraCRUD:

```python
from cassandracrud import CassandraCRUD

# Initialize the CassandraCRUD instance
crud = CassandraCRUD(
    contact_points=["localhost"],
    keyspace="my_keyspace",
    username="user",
    password="pass"
)

# Connect to the database
crud.connect()

# Execute a query
result = crud.execute("SELECT * FROM my_table")

# Create a new record
crud.create("my_table", {"id": 1, "name": "John Doe"})

# Close the connection
crud.close()
```

## Configuration

You can configure CassandraCRUD either by passing parameters to the constructor or by setting environment variables:

- `CASSANDRA_PROD_CONTACT_POINTS`: Comma-separated list of contact points
- `CASSANDRA_PROD_KEYSPACE`: Keyspace name
- `CASSANDRA_PROD_USERNAME`: Username for authentication
- `CASSANDRA_PROD_PASSWORD`: Password for authentication

If both environment variables and constructor parameters are provided, the constructor parameters take precedence.

## Main Methods

- `connect()`: Establishes a connection to the Cassandra cluster
- `execute(query, params=None)`: Executes a CQL query
- `execute_async(query, params=None)`: Executes a CQL query asynchronously
- `prepare(query)`: Prepares a CQL statement
- `execute_batch(statements)`: Executes a batch of CQL statements
- `create(table, data)`: Inserts a new record into a table
- `read(table, conditions=None)`: Retrieves records from a table
- `update(table, data, conditions)`: Updates records in a table
- `delete(table, conditions)`: Deletes records from a table

## Utility Methods

- `table_exists(table_name)`: Checks if a table exists
- `create_table(table_name, column_definitions)`: Creates a new table
- `drop_table(table_name)`: Drops a table
- `get_table_schema(table_name)`: Retrieves the schema of a table
- `get_metrics()`: Retrieves cluster metrics
- `set_consistency_level(consistency_level)`: Sets the consistency level for queries

## Advanced Usage

### Asynchronous Queries

```python
future = crud.execute_async("SELECT * FROM my_table")
# Do other work...
result = future.result()
```

### Batch Operations

```python
statements = [
    ("INSERT INTO my_table (id, name) VALUES (%s, %s)", (1, "John")),
    ("INSERT INTO my_table (id, name) VALUES (%s, %s)", (2, "Jane"))
]
crud.execute_batch(statements)
```

### Changing Consistency Level

```python
from cassandra import ConsistencyLevel
crud.set_consistency_level(ConsistencyLevel.ALL)
```

## Contributing

Contributions to CassandraCRUD are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.