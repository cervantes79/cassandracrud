# cassandracrud

cassandracrud is an advanced Python ORM (Object-Relational Mapping) library for Apache Cassandra. It simplifies CRUD (Create, Read, Update, Delete) operations with automatic schema discovery, dynamic model creation, and pandas DataFrame integration.

## Features

- Automatic database schema discovery and dynamic model creation
- Data processing with pandas DataFrame integration
- Simplified CRUD operations
- Support for bulk data insertion
- Custom query execution capability
- Flexible query conditions (e.g., IN queries)
- Connection pooling and automatic reconnection
- Secure connections with SSL support

## Installation

```
pip install cassandracrud
```

## Quick Start

```python
from cassandracrud import CassandraCRUD
import pandas as pd

# Initialize the connection
crud = CassandraCRUD(
    contact_points=['localhost'],
    keyspace='my_keyspace',
    username='user',
    password='password'
)

# Create data
user_data = pd.DataFrame([
    {'id': 1, 'name': 'John Doe', 'email': 'john@example.com'},
    {'id': 2, 'name': 'Jane Doe', 'email': 'jane@example.com'}
])
crud.create('users', user_data)

# Read data
users = crud.read('users', conditions={'id': [1, 2]})
print(users)

# Update data
crud.update('users', {'email': 'johndoe@example.com'}, {'id': 1})

# Delete data
crud.delete('users', {'id': 2})

# Execute a raw query
result = crud.execute_raw("SELECT * FROM users WHERE id = %s", [1])
print(result)

# Close the connection
crud.close()
```

## Advanced Usage

### Automatic Model Creation

CassandraCRUD automatically discovers the database schema and dynamically creates Model classes for each table. You can access these models as follows:

```python
UserModel = crud.get_model('users')
print(UserModel._columns)
print(UserModel._primary_key)
```

### Bulk Data Insertion

You can insert bulk data using pandas DataFrames:

```python
bulk_data = pd.DataFrame([
    {'id': 3, 'name': 'Alice', 'email': 'alice@example.com'},
    {'id': 4, 'name': 'Bob', 'email': 'bob@example.com'},
    {'id': 5, 'name': 'Charlie', 'email': 'charlie@example.com'}
])
crud.create('users', bulk_data)
```

### Flexible Query Conditions

Supports more complex conditions like IN queries:

```python
users = crud.read('users', conditions={'id': [1, 3, 5]})
```

### Custom Query Execution

You can execute custom CQL queries using the `execute_raw` method:

```python
result = crud.execute_raw("SELECT * FROM users WHERE name LIKE %s", ['%Doe'])
print(result)
```

## Configuration Options

The CassandraCRUD class offers various options to customize the connection and query execution behavior:

- `contact_points`: List of Cassandra nodes
- `keyspace`: Name of the keyspace to use
- `username` and `password`: Authentication credentials
- `pool_size`: Connection pool size
- `consistency_level`: Consistency level
- `retry_policy`: Retry policy
- `load_balancing_policy`: Load balancing policy
- `protocol_version`: Cassandra protocol version
- `port`: Cassandra port number
- `ssl_context`: SSL context for SSL connection
- `compression`: Whether to use compression

## Error Handling

CassandraCRUD raises `CassandraORMException` for errors that may occur during operations. You can catch these errors as follows:

```python
from cassandracrud import CassandraCRUD, CassandraORMException

try:
    crud.create('non_existent_table', {'data': 'value'})
except CassandraORMException as e:
    print(f"An error occurred: {str(e)}")
```

## Performance Tips

- Use DataFrames for bulk operations.
- Use pagination with the `read` method for large datasets.
- Use prepared statements for frequently used queries.

## Contributing

We welcome contributions! Please make sure to run your tests before submitting a pull request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more information.

## Contact

For questions or feedback, please open an issue on GitHub or send an email to info@gencbaris.com .