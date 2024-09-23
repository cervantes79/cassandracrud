import traceback
from cassandra.cluster import Cluster, ExecutionProfile, ConsistencyLevel, ResultSet
from cassandra.policies import WhiteListRoundRobinPolicy, RetryPolicy, DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement, BatchStatement
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import time
import logging
from typing import Dict, Any, List, Type, TypeVar

T = TypeVar('T', bound='Model')

class CassandraORMException(Exception):
    pass

class Model:
    _table_name: str = None
    _primary_key: str = None
    _columns: Dict[str, str] = {}

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def from_row(cls, row):
        return cls(**row)

    def to_dict(self):
        return {col: getattr(self, col) for col in self._columns}

class CassandraCRUD:
    def __init__(self, contact_points, keyspace, username=None, password=None, 
                 pool_size=50, consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                 retry_policy=RetryPolicy(), load_balancing_policy=None, 
                 protocol_version=5, port=9042, ssl_context=None,
                 compression=True):
        self.contact_points = contact_points
        self.keyspace = keyspace
        self.username = username
        self.password = password
        self.pool_size = pool_size
        self.consistency_level = consistency_level
        self.retry_policy = retry_policy
        self.load_balancing_policy = load_balancing_policy or WhiteListRoundRobinPolicy(contact_points)
        self.protocol_version = protocol_version
        self.port = port
        self.ssl_context = ssl_context
        self.compression = compression
        self.session = None
        self.cluster = None
        self.logger = logging.getLogger(__name__)
        self.models = {}
        self.connect()
        self.introspect_schema()

    def connect(self):
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            try:
                execution_profile = ExecutionProfile(
                    load_balancing_policy=self.load_balancing_policy,
                    retry_policy=self.retry_policy,
                    consistency_level=self.consistency_level,
                    serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
                    request_timeout=15,
                    row_factory=None  # We'll convert to pandas DataFrame ourselves
                )
                auth_provider = PlainTextAuthProvider(username=self.username, password=self.password) if self.username and self.password else None

                self.cluster = Cluster(
                    contact_points=self.contact_points,
                    port=self.port,
                    execution_profiles={"default": execution_profile},
                    protocol_version=self.protocol_version,
                    auth_provider=auth_provider,
                    ssl_context=self.ssl_context,
                    compression=self.compression
                )
                self.session = self.cluster.connect(self.keyspace)
                self.logger.info(f"Connected to Cassandra keyspace: {self.keyspace}")
                break
            except Exception as e:
                self.logger.error(f"Connection error: {str(e)}")
                retry_count += 1
                time.sleep(1)
        
        if retry_count == max_retries:
            raise ConnectionError("Failed to connect to Cassandra after multiple attempts")

    def introspect_schema(self):
        tables_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
        tables = self.execute(tables_query, [self.keyspace])
        
        for table in tables['table_name']:
            columns_query = "SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = %s AND table_name = %s"
            columns = self.execute(columns_query, [self.keyspace, table])
            
            primary_key_query = """
                SELECT column_name 
                FROM system_schema.columns 
                WHERE keyspace_name = %s AND table_name = %s AND kind = 'partition_key'
            """
            primary_key = self.execute(primary_key_query, [self.keyspace, table])
            
            model_name = ''.join(word.capitalize() for word in table.split('_'))
            columns_dict = dict(zip(columns['column_name'], columns['type']))
            primary_key = primary_key['column_name'][0] if not primary_key.empty else None
            
            model = type(model_name, (Model,), {
                '_table_name': table,
                '_primary_key': primary_key,
                '_columns': columns_dict
            })
            
            self.models[table] = model

    def get_model(self, table_name):
        return self.models.get(table_name)

    def execute(self, query, params=None):
        try:
            statement = SimpleStatement(query)
            result = self.session.execute(statement, params) if params else self.session.execute(statement)
            return pd.DataFrame(result)
        except Exception as e:
            self.logger.error(f"Query execution error: {str(e)}")
            self.logger.error(f"Query: {query}")
            return pd.DataFrame()

    def execute_raw(self, query, params=None):
        return self.execute(query, params)

    def create(self, table, data):
        model = self.get_model(table)
        if not model:
            raise CassandraORMException(f"Table {table} not found in the schema")
        
        if isinstance(data, pd.DataFrame):
            batch = BatchStatement(consistency_level=self.consistency_level)
            columns = ", ".join(data.columns)
            placeholders = ", ".join(["%s"] * len(data.columns))
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            
            for _, row in data.iterrows():
                batch.add(SimpleStatement(query), list(row))
            
            self.session.execute(batch)
        elif isinstance(data, dict):
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            self.execute(query, list(data.values()))
        else:
            raise CassandraORMException("Data must be a pandas DataFrame or a dictionary")

    def read(self, table, conditions=None, columns=None):
        model = self.get_model(table)
        if not model:
            raise CassandraORMException(f"Table {table} not found in the schema")
        
        select_columns = "*" if not columns else ", ".join(columns)
        query = f"SELECT {select_columns} FROM {table}"
        params = []
        
        if conditions:
            where_clauses = []
            for key, value in conditions.items():
                if isinstance(value, (list, tuple)):
                    where_clauses.append(f"{key} IN %s")
                    params.append(tuple(value))
                else:
                    where_clauses.append(f"{key} = %s")
                    params.append(value)
            query += " WHERE " + " AND ".join(where_clauses)
        
        result = self.execute(query, params)
        return result.apply(lambda row: model.from_row(row), axis=1).tolist() if not result.empty else []

    def update(self, table, data, conditions):
        model = self.get_model(table)
        if not model:
            raise CassandraORMException(f"Table {table} not found in the schema")
        
        set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
        where_clause = " AND ".join([f"{k} = %s" for k in conditions.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        params = list(data.values()) + list(conditions.values())
        self.execute(query, params)

    def delete(self, table, conditions):
        model = self.get_model(table)
        if not model:
            raise CassandraORMException(f"Table {table} not found in the schema")
        
        where_clause = " AND ".join([f"{k} = %s" for k in conditions.keys()])
        query = f"DELETE FROM {table} WHERE {where_clause}"
        self.execute(query, list(conditions.values()))

    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            self.logger.info("Cassandra cluster connection closed.")

# Example usage:
if __name__ == "__main__":
    crud = CassandraCRUD(contact_points=['localhost'], keyspace='my_keyspace')

    # Create
    user_data = pd.DataFrame([
        {'id': 1, 'name': 'John Doe', 'email': 'john@example.com'},
        {'id': 2, 'name': 'Jane Doe', 'email': 'jane@example.com'}
    ])
    crud.create('users', user_data)

    # Read
    users = crud.read('users', conditions={'id': [1, 2]})
    print(users)

    # Update
    crud.update('users', {'email': 'johndoe@example.com'}, {'id': 1})

    # Delete
    crud.delete('users', {'id': 2})

    # Raw query
    result = crud.execute_raw("SELECT * FROM users WHERE id = %s", [1])
    print(result)

    crud.close()