import traceback
from cassandra.cluster import Cluster, ExecutionProfile, ConsistencyLevel, ResultSet
from cassandra.policies import WhiteListRoundRobinPolicy, RetryPolicy
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import time
import os
from datetime import datetime

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

class CassandraCRUD:
    def __init__(self, 
                 contact_points=None,
                 keyspace=None,
                 username=None,
                 password=None,
                 pool_size=50,
                 consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                 serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
                 request_timeout=15,
                 load_balancing_policy=None,
                 retry_policy=None,
                 protocol_version=5,
                 row_factory=pandas_factory):
        self.contact_points = contact_points
        self.keyspace = keyspace
        self.username = username
        self.password = password
        self.pool_size = pool_size
        self.consistency_level = consistency_level
        self.serial_consistency_level = serial_consistency_level
        self.request_timeout = request_timeout
        self.load_balancing_policy = load_balancing_policy
        self.retry_policy = retry_policy or RetryPolicy()
        self.protocol_version = protocol_version
        self.row_factory = row_factory
        
        self.session = None
        self.cluster = None

    def connect(self):
        if not self.contact_points or not self.keyspace:
            self._configure_from_environment()

        try:
            execution_profile = ExecutionProfile(
                load_balancing_policy=self.load_balancing_policy or WhiteListRoundRobinPolicy(self.contact_points),
                retry_policy=self.retry_policy,
                consistency_level=self.consistency_level,
                serial_consistency_level=self.serial_consistency_level,
                request_timeout=self.request_timeout,
                row_factory=self.row_factory)
            
            auth_provider = PlainTextAuthProvider(username=self.username, password=self.password) if self.username and self.password else None
            
            self.cluster = Cluster(
                contact_points=self.contact_points,
                execution_profiles={"default": execution_profile},
                protocol_version=self.protocol_version,
                auth_provider=auth_provider,
            )
            self.session = self.cluster.connect(self.keyspace)
            print(f"Connected to Cassandra keyspace: {self.keyspace}")
        except Exception as e:
            print(f"Connection error: {str(e)}")
            raise

    def _configure_from_environment(self):
        self.contact_points = os.getenv("CASSANDRA_PROD_CONTACT_POINTS", "cassandra.prod.svc.cluster.local").split(",")
        self.keyspace = os.getenv("CASSANDRA_PROD_KEYSPACE", "prod_keyspace")
        self.username = os.getenv("CASSANDRA_PROD_USERNAME", "")
        self.password = os.getenv("CASSANDRA_PROD_PASSWORD", "")

    def is_connected(self):
        if self.session is not None and self.cluster is not None:
            try:
                self.session.execute("SELECT now() FROM system.local")
                return True
            except Exception as e:
                print(f"Connection check failed: {str(e)}")
                return False
        return False

    def execute(self, query, params=None):
        if not self.is_connected():
            self.connect()
        try:
            statement = SimpleStatement(query)
            result = self.session.execute(statement, params) if params else self.session.execute(statement)
            if isinstance(result, ResultSet):
                return pd.DataFrame(list(result))
            else:
                return result
        except Exception as e:
            print(f"Query execution error: {str(e)}")
            print(f"Query: {query}")
            return pd.DataFrame()

    def execute_async(self, query, params=None):
        if not self.is_connected():
            self.connect()
        try:
            statement = SimpleStatement(query)
            return self.session.execute_async(statement, params)
        except Exception as e:
            print(f"Async query execution error: {str(e)}")
            print(f"Query: {query}")
            raise

    def prepare(self, query):
        if not self.is_connected():
            self.connect()
        return self.session.prepare(query)

    def execute_batch(self, statements):
        if not self.is_connected():
            self.connect()
        try:
            batch = self.session.batch(statements)
            return self.session.execute(batch)
        except Exception as e:
            print(f"Batch execution error: {str(e)}")
            raise

    def get_metrics(self):
        if self.cluster:
            return self.cluster.metrics
        return None

    def set_consistency_level(self, consistency_level):
        self.consistency_level = consistency_level
        if self.session:
            self.session.default_consistency_level = consistency_level

    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            print("Cassandra cluster connection closed.")

    # CRUD operations
    def create(self, table, data):
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return self.execute(query, list(data.values()))

    def read(self, table, conditions=None):
        query = f"SELECT * FROM {table}"
        if conditions:
            where_clause = " AND ".join([f"{k} = %s" for k in conditions.keys()])
            query += f" WHERE {where_clause}"
            return self.execute(query, list(conditions.values()))
        return self.execute(query)

    def update(self, table, data, conditions):
        set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
        where_clause = " AND ".join([f"{k} = %s" for k in conditions.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        params = list(data.values()) + list(conditions.values())
        return self.execute(query, params)

    def delete(self, table, conditions):
        where_clause = " AND ".join([f"{k} = %s" for k in conditions.keys()])
        query = f"DELETE FROM {table} WHERE {where_clause}"
        return self.execute(query, list(conditions.values()))

    # Additional utility methods
    def table_exists(self, table_name):
        query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s"
        result = self.execute(query, (self.keyspace, table_name))
        return len(result) > 0

    def create_table(self, table_name, column_definitions):
        columns = ", ".join([f"{name} {type}" for name, type in column_definitions.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        return self.execute(query)

    def drop_table(self, table_name):
        query = f"DROP TABLE IF EXISTS {table_name}"
        return self.execute(query)

    def get_table_schema(self, table_name):
        query = f"SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = %s AND table_name = %s"
        return self.execute(query, (self.keyspace, table_name))