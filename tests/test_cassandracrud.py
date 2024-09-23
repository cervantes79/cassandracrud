import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from cassandracrud.cassandracrud import CassandraCRUD, CassandraORMException

class TestCassandraCRUD(unittest.TestCase):

    @patch('cassandracrud.Cluster')
    def setUp(self, mock_cluster):
        self.mock_session = MagicMock()
        mock_cluster.return_value.connect.return_value = self.mock_session
        self.crud = CassandraCRUD(contact_points=['localhost'], keyspace='test_keyspace')

    def test_connection(self):
        self.assertIsNotNone(self.crud.session)
        self.assertEqual(self.crud.keyspace, 'test_keyspace')

    @patch('cassandracrud.CassandraCRUD.execute')
    def test_introspect_schema(self, mock_execute):
        mock_execute.side_effect = [
            pd.DataFrame({'table_name': ['users', 'products']}),
            pd.DataFrame({'column_name': ['id', 'name'], 'type': ['int', 'text']}),
            pd.DataFrame({'column_name': ['id']}),
            pd.DataFrame({'column_name': ['id', 'name'], 'type': ['int', 'text']}),
            pd.DataFrame({'column_name': ['id']})
        ]
        self.crud.introspect_schema()
        self.assertIn('users', self.crud.models)
        self.assertIn('products', self.crud.models)
        self.assertEqual(self.crud.models['users']._primary_key, 'id')

    def test_get_model(self):
        self.crud.models['test_table'] = MagicMock()
        model = self.crud.get_model('test_table')
        self.assertIsNotNone(model)
        with self.assertRaises(CassandraORMException):
            self.crud.get_model('non_existent_table')

    @patch('cassandracrud.CassandraCRUD.execute')
    def test_create(self, mock_execute):
        data = pd.DataFrame({'id': [1, 2], 'name': ['John', 'Jane']})
        self.crud.create('users', data)
        mock_execute.assert_called()

    @patch('cassandracrud.CassandraCRUD.execute')
    def test_read(self, mock_execute):
        mock_execute.return_value = pd.DataFrame({'id': [1], 'name': ['John']})
        result = self.crud.read('users', conditions={'id': 1})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, 'John')

    @patch('cassandracrud.CassandraCRUD.execute')
    def test_update(self, mock_execute):
        self.crud.update('users', {'name': 'John Doe'}, {'id': 1})
        mock_execute.assert_called()

    @patch('cassandracrud.CassandraCRUD.execute')
    def test_delete(self, mock_execute):
        self.crud.delete('users', {'id': 1})
        mock_execute.assert_called()

    @patch('cassandracrud.CassandraCRUD.execute')
    def test_execute_raw(self, mock_execute):
        mock_execute.return_value = pd.DataFrame({'id': [1], 'name': ['John']})
        result = self.crud.execute_raw("SELECT * FROM users WHERE id = %s", [1])
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)

    def test_close(self):
        self.crud.close()
        self.crud.cluster.shutdown.assert_called_once()

if __name__ == '__main__':
    unittest.main()