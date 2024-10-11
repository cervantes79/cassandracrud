import unittest
from unittest.mock import patch, MagicMock
from cassandracrud import CassandraCRUD

class TestCassandraCRUD(unittest.TestCase):
    
    @patch('cassandracrud.core.Cluster')
    def setUp(self, mock_cluster):
        self.crud = CassandraCRUD(contact_points=['localhost'], keyspace='test_keyspace')
        self.crud.connect()

    def test_connect(self):
        self.assertIsNotNone(self.crud.session)
        self.assertIsNotNone(self.crud.cluster)

    def test_execute(self):
        mock_result = MagicMock()
        self.crud.session.execute = MagicMock(return_value=mock_result)
        result = self.crud.execute("SELECT * FROM test_table")
        self.assertEqual(result, mock_result)

if __name__ == '__main__':
    unittest.main()