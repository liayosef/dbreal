import unittest
import threading
import multiprocessing
import time
from main import SynchronizedDatabase, DatabaseTesterThreads, DatabaseTesterProcesses


class TestDatabaseThreads(unittest.TestCase):
    def setUp(self):
        self.database = SynchronizedDatabase(mode='threads')
        self.tester = DatabaseTesterThreads(self.database)

    def test_write_permission_without_race(self):
        """Test that writing to the database works without race conditions."""
        result = self.database.value_set('test_key', 'initial_value')
        self.assertIsNone(result)  # Ensure write is successful

        value = self.database.value_get('test_key')
        self.assertEqual(value, 'initial_value')

    def test_read_permission_without_race(self):
        """Test that reading from the database works without race conditions."""
        self.database.value_set('test_key', 'read_value')
        value = self.database.value_get('test_key')
        self.assertEqual(value, 'read_value')

    def test_write_permission_during_read(self):
        """Test that writing cannot happen while reading is in progress."""
        def read():
            self.database.value_get('test_key')

        read_thread = threading.Thread(target=read)
        read_thread.start()
        time.sleep(0.1)  # Ensure the read starts first

        result = self.database.value_set('test_key', 'new_value')
        self.assertIsNotNone(result)  # Should not write while reading is in progress

        read_thread.join()

    def test_read_permission_during_write(self):
        """Test that reading cannot happen while writing is in progress."""
        def write():
            self.database.value_set('test_key', 'new_value')

        write_thread = threading.Thread(target=write)
        write_thread.start()
        time.sleep(0.1)  # Ensure the write starts first

        value = self.database.value_get('test_key')
        self.assertIsNone(value)  # Should not read while writing is in progress

        write_thread.join()

    def test_multiple_readers(self):
        """Test that multiple readers can access the database at the same time."""
        self.database.value_set('test_key', 'multiple_readers')

        def read():
            return self.database.value_get('test_key')

        threads = [threading.Thread(target=read) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        value = self.database.value_get('test_key')
        self.assertEqual(value, 'multiple_readers')

    def test_readers_block_writers(self):
        """Test that readers block writers."""
        self.database.value_set('test_key', 'initial_value')

        def read():
            return self.database.value_get('test_key')

        def write():
            self.database.value_set('test_key', 'new_value')

        readers = [threading.Thread(target=read) for _ in range(5)]
        writer = threading.Thread(target=write)

        for r in readers:
            r.start()
        time.sleep(0.1)  # Ensure readers start first

        writer.start()
        writer.join()  # Wait for writer to finish

        for r in readers:
            r.join()

        value = self.database.value_get('test_key')
        self.assertEqual(value, 'initial_value')


class TestDatabaseProcesses(unittest.TestCase):
    def setUp(self):
        self.database = SynchronizedDatabase(mode='processes')
        self.tester = DatabaseTesterProcesses(self.database)

    def test_write_permission_without_race(self):
        """Test that writing to the database works without race conditions."""
        result = self.database.value_set('test_key', 'initial_value')
        self.assertIsNone(result)  # Ensure write is successful

        value = self.database.value_get('test_key')
        self.assertEqual(value, 'initial_value')

    def test_read_permission_without_race(self):
        """Test that reading from the database works without race conditions."""
        self.database.value_set('test_key', 'read_value')
        value = self.database.value_get('test_key')
        self.assertEqual(value, 'read_value')

    def test_write_permission_during_read(self):
        """Test that writing cannot happen while reading is in progress."""
        def read():
            self.database.value_get('test_key')

        read_process = multiprocessing.Process(target=read)
        read_process.start()
        time.sleep(0.1)  # Ensure the read starts first

        result = self.database.value_set('test_key', 'new_value')
        self.assertIsNotNone(result)  # Should not write while reading is in progress

        read_process.join()

    def test_read_permission_during_write(self):
        """Test that reading cannot happen while writing is in progress."""
        def write():
            self.database.value_set('test_key', 'new_value')

        write_process = multiprocessing.Process(target=write)
        write_process.start()
        time.sleep(0.1)  # Ensure the write starts first

        value = self.database.value_get('test_key')
        self.assertIsNone(value)  # Should not read while writing is in progress

        write_process.join()

    def test_multiple_readers(self):
        """Test that multiple readers can access the database at the same time."""
        self.database.value_set('test_key', 'multiple_readers')

        def read():
            return self.database.value_get('test_key')

        processes = [multiprocessing.Process(target=read) for _ in range(5)]
        for p in processes:
            p.start()
        for p in processes:
            p.join()

        value = self.database.value_get('test_key')
        self.assertEqual(value, 'multiple_readers')

    def test_readers_block_writers(self):
        """Test that readers block writers."""
        self.database.value_set('test_key', 'initial_value')

        def read():
            return self.database.value_get('test_key')

        def write():
            self.database.value_set('test_key', 'new_value')

        readers = [multiprocessing.Process(target=read) for _ in range(5)]
        writer = multiprocessing.Process(target=write)

        for r in readers:
            r.start()
        time.sleep(0.1)  # Ensure readers start first

        writer.start()
        writer.join()  # Wait for writer to finish

        for r in readers:
            r.join()

        value = self.database.value_get('test_key')
        self.assertEqual(value, 'initial_value')


if __name__ == '__main__':
    unittest.main()