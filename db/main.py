import threading
import multiprocessing
import pickle
import os
import time
import random
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Database:
    """Represents a simple key-value database stored in a file using pickle."""
    
    def __init__(self, file_path='database.pkl'):
        """
        Initializes the database with a given file path.

        Args:
            file_path (str): The file path to load or save the database.
        """
        self.file_path = file_path
        self._data = self._load_data()

    def _load_data(self):
        """Loads the database from the file if it exists, otherwise returns an empty dictionary."""
        if os.path.exists(self.file_path):
            with open(self.file_path, 'rb') as f:
                return pickle.load(f)
        return {}

    def _save_data(self):
        """Saves the current database state to a file."""
        with open(self.file_path, 'wb') as f:
            pickle.dump(self._data, f)

    def value_set(self, key, val):
        """
        Inserts a value with the given key into the database.

        Args:
            key (str): The key to insert.
            val (any): The value to insert.

        Returns:
            bool: True if the insertion is successful.
        """
        logging.debug(f"Setting value: {key} = {val}")
        self._data[key] = val
        self._save_data()
        return True

    def value_get(self, key):
        """
        Retrieves the value associated with the given key.

        Args:
            key (str): The key for which to retrieve the value.

        Returns:
            any: The value associated with the key or None if it doesn't exist.
        """
        value = self._data.get(key, None)
        logging.debug(f"Getting value for key '{key}': {value}")
        return value

    def value_delete(self, key):
        """
        Deletes the value associated with the given key.

        Args:
            key (str): The key to delete.

        Returns:
            any: The value that was deleted or None if the key doesn't exist.
        """
        if key in self._data:
            val = self._data.pop(key)
            self._save_data()
            logging.debug(f"Deleted key: {key} with value: {val}")
            return val
        return None


class FileDatabase(Database):
    """Represents a file-based key-value database."""
    
    def __init__(self, file_path='database.pkl'):
        """Initializes the file database."""
        super().__init__(file_path)

    def value_set(self, key, val):
        """
        Inserts a value with the given key and saves to file immediately.

        Args:
            key (str): The key to insert.
            val (any): The value to insert.

        Returns:
            bool: True if the insertion is successful.
        """
        success = super().value_set(key, val)
        if success:
            self._save_data()  # Save data after insertion
        return success

    def value_delete(self, key):
        """
        Deletes the value associated with the given key, saves to file, and returns the value.

        Args:
            key (str): The key to delete.

        Returns:
            any: The value that was deleted or None if the key doesn't exist.
        """
        val = super().value_delete(key)
        if val is not None:
            self._save_data()  # Save data after deletion
        return val


class SynchronizedDatabase(FileDatabase):
    """A synchronized database that supports concurrent read and write operations."""

    def __init__(self, mode='threads', file_path='database.pkl'):
        """
        Initializes the synchronized database.

        Args:
            mode (str): The mode of synchronization ('threads' or 'processes').
            file_path (str): The file path to load or save the database.
        """
        super().__init__(file_path)
        self.mode = mode

        self.read_semaphore = (threading.Semaphore(10) if mode == 'threads'
                               else multiprocessing.Semaphore(10))
        self.write_lock = (threading.Lock() if mode == 'threads'
                           else multiprocessing.Lock())

    def value_set(self, key, val):
        """Inserts a value with exclusive write access."""
        with self.write_lock:
            logging.debug(f"Writing value to database: {key} = {val}")
            return super().value_set(key, val)

    def value_get(self, key):
        """Allows concurrent read access while preventing writes."""
        with self.read_semaphore:
            return super().value_get(key)

    def value_delete(self, key):
        """Deletes a value with exclusive write access."""
        with self.write_lock:
            return super().value_delete(key)


class DatabaseTesterThreads:
    """Tests the SynchronizedDatabase using threads."""
    
    def __init__(self, synchronized_database):
        """
        Initializes the database tester.

        Args:
            synchronized_database (SynchronizedDatabase): The synchronized database to test.
        """
        self.synchronized_database = synchronized_database
        self.threads = []

    def read_from_db(self, thread_id):
        """Function for a thread to read from the database."""
        logging.debug(f"Thread {thread_id} attempting to read.")
        value = self.synchronized_database.value_get('test_key')
        logging.debug(f"Thread {thread_id} read value: {value}")
        assert value is not None, f"Thread {thread_id} failed to read value from database."
        time.sleep(random.uniform(0.1, 0.5))

    def write_to_db(self, thread_id, value):
        """Function for a thread to write to the database."""
        logging.debug(f"Thread {thread_id} attempting to write value: {value}")
        self.synchronized_database.value_set('test_key', value)
        logging.debug(f"Thread {thread_id} wrote value: {value}")
        time.sleep(random.uniform(0.1, 0.5))

    def start_test(self):
        """Starts the testing process with multiple threads."""
        for i in range(10):
            t = threading.Thread(target=self.read_from_db, args=(i,))
            self.threads.append(t)

        for i in range(5):
            t = threading.Thread(target=self.write_to_db, args=(i + 10, f"value_{i}"))
            self.threads.append(t)

        for t in self.threads:
            t.start()

        for t in self.threads:
            t.join()

        logging.info("All threads have finished execution.")


class DatabaseTesterProcesses:
    """Tests the SynchronizedDatabase using processes."""
    
    def __init__(self, synchronized_database):
        """
        Initializes the database tester.

        Args:
            synchronized_database (SynchronizedDatabase): The synchronized database to test.
        """
        self.synchronized_database = synchronized_database

    def read_from_db(self, process_id):
        """Function for a process to read from the database."""
        logging.debug(f"Process {process_id} attempting to read.")
        value = self.synchronized_database.value_get('test_key')
        logging.debug(f"Process {process_id} read value: {value}")
        assert value is not None, f"Process {process_id} failed to read value from database."
        time.sleep(random.uniform(0.1, 0.5))

    def write_to_db(self, process_id, value):
        """Function for a process to write to the database."""
        logging.debug(f"Process {process_id} attempting to write value: {value}")
        self.synchronized_database.value_set('test_key', value)
        logging.debug(f"Process {process_id} wrote value: {value}")
        time.sleep(random.uniform(0.1, 0.5))

    def start_test(self):
        """Starts the testing process with multiple processes."""
        processes = []

        for i in range(10):
            p = multiprocessing.Process(target=self.read_from_db, args=(i,))
            processes.append(p)

        for i in range(5):
            p = multiprocessing.Process(target=self.write_to_db, args=(i + 10, f"value_{i}"))
            processes.append(p)

        for p in processes:
            p.start()

        for p in processes:
            p.join()

        logging.info("All processes have finished execution.")


if __name__ == "__main__":
    # Test for threads
    synchronized_database_threads = SynchronizedDatabase(mode='threads')
    tester_threads = DatabaseTesterThreads(synchronized_database_threads)
    tester_threads.start_test()

    # Test for processes
    synchronized_database_processes = SynchronizedDatabase(mode='processes')
    tester_processes = DatabaseTesterProcesses(synchronized_database_processes)
    tester_processes.start_test()
