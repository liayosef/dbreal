import threading
import multiprocessing
import pickle
import os
import time
import random


class Database:
    def __init__(self, file_path='database.pkl'):
        self.file_path = file_path
        self._data = self._load_data()

    def _load_data(self):
        """Loads the database from a file if it exists, otherwise returns an empty dictionary."""
        if os.path.exists(self.file_path):
            with open(self.file_path, 'rb') as f:
                return pickle.load(f)
        return {}

    def _save_data(self):
        """Saves the database to a file."""
        with open(self.file_path, 'wb') as f:
            pickle.dump(self._data, f)

    def value_set(self, key, val):
        """Inserts a value with the given key, returns True on success, False otherwise."""
        self._data[key] = val
        self._save_data()
        return True

    def value_get(self, key):
        """Returns the value mapped to the key, or None if it doesn't exist."""
        return self._data.get(key, None)

    def value_delete(self, key):
        """Deletes a value mapped to the key and returns it, or None if the key doesn't exist."""
        if key in self._data:
            val = self._data.pop(key)
            self._save_data()
            return val
        return None


class FileDatabase(Database):
    def __init__(self, file_path='database.pkl'):
        super().__init__(file_path)

    def value_set(self, key, val):
        """Inserts a value with the given key and saves to file immediately."""
        success = super().value_set(key, val)
        if success:
            self._save_data()  # Save data after insertion
        return success

    def value_delete(self, key):
        """Deletes a value mapped to the key, saves to file, and returns the value."""
        val = super().value_delete(key)
        if val is not None:
            self._save_data()  # Save data after deletion
        return val


class SynchronizedDatabase(FileDatabase):
    def __init__(self, mode='threads', file_path='database.pkl'):
        super().__init__(file_path)
        self.mode = mode

        # Semaphore to control concurrent read access (up to 10)
        self.read_semaphore = (threading.Semaphore(10) if mode == 'threads'
                               else multiprocessing.Semaphore(10))

        # Lock to control exclusive write access
        self.write_lock = (threading.Lock() if mode == 'threads'
                           else multiprocessing.Lock())

    def value_set(self, key, val):
        """Inserts a value with exclusive write access."""
        with self.write_lock:  # Exclusive access for write
            return super().value_set(key, val)

    def value_get(self, key):
        """Allows up to 10 concurrent read accesses, but blocks if a write is in progress."""
        with self.write_lock:  # Prevents reading if a write is in progress
            with self.read_semaphore:  # Limited concurrent read access
                return super().value_get(key)

    def value_delete(self, key):
        """Deletes a value with exclusive write access."""
        with self.write_lock:  # Exclusive access for delete
            return super().value_delete(key)


class DatabaseTesterThreads:
    def __init__(self, synchronized_database1):
        self.synchronized_database = synchronized_database1
        self.threads = []

    def read_from_db(self, thread_id):
        """Function for a thread to read from the database."""
        print(f"Thread {thread_id} attempting to read.")
        value = self.synchronized_database.value_get('test_key')
        print(f"Thread {thread_id} read value: {value}")
        time.sleep(random.uniform(0.1, 0.5))  # Simulate time spent reading

    def write_to_db(self, thread_id, value):
        """Function for a thread to write to the database."""
        print(f"Thread {thread_id} attempting to write value: {value}")
        self.synchronized_database.value_set('test_key', value)
        print(f"Thread {thread_id} wrote value: {value}")
        time.sleep(random.uniform(0.1, 0.5))  # Simulate time spent writing

    def start_test(self):
        """Sets up and starts threads for testing the SynchronizedDatabase."""
        # Create 10 threads for reading
        for i in range(10):
            t = threading.Thread(target=self.read_from_db, args=(i,))
            self.threads.append(t)

        # Create 5 threads for writing
        for i in range(5):
            t = threading.Thread(target=self.write_to_db, args=(i + 10, f"value_{i}"))
            self.threads.append(t)

        # Start all threads
        for t in self.threads:
            t.start()

        # Wait for all threads to complete
        for t in self.threads:
            t.join()

        print("All threads have finished execution.")


# Example of using DatabaseTesterThreads
if __name__ == "__main__":
    # Create a SynchronizedDatabase in thread mode
    synchronized_database = SynchronizedDatabase(mode='threads')

    # Instantiate and run the tester
    tester = DatabaseTesterThreads(synchronized_database)
    tester.start_test()


class DatabaseTesterProcesses:
    def __init__(self, synchronized_database2):
        self.synchronized_database = synchronized_database2

    def read_from_db(self, process_id):
        """Function for a process to read from the database."""
        print(f"Process {process_id} attempting to read.")
        value = self.synchronized_database.value_get('test_key')
        print(f"Process {process_id} read value: {value}")
        time.sleep(random.uniform(0.1, 0.5))  # Simulate time spent reading

    def write_to_db(self, process_id, value):
        """Function for a process to write to the database."""
        print(f"Process {process_id} attempting to write value: {value}")
        self.synchronized_database.value_set('test_key', value)
        print(f"Process {process_id} wrote value: {value}")
        time.sleep(random.uniform(0.1, 0.5))  # Simulate time spent writing

    def start_test(self):
        """Sets up and starts processes for testing the SynchronizedDatabase."""
        processes = []

        # Create 10 processes for reading
        for i in range(10):
            p = multiprocessing.Process(target=self.read_from_db, args=(i,))
            processes.append(p)

        # Create 5 processes for writing
        for i in range(5):
            p = multiprocessing.Process(target=self.write_to_db, args=(i + 10, f"value_{i}"))
            processes.append(p)

        # Start all processes
        for p in processes:
            p.start()

        # Wait for all processes to complete
        for p in processes:
            p.join()

        print("All processes have finished execution.")


# Example of using DatabaseTesterProcesses
if __name__ == "__main__":
    # Create a SynchronizedDatabase in process mode
    synchronized_database = SynchronizedDatabase(mode='processes')

    # Instantiate and run the tester
    tester = DatabaseTesterProcesses(synchronized_database)
    tester.start_test()