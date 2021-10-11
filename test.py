"""
Unit test to verify streamer package
"""
import unittest
import unittest.mock
from streamer import split, split_gen

class TestStreamer(unittest.TestCase):
    """
    Test split utilities from streamer package
    """

    def setUp(self):
        """
        Prepare input for splitting
        """
        self.records = [
            # Length: 84
            'I Would Have Thought The Hardest Part Was Serving That Pressing Need Of Yours To ...',
            # Length: 64
            'Ive Got A Charge In My Head, Im Going To Die Unless You Kill Me!',
            # Length: 42
            'Just Stay Alive! Im Not Going To Lose You!',
            # Length: 55
            'If I Let You Know Where Im Going, I Wont Be On Holiday.',
            # Length: 40
            'Whats Done Is Done When We Say Its Done!',
            # Length: 21
            'Mission Accomplished!',
            # Length: 85
            'We just rolled up a snowball and tossed it into hell. Now let’s see what chance it...',
            # Length: 42
            'Kittridge, Youve Never Seen Me Very Upset!',
            # Length: 29
            'The Countdown Is Not Helping.',
            # Length: 21
            'Thats The Wrong Door!',
            # Length: 40
            'Well Burn That Bridge When We Get To It.',
            # Length: 23
            'Red Light! Green Light!'
        ]

    def test_split_on_batch_size(self):
        """
        Test if batches are being made correctly on reaching size threshold
        """
        batches = split(records = self.records,
                        max_record_size = 70,
                        max_batch_size = 200,
                        max_batch_len = 20)

        self.assertEqual(len(batches), 3)
        self.assertIsInstance(batches, list)
        # First record (record 0) is ignored as its size > 70, Batch size: 161
        self.assertEqual(batches[0], [self.records[1], self.records[2], self.records[3]])
        # Record 6 is ignored as its size > 70, Batch size: 193
        self.assertEqual(batches[1], [self.records[4], self.records[5], self.records[7],
                                      self.records[8], self.records[9], self.records[10]])
        # Last batch, batch size: 23
        self.assertEqual(batches[2], [self.records[11]])

    def test_split_on_batch_len(self):
        """
        Test if batches are being made correctly on reaching limit on length
        """
        batches = split(records = self.records,
                        max_record_size = 70,
                        max_batch_size = 400,
                        max_batch_len = 5)

        self.assertEqual(len(batches), 2)
        self.assertIsInstance(batches, list)
        # First record (record 0) is ignored as its size > 70, Batch length: 5
        self.assertEqual(batches[0], [self.records[1], self.records[2], self.records[3],
                                      self.records[4], self.records[5]])
        # Record 6 is ignored as its size > 70, Batch length: 5
        self.assertEqual(batches[1], [self.records[7], self.records[8], self.records[9],
                                      self.records[10], self.records[11]])

    def test_split_gen(self):
        """
        Test if generator is creating batches correctly
        """
        batches = split_gen(records = self.records,
                            max_record_size = 70,
                            max_batch_size = 200,
                            max_batch_len = 4)

        # Generator returned an iterator
        self.assertTrue(hasattr(batches, '__iter__'))

        # First record (record 0) is ignored as its size > 70
        # Batch size: 161, Length: 3, Break on batch size
        batch = next(batches)
        self.assertEqual(batch, [self.records[1], self.records[2], self.records[3]])
        # Record 6 is ignored as its size > 70
        # Batch size: 153, Length: 4, Break on batch length
        batch = next(batches)
        self.assertEqual(batch, [self.records[4], self.records[5], self.records[7],
                                 self.records[8]])
        # Last batch, batch size: 84, Length: 3
        batch = next(batches)
        self.assertEqual(batch, [self.records[9], self.records[10], self.records[11]])

        # No more batch
        with self.assertRaises(StopIteration):
            next(batches)

    def test_invalid_config_batch_len(self):
        """
        Test if corrrect error is thrown when batch length is configured incorrectly
        """
        with self.assertRaises(ValueError) as err_cntx:
            split(records = self.records,
                  max_record_size = 70,
                  max_batch_size = 200,
                  max_batch_len = 0)
            self.assertEqual('Max batch length must be greater than zero', str(err_cntx.exception))

    def test_invalid_config_record_size(self):
        """
        Test if corrrect error is thrown when record size is configured incorrectly
        """
        with self.assertRaises(ValueError) as err_cntx:
            split(records = self.records,
                  max_record_size = 0,
                  max_batch_size = 200,
                  max_batch_len = 5)
            self.assertEqual('Max record size must be greater than zero', str(err_cntx.exception))

    def test_invalid_config_batch_size(self):
        """
        Test if corrrect error is thrown when batch size is configured incorrectly
        """
        with self.assertRaises(ValueError) as err_cntx:
            split(records = self.records,
                  max_record_size = 200,
                  max_batch_size = 100,
                  max_batch_len = 5)
            self.assertEqual('Max batch size must be greater than or equal to max record size',
                              str(err_cntx.exception))

if __name__ == '__main__':
    unittest.main()
