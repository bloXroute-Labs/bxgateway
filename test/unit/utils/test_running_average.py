import unittest

from bxgateway.utils.running_average import RunningAverage


class TestRunningAverage(unittest.TestCase):

    def test_system(self):
        sut = RunningAverage(4)

        sut.add_value(1)
        self.assertEqual(1, sut.average)

        sut.add_value(3)
        self.assertEqual(2, sut.average)

        sut.add_value(11)
        self.assertEqual(5, sut.average)

        sut.add_value(5)
        self.assertEqual(5, sut.average)

        sut.add_value(1)
        self.assertEqual(5, sut.average)

        sut.add_value(3)
        self.assertEqual(5, sut.average)
