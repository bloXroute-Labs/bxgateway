import unittest
from bxgateway import log_messages


class TestLogMessageCodes(unittest.TestCase):
    def test_log_message_codes_are_unique(self):
        codes = set()
        for item in dir(log_messages):
            obj = getattr(log_messages, item)
            if isinstance(obj, log_messages.LogMessage):
                self.assertNotIn(obj.code, codes)
                codes.add(obj.code)
            else:
                print(obj)


if __name__ == '__main__':
    unittest.main()
