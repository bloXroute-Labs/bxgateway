import unittest

from bxcommon.test_util.pylint_reporter import *


class LintTests(unittest.TestCase):
    def test_lint_score(self):
        lint_score = lint_directory("src/bxgateway")

        self.assertGreater(lint_score, MIN_PYLINT_SCORE, "Lint score was too low, please lint and refactor your code.")
