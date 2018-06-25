import re
import unittest

from pylint import lint
from pylint.reporters.text import TextReporter


class PyLintWritable(object):
    def __init__(self):
        self.content = []

    def write(self, st):
        self.content.append(st)

    def read(self):
        return self.content


class LintTests(unittest.TestCase):
    def test_lint_client(self):
        pylint_args = ["-r", "n", "--rcfile=pylintrc",
                       "'--msg-template={path}:{line}: [{msg_id}({symbol}), {obj}] {msg}'"]
        pylint_output = PyLintWritable()

        lint.Run(["src/client"] + pylint_args, reporter=TextReporter(pylint_output), exit=False)

        output = list(pylint_output.read())

        rate = 0
        if output:
            match = re.match(r'[^\d]+(\-?\d{1,3}\.\d{2}).*', output[-3])
            if not match:
                return 0
            rate = float(match.groups()[0])

        print "".join(output)

        self.assertGreater(rate, 9.5, "Lint score was too low, please lint and refactor your code.")
