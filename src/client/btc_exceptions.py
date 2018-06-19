class ParseError(Exception):
    def __init__(self, msg):
        self.msg = msg


class UnrecognizedCommandError(ParseError):
    def __init__(self, msg, raw_data):
        self.msg = msg
        self.raw_data = raw_data


class PayloadLenError(ParseError):
    def __init__(self, msg):
        ParseError.__init__(self, msg)


class ChecksumError(ParseError):
    def __init__(self, msg, raw_data):
        ParseError.__init__(self, msg)
        self.raw_data = raw_data


class TerminationError(Exception):
    pass
