# FIXME Duplicate Code


class ParseError(Exception):
    def __init__(self, msg):
        super(ParseError).__init__(msg)

        self.msg = msg


class UnrecognizedCommandError(ParseError):
    def __init__(self, msg, raw_data):
        super(UnrecognizedCommandError).__init__(msg)

        self.msg = msg
        self.raw_data = raw_data


class PayloadLenError(ParseError):
    def __init__(self, msg):
        super(PayloadLenError).__init__(msg)

        ParseError.__init__(self, msg)


class ChecksumError(ParseError):
    def __init__(self, msg, raw_data):
        super(ChecksumError).__init__(msg)

        self.raw_data = raw_data


class TerminationError(Exception):
    def __init__(self, msg):
        super(TerminationError).__init__(msg)
