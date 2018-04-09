#
# Copyright (C) 2017, bloXroute Labs, All rights reserved.
# See the file COPYING for details.
#
# Exceptions
#
# Authors: Soumya Basu, Emin Gun Sirer
#

class ParseError (Exception):
    def __init__ (self, msg):
        self.msg = msg

class UnrecognizedCommandError(ParseError):
    def __init__ (self, msg, raw_data):
        self.msg = msg
        self.raw_data = raw_data

class PayloadLenError (ParseError):
    def __init__ (self, msg):
        ParseError.__init__(self, msg)

class TerminationError (Exception):
    pass

