class ETHError(Exception):
    def __init__(self, msg):
        super(ETHError, self).__init__(msg)

        self.msg = msg


class WrongMACError(ETHError):
    def __init__(self, msg):
        super(WrongMACError, self).__init__(msg)


class InvalidSignatureError(ETHError):
    def __init__(self, msg):
        super(InvalidSignatureError, self).__init__(msg)


class InvalidKeyError(ETHError):
    def __init__(self, msg):
        super(InvalidKeyError, self).__init__(msg)


class AuthenticationError(ETHError):
    def __init__(self, msg):
        super(AuthenticationError, self).__init__(msg)


class DecryptionError(ETHError):
    def __init__(self, msg):
        super(DecryptionError, self).__init__(msg)


class CipherNotInitializedError(ETHError):
    def __init__(self, msg):
        super(CipherNotInitializedError, self).__init__(msg)
