# -*- coding: utf-8 -*-


class AwordError(Exception):
    status_code = 400

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class AwordPermissionError(AwordError):
    status_code = 403
