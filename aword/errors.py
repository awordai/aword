# -*- coding: utf-8 -*-


class AwordError(Exception):
    status_code = 400


class AwordFetchError(AwordError):

    def __init__(self, message: str, response):
        super().__init__('\n'.join([f'{message}',
                                    f'Status code:{response.status_code}',
                                    f'Message: {response.json()["message"]}']))


class AwordPermissionError(AwordError):
    status_code = 403
