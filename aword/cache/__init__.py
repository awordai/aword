# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod


class Cache(ABC):

    @abstractmethod
    def get_last_edited_timestamp(self, source, source_unit_id):
        """
        This method should be implemented by any class that extends Cache.
        It should return the last seen timestamp for the provided source and source_unit_id.
        """
        pass
