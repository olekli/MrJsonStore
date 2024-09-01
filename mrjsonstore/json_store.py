# Copyright 2024 Ole Kliemann
# SPDX-License-Identifier: Apache-2.0

import os
import json
from types import MappingProxyType
from result import as_result
from mrjsonstore.handle import Handle

class JsonStore:
    @staticmethod
    @as_result(
        FileNotFoundError,
        PermissionError,
        IsADirectoryError,
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError
    )
    def make(filename):
        return JsonStore(filename)

    def __init__(self, filename):
        self.filename = filename
        self.content = {}
        if os.path.exists(self.filename):
          with open(self.filename) as f:
              self.content = json.loads(f.read())

    def __call__(self):
        return Handle(self, False)

    def transaction(self):
        return Handle(self, True)

    def view(self):
        return MappingProxyType(self.content)
