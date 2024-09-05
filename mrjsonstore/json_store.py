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
    def make(filename, dry_run=False):
        return JsonStore(filename, dry_run)

    def __init__(self, filename, dry_run=False):
        self.filename = filename
        self.dry_run = dry_run
        self.content = {}
        if os.path.exists(self.filename):
          with open(self.filename) as f:
              self.content = json.loads(f.read())

    def __call__(self):
        return Handle(self, is_transaction=False, dry_run=self.dry_run)

    def transaction(self):
        return Handle(self, is_transaction=True, dry_run=self.dry_run)

    def view(self):
        return json.loads(json.dumps(self.content))
