# Copyright 2024 Ole Kliemann
# SPDX-License-Identifier: Apache-2.0

import json
from atomicwrites import atomic_write

class Handle:
    def __init__(self, store, is_transaction, dry_run):
        self.store = store
        self.is_transaction = is_transaction
        self.dry_run = dry_run
        self.rollback = None

    def __enter__(self):
        if self.is_transaction:
            self.rollback = json.dumps(self.store.content)
        return self.store.content

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type or not self.is_transaction:
            if not self.dry_run:
                with atomic_write(self.store.filename, overwrite=True) as f:
                    f.write(json.dumps(self.store.content))
        else:
            self.store.content.clear()
            self.store.content.update(json.loads(self.rollback))
