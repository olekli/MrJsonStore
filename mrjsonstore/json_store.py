# Copyright 2024 Ole Kliemann
# SPDX-License-Identifier: Apache-2.0

import os
import yaml
import json
from enum import Enum
from typing import Optional, Callable, NewType
from atomicwrites import atomic_write
from drresult import returns_result, constructs_as_result, noexcept, gather_result, Ok, Err, Result

json_serialiser: Callable[[dict], str] = lambda x: json.dumps(x)
yaml_serialiser: Callable[[dict], str] = lambda x: yaml.safe_dump(x)

json_deserialiser: Callable[[str], dict] = lambda x: json.loads(x)
yaml_deserialiser: Callable[[str], dict] = lambda x: yaml.safe_load(x)


@constructs_as_result
class JsonStore:
    def __init__(self, filename: str, dry_run: bool = False):
        self._filename = filename
        self._dry_run = dry_run
        self._content: dict = {}
        self._current_transaction: Optional['Transaction'] = None
        _, extension = os.path.splitext(self._filename)
        if extension == '.yaml' or extension == '.yml':
            self._serialiser = yaml_serialiser
            self._deserialiser = yaml_deserialiser
        else:
            self._serialiser = json_serialiser
            self._deserialiser = json_deserialiser
        if os.path.exists(self._filename):
            with open(self._filename) as f:
                self._content = self._deserialiser(f.read())

    @property
    def content(self) -> dict:
        return self._content

    @property
    def current_transaction(self) -> Optional['Transaction']:
        return self._current_transaction

    @noexcept
    def transaction(self, rollback: bool = True) -> 'Transaction':
        assert not self._current_transaction or not self._current_transaction._active
        self._current_transaction = Transaction(self, rollback=rollback)
        return self._current_transaction

    @noexcept
    def rollback(self) -> None:
        assert self._current_transaction and self._current_transaction._active
        self._current_transaction.rollback()
        self._current_transaction = None

    @returns_result
    def commit(self) -> Result['Transaction.State']:
        if not self._current_transaction or not self._current_transaction._active:
            transaction = Transaction(self, rollback=False)
        else:
            transaction = self._current_transaction
            self._current_transaction = None
        return transaction.commit()


class Transaction:
    State = Enum('State', ['Active', 'Committed', 'Rolledback'])

    def __init__(self, store: JsonStore, rollback: bool):
        self._store = store
        self._rollback: Optional[str] = None
        if rollback:
            self._rollback = json.dumps(store._content)
        self._active: bool = True
        self._result: Result[Transaction.State] = Err(ValueError(Transaction.State.Active))

    @property
    def active(self) -> bool:
        return self._active

    @property
    def result(self) -> Result['Transaction.State']:
        return self._result

    @noexcept
    def __enter__(self) -> 'Transaction':
        assert self._active
        return self

    @noexcept
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        assert self._active
        if exc_type and self._rollback:
            self.rollback()
        else:
            self._result = self.commit()

    @returns_result
    def commit(self) -> Result['Transaction.State']:
        assert self._active
        self._active = False
        with gather_result() as result:
            if not self._store._dry_run:
                with atomic_write(self._store._filename, overwrite=True) as f:
                    f.write(self._store._serialiser(self._store._content))
            result.set(Ok(Transaction.State.Committed))
        self._result = result.get()
        return self._result

    @noexcept
    def rollback(self) -> None:
        assert self._active and self._rollback
        self._store._content.clear()
        self._store._content.update(json.loads(self._rollback))
        self._rollback = None
        self._active = False
        self._result = Ok(Transaction.State.Rolledback)
