# Copyright 2024 Ole Kliemann
# SPDX-License-Identifier: Apache-2.0

import os
from mrjsonstore import JsonStore, Transaction

import pytest


@pytest.fixture(params=['test.json', 'test.yaml', 'test.yml'])
def filename(request):
    return request.param


def test_no_rollback_no_exception_dry_run(tmp_path, filename):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=True)
    assert store
    store = store.unwrap()
    with store.transaction(rollback=False) as t:
        assert len(store.content) == 0

        store.content['foo'] = 'bar'
        store.content['nested'] = {'baz': 123}
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    with store.transaction(rollback=False) as t:
        assert len(store.content) == 2
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
        store.content['nested']['baz'] = 321
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    store_ = JsonStore(filename, dry_run=True)
    assert store_
    store_ = store_.unwrap()

    with store_.transaction(rollback=False) as t:
        assert len(store_.content) == 0
        store_.content['nested'] = {}
        store_.content['nested']['baz'] = 1234
        store_.content['nested']['bar'] = 123
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    store__ = JsonStore(filename)
    assert store__
    store__ = store__.unwrap()

    assert len(store__.content) == 0


def test_rollback_no_exception_dry_run(tmp_path, filename):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=True)
    assert store
    store = store.unwrap()
    with store.transaction(rollback=True) as t:
        assert len(store.content) == 0

        store.content['foo'] = 'bar'
        store.content['nested'] = {'baz': 123}
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    with store.transaction(rollback=True) as t:
        assert len(store.content) == 2
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
        store.content['nested']['baz'] = 321
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    store_ = JsonStore(filename, dry_run=True)
    assert store_
    store_ = store_.unwrap()

    with store_.transaction(rollback=True) as t:
        assert len(store_.content) == 0
        store_.content['nested'] = {}
        store_.content['nested']['baz'] = 1234
        store_.content['nested']['bar'] = 123
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    store__ = JsonStore(filename, dry_run=True)
    assert store__
    store__ = store__.unwrap()

    assert len(store__.content) == 0


def test_no_rollback_exception_dry_run(tmp_path, filename):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=True)
    assert store
    store = store.unwrap()
    try:
        with store.transaction(rollback=False) as t:
            assert len(store.content) == 0

            store.content['foo'] = 'bar'
            store.content['nested'] = {'baz': 123}
            assert store.content['foo'] == 'bar'
            assert store.content['nested']['baz'] == 123
            raise RuntimeError()
    except RuntimeError:
        pass
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    with store.transaction(rollback=False) as t:
        assert len(store.content) == 2
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
        store.content['nested']['baz'] = 321
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    store_ = JsonStore(filename, dry_run=True)
    assert store_
    store_ = store_.unwrap()

    try:
        with store_.transaction(rollback=False) as t:
            assert len(store_.content) == 0
            store_.content['nested'] = {}
            store_.content['nested']['baz'] = 1234
            store_.content['nested']['bar'] = 123
            raise RuntimeError()
    except RuntimeError:
        pass
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    store__ = JsonStore(filename)
    assert store__
    store__ = store__.unwrap()

    assert len(store__.content) == 0


def test_rollback_exception_dry_run(tmp_path, filename):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=True)
    assert store
    store = store.unwrap()
    try:
        with store.transaction(rollback=True) as t:
            assert len(store.content) == 0

            store.content['foo'] = 'bar'
            store.content['nested'] = {'baz': 123}
            assert store.content['foo'] == 'bar'
            assert store.content['nested']['baz'] == 123
            raise RuntimeError()
    except RuntimeError:
        pass
    assert t.result
    assert t.result.unwrap() == Transaction.State.Rolledback

    try:
        with store.transaction(rollback=True) as t:
            assert len(store.content) == 0
            store.content['nested'] = {}
            store.content['nested']['baz'] = 321
            assert store.content['nested']['baz'] == 321
            raise RuntimeError()
    except RuntimeError:
        pass
    assert t.result
    assert t.result.unwrap() == Transaction.State.Rolledback

    store_ = JsonStore(filename, dry_run=True)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_no_rollback_exception_modifying_existing_data_dry_run(tmp_path, filename):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename)
    assert store
    store = store.unwrap()
    with store.transaction() as t:
        assert len(store.content) == 0

        store.content['foo'] = 'bar'
        store.content['nested'] = {'baz': 123}
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    store = JsonStore(filename, dry_run=True)
    assert store
    store = store.unwrap()
    try:
        with store.transaction(rollback=False) as t:
            assert len(store.content) == 2
            assert store.content['foo'] == 'bar'
            assert store.content['nested']['baz'] == 123
            store.content['nested']['baz'] = 321
            raise RuntimeError()
    except RuntimeError as e:
        pass
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 321

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 2
    assert store_.content['foo'] == 'bar'
    assert store_.content['nested']['baz'] == 123


def test_rollback_exception_modifying_existing_data_dry_run(tmp_path, filename):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename)
    assert store
    store = store.unwrap()
    with store.transaction() as t:
        assert len(store.content) == 0

        store.content['foo'] = 'bar'
        store.content['nested'] = {'baz': 123}
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    store = JsonStore(filename, dry_run=True)
    assert store
    store = store.unwrap()
    try:
        with store.transaction(rollback=True) as t:
            assert len(store.content) == 2
            assert store.content['foo'] == 'bar'
            assert store.content['nested']['baz'] == 123
            store.content['nested']['baz'] = 321
            raise RuntimeError()
    except RuntimeError as e:
        pass
    assert t.result
    assert t.result.unwrap() == Transaction.State.Rolledback

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 2
    assert store_.content['foo'] == 'bar'
    assert store_.content['nested']['baz'] == 123
