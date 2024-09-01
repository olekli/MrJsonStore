# Copyright 2024 Ole Kliemann
# SPDX-License-Identifier: Apache-2.0

import os
from result import is_ok
from mrjsonstore import JsonStore

def test_no_transaction_no_exception(tmp_path):
    filename = os.path.join(tmp_path, 'test.json')
    store = JsonStore.make(filename)
    assert is_ok(store)
    store = store.unwrap()
    with store() as x:
        assert len(x) == 0

        x['foo'] = 'bar'
        x['nested'] = { 'baz': 123 }
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 123

    with store() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 123
        x['nested']['baz'] = 321

    store_ = JsonStore.make(filename)
    assert is_ok(store_)
    store_ = store_.unwrap()

    with store_() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 321
        x['nested']['baz'] = 1234
        x['nested']['bar'] = 123

    store__ = JsonStore.make(filename)
    assert is_ok(store__)
    store__ = store__.unwrap()

    with store__() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 1234
        assert x['nested']['bar'] == 123

def test_transaction_no_exception(tmp_path):
    filename = os.path.join(tmp_path, 'test.json')
    store = JsonStore.make(filename)
    assert is_ok(store)
    store = store.unwrap()
    with store.transaction() as x:
        assert len(x) == 0

        x['foo'] = 'bar'
        x['nested'] = { 'baz': 123 }
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 123

    with store.transaction() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 123
        x['nested']['baz'] = 321

    store_ = JsonStore.make(filename)
    assert is_ok(store_)
    store_ = store_.unwrap()

    with store_.transaction() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 321
        x['nested']['baz'] = 1234
        x['nested']['bar'] = 123

    store__ = JsonStore.make(filename)
    assert is_ok(store__)
    store__ = store__.unwrap()

    with store__() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 1234
        assert x['nested']['bar'] == 123

def test_no_transaction_exception(tmp_path):
    filename = os.path.join(tmp_path, 'test.json')
    store = JsonStore.make(filename)
    assert is_ok(store)
    store = store.unwrap()
    try:
        with store() as x:
            assert len(x) == 0

            x['foo'] = 'bar'
            x['nested'] = { 'baz': 123 }
            assert x['foo'] == 'bar'
            assert x['nested']['baz'] == 123
            raise RuntimeError()
    except RuntimeError as e:
        pass

    try:
        with store() as x:
            assert len(x) == 2
            assert x['foo'] == 'bar'
            assert x['nested']['baz'] == 123
            x['nested']['baz'] = 321
            raise RuntimeError()
    except RuntimeError as e:
        pass

    store_ = JsonStore.make(filename)
    assert is_ok(store_)
    store_ = store_.unwrap()

    try:
        with store_() as x:
            assert len(x) == 2
            assert x['foo'] == 'bar'
            assert x['nested']['baz'] == 321
            x['nested']['baz'] = 1234
            x['nested']['bar'] = 123
            raise RuntimeError()
    except RuntimeError as e:
        pass

    store__ = JsonStore.make(filename)
    assert is_ok(store__)
    store__ = store__.unwrap()

    with store__() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 1234
        assert x['nested']['bar'] == 123

def test_transaction_exception(tmp_path):
    filename = os.path.join(tmp_path, 'test.json')
    store = JsonStore.make(filename)
    assert is_ok(store)
    store = store.unwrap()
    try:
        with store.transaction() as x:
            assert len(x) == 0

            x['foo'] = 'bar'
            x['nested'] = { 'baz': 123 }
            assert x['foo'] == 'bar'
            assert x['nested']['baz'] == 123
            raise RuntimeError()
    except RuntimeError as e:
        pass

    with store() as x:
        assert len(x) == 0

    store_ = JsonStore.make(filename)
    assert is_ok(store_)
    store_ = store_.unwrap()

    with store_() as x:
        assert len(x) == 0

def test_no_transaction_exception_modifying_data(tmp_path):
    filename = os.path.join(tmp_path, 'test.json')
    store = JsonStore.make(filename)
    assert is_ok(store)
    store = store.unwrap()
    with store() as x:
        assert len(x) == 0

        x['foo'] = 'bar'
        x['nested'] = { 'baz': 123 }
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 123

    try:
        with store.transaction() as x:
            assert len(x) == 2
            assert x['foo'] == 'bar'
            assert x['nested']['baz'] == 123
            x['nested']['baz'] = 321
            raise RuntimeError()
    except RuntimeError as e:
        pass

    with store() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 123

    store_ = JsonStore.make(filename)
    assert is_ok(store_)
    store_ = store_.unwrap()

    with store_() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 123

def test_view(tmp_path):
    filename = os.path.join(tmp_path, 'test.json')
    store = JsonStore.make(filename)
    assert is_ok(store)
    store = store.unwrap()
    with store() as x:
        assert len(x) == 0

        x['foo'] = 'bar'
        x['nested'] = { 'baz': 123 }
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 123

    with store() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 123
        x['nested']['baz'] = 321

    store_ = JsonStore.make(filename)
    assert is_ok(store_)
    store_ = store_.unwrap()

    with store_() as x:
        assert len(x) == 2
        assert x['foo'] == 'bar'
        assert x['nested']['baz'] == 321
        x['nested']['baz'] = 1234
        x['nested']['bar'] = 123

    store__ = JsonStore.make(filename)
    assert is_ok(store__)
    store__ = store__.unwrap()

    view = store__.view()
    assert len(view) == 2
    assert view['foo'] == 'bar'
    assert view['nested']['baz'] == 1234
    assert view['nested']['bar'] == 123
