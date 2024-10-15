# Copyright 2024 Ole Kliemann
# SPDX-License-Identifier: Apache-2.0

import os
from drresult import Panic
from mrjsonstore import JsonStore, Transaction

import pytest


@pytest.fixture(params=['test.json', 'test.yaml', 'test.yml'])
def filename(request):
    return request.param


@pytest.fixture(params=['broken.json', 'broken.yaml'])
def broken_filename(request):
    return request.param


@pytest.fixture(params=[True, False])
def dry_run(request):
    return request.param


file_content = {
    'test.json': r'''
{
    "foo": {
        "bar": "baz"
    }
}
''',
    'test.yaml': r'''
foo:
    bar: baz
''',
    'test.yml': r'''
foo:
    bar: baz
''',
    'broken.json': r'''
    "foo": {
        "bar": "baz"
    }
}
''',
    'broken.yaml': r'''
%aieatie
''',
}


def test_no_rollback_no_exception(tmp_path, filename):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename)
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

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    with store_.transaction(rollback=False) as t:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 321
        store_.content['nested']['baz'] = 1234
        store_.content['nested']['bar'] = 123
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    store__ = JsonStore(filename)
    assert store__
    store__ = store__.unwrap()

    assert len(store__.content) == 2
    assert store__.content['foo'] == 'bar'
    assert store__.content['nested']['baz'] == 1234
    assert store__.content['nested']['bar'] == 123


def test_rollback_no_exception(tmp_path, filename):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename)
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

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    with store_.transaction(rollback=True) as t:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 321
        store_.content['nested']['baz'] = 1234
        store_.content['nested']['bar'] = 123
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    store__ = JsonStore(filename)
    assert store__
    store__ = store__.unwrap()

    assert len(store__.content) == 2
    assert store__.content['foo'] == 'bar'
    assert store__.content['nested']['baz'] == 1234
    assert store__.content['nested']['bar'] == 123


def test_no_rollback_exception(tmp_path, filename):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename)
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

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    try:
        with store_.transaction(rollback=False) as t:
            assert len(store_.content) == 2
            assert store_.content['foo'] == 'bar'
            assert store_.content['nested']['baz'] == 321
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

    assert len(store__.content) == 2
    assert store__.content['foo'] == 'bar'
    assert store__.content['nested']['baz'] == 1234
    assert store__.content['nested']['bar'] == 123


def test_rollback_exception(tmp_path, filename):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename)
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

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_no_rollback_exception_modifying_existing_data(tmp_path, filename):
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
    assert store_.content['nested']['baz'] == 321


def test_rollback_exception_modifying_existing_data(tmp_path, filename):
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


def test_read_existing_file(tmp_path, filename, dry_run):
    filepath = os.path.join(tmp_path, filename)
    with open(filepath, 'w') as f:
        f.write(file_content[filename])

    store = JsonStore(filepath, dry_run=dry_run)
    assert store
    store = store.unwrap()
    assert store.content == {'foo': {'bar': 'baz'}}


def test_read_existing_file_broken(tmp_path, broken_filename, dry_run):
    filepath = os.path.join(tmp_path, broken_filename)
    with open(filepath, 'w') as f:
        f.write(file_content[broken_filename])

    store = JsonStore(filepath, dry_run=dry_run)
    assert not store


def test_no_rollback_no_exception_invalid_file(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, 'invalid', 'non', 'existing', 'path', filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    with store.transaction(rollback=False) as t:
        assert len(store.content) == 0

        store.content['foo'] = 'bar'
        store.content['nested'] = {'baz': 123}
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
    assert (dry_run and t.result) or not t.result

    with store.transaction(rollback=False) as t:
        assert len(store.content) == 2
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
        store.content['nested']['baz'] = 321
    assert (dry_run and t.result) or not t.result

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()
    assert len(store_.content) == 0


def test_rollback_no_exception_invalid_file(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, 'invalid', 'non', 'existing', 'path', filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    with store.transaction(rollback=True) as t:
        assert len(store.content) == 0

        store.content['foo'] = 'bar'
        store.content['nested'] = {'baz': 123}
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
    assert (dry_run and t.result) or not t.result

    with store.transaction(rollback=True) as t:
        assert len(store.content) == 2
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
        store.content['nested']['baz'] = 321
    assert (dry_run and t.result) or not t.result

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_no_rollback_exception_invalid_file(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, 'invalid', 'non', 'existing', 'path', filename)
    store = JsonStore(filename, dry_run=dry_run)
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
    assert (dry_run and t.result) or not t.result

    with store.transaction(rollback=False) as t:
        assert len(store.content) == 2
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
        store.content['nested']['baz'] = 321
    assert (dry_run and t.result) or not t.result

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()
    assert len(store_.content) == 0


def test_rollback_exception_invalid_file(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, 'invalid', 'non', 'existing', 'path', filename)
    store = JsonStore(filename, dry_run=dry_run)
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

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_no_rollback_exception_modifying_existing_data_invalid_file(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, 'invalid', 'non', 'existing', 'path', filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    with store.transaction() as t:
        assert len(store.content) == 0

        store.content['foo'] = 'bar'
        store.content['nested'] = {'baz': 123}
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
    assert (dry_run and t.result) or not t.result

    try:
        with store.transaction(rollback=False) as t:
            assert len(store.content) == 2
            assert store.content['foo'] == 'bar'
            assert store.content['nested']['baz'] == 123
            store.content['nested']['baz'] = 321
            raise RuntimeError()
    except RuntimeError as e:
        pass
    assert (dry_run and t.result) or not t.result

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 321

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_rollback_exception_modifying_existing_data_invalid_path(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, 'invalid', 'non', 'existing', 'path', filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    with store.transaction() as t:
        assert len(store.content) == 0

        store.content['foo'] = 'bar'
        store.content['nested'] = {'baz': 123}
        assert store.content['foo'] == 'bar'
        assert store.content['nested']['baz'] == 123
    assert (dry_run and t.result) or not t.result

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

    assert len(store_.content) == 0


def test_without_contextmanager_no_rollback_commits(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=False)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = t.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_rollback_commits(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = t.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_rollback_rollsback(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    t.rollback()
    assert t.result
    assert t.result.unwrap() == Transaction.State.Rolledback

    assert len(store.content) == 0

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_without_contextmanager_no_rollback_cannot_commit_twice(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=False)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = t.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        t.commit()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_rollback_cannot_commit_twice(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = t.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        t.commit()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_no_rollback_cannot_rollback(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=False)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        t.rollback()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_without_contextmanager_no_rollback_no_write_without_commit(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=False)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_without_contextmanager_rollback_no_write_without_commit(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_without_contextmanager_no_transaction_commits(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = store.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_no_transaction_no_write_without_commit(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_without_contextmanager_no_transaction_can_commit_twice(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = store.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    store.commit()

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_no_transaction_cannot_rollback(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        store.rollback()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_without_contextmanager_no_rollback_commits_on_store(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    store.transaction(rollback=False)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = store.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_rollback_commits_on_store(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = store.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_rollback_rollsback_on_store(tmp_path, filename, dry_run):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    store.rollback()

    assert len(store.content) == 0

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_without_contextmanager_no_rollback_cannot_rollback_on_transaction_after_commit_on_transaction(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=False)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = t.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        t.rollback()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_rollback_cannot_rollback_on_transaction_after_commit_on_transaction(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = t.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed
    assert t.result
    assert t.result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        t.rollback()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_no_rollback_cannot_rollback_on_transaction_after_commit_on_store(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=False)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = store.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        t.rollback()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_rollback_cannot_rollback_on_transaction_after_commit_on_store(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = store.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        t.rollback()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_no_rollback_cannot_rollback_on_store_after_commit_on_transaction(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=False)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = t.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        store.rollback()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_rollback_cannot_rollback_on_store_after_commit_on_transaction(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = t.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        store.rollback()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_no_rollback_cannot_rollback_on_store_after_commit_on_store(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=False)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = store.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        store.rollback()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_rollback_cannot_rollback_on_store_after_commit_on_store(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = store.commit()
    assert result
    assert result.unwrap() == Transaction.State.Committed

    assert len(store.content) == 2
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123

    with pytest.raises(Panic):
        store.rollback()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    if dry_run:
        assert len(store_.content) == 0
    else:
        assert len(store_.content) == 2
        assert store_.content['foo'] == 'bar'
        assert store_.content['nested']['baz'] == 123


def test_without_contextmanager_rollback_cannot_commit_on_transaction_after_rollback_on_transaction(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    t.rollback()
    assert t.result
    assert t.result.unwrap() == Transaction.State.Rolledback

    assert len(store.content) == 0

    with pytest.raises(Panic):
        t.commit()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_without_contextmanager_rollback_cannot_commit_on_transaction_after_rollback_on_store(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    store.rollback()

    assert len(store.content) == 0

    with pytest.raises(Panic):
        t.commit()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_without_contextmanager_rollback_can_commit_on_store_after_rollback_on_transaction(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    t.rollback()
    assert t.result
    assert t.result.unwrap() == Transaction.State.Rolledback

    assert len(store.content) == 0

    store.commit()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0


def test_without_contextmanager_rollback_can_commit_on_store_after_rollback_on_store(
    tmp_path, filename, dry_run
):
    filename = os.path.join(tmp_path, filename)
    store = JsonStore(filename, dry_run=dry_run)
    assert store
    store = store.unwrap()
    t = store.transaction(rollback=True)
    store.content['foo'] = 'bar'
    store.content['nested'] = {'baz': 123}
    assert store.content['foo'] == 'bar'
    assert store.content['nested']['baz'] == 123
    result = store.rollback()

    assert len(store.content) == 0

    store.commit()

    store_ = JsonStore(filename)
    assert store_
    store_ = store_.unwrap()

    assert len(store_.content) == 0
