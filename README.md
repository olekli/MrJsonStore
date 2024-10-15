# mrjsonstore
Simple, transparent on-disk JSON store using `atomicwrites`.

## Basic Usage

### Without context

```python
store = JsonStore('example.json')
assert isinstance(store.content, dict)
store.content['woohoo'] = 'I am just a Python dictionary'
result = store.commit()
if not result:
    print(f'There was a problem when writing: {result})
```

You can also use transactions...

```python
store = JsonStore('example.json')
t = store.transaction()
store.content['woohoo'] = 'I am just a Python dictionary'
result = t.commit()
if not result:
    print(f'There was a problem when writing: {result})
```

... and possibly roll them back:

```python
store = JsonStore('example.json')
store.content['woohoo'] = 'I am just a Python dictionary'
t = store.transaction()
store.content['woohoo'] = 'I am going to be rolled back!'
t.rollback()
assert store.content['woohoo'] == 'I am just a Python dictionary'
```

### With context

```python
store = JsonStore('example.json')
with store.transaction() as t:
    store.content['woohoo'] = 'I am just a Python dictionary'
```

Changes will be committed on context exit, unless there is an exception:

```python
store = JsonStore('example.json')
store.content['woohoo'] = 'I am just a Python dictionary'
with store.transaction() as t:
    store.content['woohoo'] = 'I am going to be rolled back!'
    raise RuntimeError()
[...]
assert store.content['woohoo'] == 'I am just a Python dictionary'
```

If you want to commit regardless of exceptions, you can choose not to rollback:

```python
store = JsonStore('example.json')
store.content['woohoo'] = 'I am just a Python dictionary'
with store.transaction(rollback=False) as t:
    store.content['woohoo'] = 'I am not going to be rolled back!'
    raise RuntimeError()
[...]
assert store.content['woohoo'] == 'I am not going to be rolled back!'
```

Changes will be committed to disk then.

## TODO

* Add support for concurrency?
