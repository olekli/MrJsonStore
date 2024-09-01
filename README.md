# mrjsonstore
Simple, transparent on-disk JSON store using `atomicwrites`.

```python
store = JsonStore('example.json')
with store() as x:
    assert isinstance(x, dict)
    x['woohoo'] = 'I am just a Python dictionary'
```

Changes are written on context exit, regardless of exceptions that occurred.

Unless a transaction is used:

```python
with store.transaction() as x:
  [ ... ]
  raise RuntimeError()
```

In that case any changes are rolled back on context exit.

Does not yet support concurrency.
