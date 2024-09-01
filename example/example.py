from mrjsonstore import JsonStore

def write():
    store = JsonStore('example.json')
    with store() as x:
        assert isinstance(x, dict)
        x['woohoo'] = 'I am just a Python dictionary'

def read():
    store = JsonStore('example.json')
    with store() as x:
        assert x['woohoo'] == 'I am just a Python dictionary'

write()
read()
