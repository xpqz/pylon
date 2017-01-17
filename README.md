## Pylon

`Pylon` is a minimal client library for Cloudant, written in Python. It isn't a replacement for
Cloudant's fine official library, but intended as a demonstration of some base line principles 
I consider good practice for client libraries wrapping a complex API.

### Install

`Pylon` only depends on `requests`.

    virtualenv --no-site-packages env
    . env/bin/activate
    pip install -r requirements.txt```

### To use

Check out the file `tests.py`.

    from pylon import Cloudant

    database = '...'
    cdt = Cloudant('https://name.cloudant.com', username, password)
    cdt.create_database(database)

    result = cdt.create_doc(database, {'name': 'bob'})

    data = cdt.all_docs(database)
