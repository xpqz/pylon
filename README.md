## Pylon

`Pylon` is a minimal client library for [Cloudant](https://www.cloudant.com), written in [Python](https://www.python.org/). It isn't a replacement for Cloudant's fine official [library](https://github.com/cloudant/python-cloudant), but intended as a demonstration of some base line principles I consider good practice for client libraries wrapping a complex API.

* As thin as possible level of abstraction
* Knowledge of underlying API required
* Hide json and http, but nothing else
* Where multiple ways of doing someghing in the API exist, choose one
* Implement only a minimal subset of the backing API but provide easy access to the underlying session

### Install

`Pylon` only depends on [requests](http://docs.python-requests.org/en/master/).

    virtualenv --no-site-packages env
    . env/bin/activate
    pip install -r requirements.txt

### To use

Check out the file `tests.py`.

    from pylon import Cloudant

    database = '...'
    cdt = Cloudant('https://name.cloudant.com', username, password)
    cdt.create_database(database)

    result = cdt.create_doc(database, {'name': 'bob'})

    data = cdt.all_docs(database)
