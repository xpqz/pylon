# -*- coding: utf-8 -*-

"""A minimal client library for Cloudant

Author: Stefan Kruger, IBM Cloudant
"""
__docformat__ = 'reStructuredText'

import requests
import json
from urlparse import urlsplit, urlunsplit

def path(dbname, pathstr):
    return "/{0}{1}".format(dbname, pathstr)

def endpoint(url, path):
    new_url = list(url)
    new_url[2] = path

    return urlunsplit(new_url)

class Cloudant:
    """
    The Cloudant class represents an authenticated connection to a remote CouchDB/Cloudant instance.
    """
    def __init__(self, url, username, password):
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.url = urlsplit(url)

    def request(self, method, urlstr, params={}, headers={}, json={}, throw=True):
        """
        HTTP request using the authenticated session object. `throw=True` raises exceptions
        on status >= 400

        This can be used to target API endpoints not yet implemented by Pylon:

            >>> result = remote.request('GET', endpoint(remote.url, path('directory', '/_changes'))).json()
        """
        r = self.session.request(method, urlstr, params=params, headers=headers, json=json)
    
        if throw:
            r.raise_for_status()

        return r

    def read_doc(self, database, docid, **kwargs):
        """
        Fetch a document from the primary index by `docid` 

            >>> doc = remote.read_doc('animaldb', '7a36cbc16e43e362e1ae68861aa06c0f', 
                    rev='1-5a05c08a7a601c49c8bc344d77e23020')

        See http://docs.couchdb.org/en/1.6.1/api/document/common.html#get--db-docid
        """
        return self.request('GET', endpoint(self.url, path(database, '/'+docid)), params=kwargs).json()

    def bulk_docs(self, database, data=[], **kwargs):
        """
        Raw _bulk_docs.

        This is a function primarily intended for internal use, but can
        be used directly to create, update or delete documents in bulk,
        so as to save on the HTTP overhead.

        Note that many other methods are implemented in terms of `bulk_docs`.

            >>> result = remote.bulk_docs('directory', [{'name':'alice'}, {'name':'bob'}])

        See http://docs.couchdb.org/en/1.6.1/api/database/bulk-api.html#post--db-_bulk_docs
        """
        return self.request('POST', endpoint(self.url, path(database, '/_bulk_docs')), json={"docs": data}, params=kwargs).json()

    def create_doc(self, database, data={}):
        """
        Create one or more new documents. If `data` is a dict, this is considered to be the body
        of a single document. If `data` is a list of dicts, these are considered to be a document set.

        Note that this is implemented via the `_bulk_docs` endpoint, rather 
        than a `POST` to `/{database}`.

            >>> result = remote.create_doc('directory', {'name':'alice'})

        See http://docs.couchdb.org/en/1.6.1/api/database/bulk-api.html#post--db-_bulk_docs
        """
        if isinstance(data, list):
            return self.bulk_docs(database, data)

        response = self.bulk_docs(database, [data])
        return response[0]

    def update_doc(self, database, docid, revid, body={}):
        """
        Update an existing document, creating a new revision.

        Implemented via the _bulk_docs endpoint.

            >>> result = remote.update_doc('directory', '7a36cbc16e43e362e1ae68861aa06c0f', 
                    '1-5a05c08a7a601c49c8bc344d77e23020', {'name':'alice', 'phone':'07865432236'})

        See http://docs.couchdb.org/en/1.6.1/api/database/bulk-api.html#post--db-_bulk_docs
        """
        body.update({'_id':docid, '_rev':revid})
        return self.bulk_docs(database, [body])[0]

    def delete_doc(self, database, docid, revid):
        """
        Delete a document revision. Implemented via the _bulk_docs endpoint.

            >>> result = remote.delete_doc('directory', '7a36cbc16e43e362e1ae68861aa06c0f', 
                    '1-5a05c08a7a601c49c8bc344d77e23020')

        See http://docs.couchdb.org/en/1.6.1/api/database/bulk-api.html#post--db-_bulk_docs
        """
        return self.update_doc(database, docid, revid, {'_deleted':True})

    def view_query(self, database, ddoc, viewname, **kwargs):
        """
        Query a secondary index.

        Example: 
            >>> cloudant.view_query(db, "my_ddoc", "my_view", keys=["alice", "bob"])

        Returns:
            >>> {
              "rows": [
                {"key" => "alice", "id" => "591c02fa8b8ff14dd4c0553670cc059a", "value" => 1},
                {"key" => "bob", "id" => "591c02fa8b8ff14dd4c0553670cc13c1", "value" => 1}
              ],
              "offset": 0,
              "total_rows": 7 
            )

        See https://docs.cloudant.com/using_views.html
        """
        urlstr = endpoint(self.url, path(database, "/_design/{0}/_view/{1}".format(ddoc, viewname)))

        keys = kwargs.pop('keys', None)
        if keys:    
            return self.request('POST', urlstr, json={'keys': keys}, **kwargs).json()

        return self.request('GET', urlstr, kwargs).json()

    def all_docs(self, database, **kwargs):
        """
        Query the primary index.

            >>> docs = remote.all_docs('directory', keys=[
                '7a36cbc16e43e362e1ae68861aa06c0f',
                '7a36cbc16e43e362e1ae68861aa06da1'
                ])

        See http://docs.couchdb.org/en/1.6.1/api/database/bulk-api.html#db-all-docs
        """
        urlstr = endpoint(self.url, path(database, "/_all_docs"))
        keys = kwargs.pop('keys', None)
        if keys:    
            return self.request('POST', urlstr, json={'keys': keys}, **kwargs).json()

        key = kwargs.pop('key', None)
        if key:    
            return self.request('POST', urlstr, json={'keys': [key]}, **kwargs).json()

        return self.request('GET', urlstr, kwargs).json()

    def create_database(self, database):
        """
        Create a new database on the remote end called `database`. Returns the response body
        and a boolean which is true if a database was created, false if it already existed.

            >>> (result, created) = remote.create_database('directory')

        See http://docs.couchdb.org/en/1.6.1/CouchDB/database/common.html#put--db
        """
        r = self.request('PUT', endpoint(self.url, '/'+database), throw=False)

        if r.status_code == requests.codes.ok:
            return (r.json(), True)

        if r.status_code == 412: # Database already present
            return (r.json(), False)

        r.raise_for_status

    def list_databases(self):
        """
        Return a list of all databases under the authenticated user.

            >>> result = remote.list_databases()

        See http://docs.couchdb.org/en/1.6.1/CouchDB/server/common.html#all-dbs
        """
        return self.request('GET', endpoint(self.url, '/_all_dbs')).json()

    def delete_database(self, database):
        """
        Delete `database`.

            >>> result = remote.delete_database('directory')

        See http://docs.couchdb.org/en/1.6.1/CouchDB/database/common.html#delete--db
        """
        return self.request('DELETE', endpoint(self.url, '/'+database)).json()

    def database_info(self, database):
        """
        Return the metadata about `database`.

            >>> result = remote.database_info('directory')

        See http://docs.couchdb.org/en/1.6.1/CouchDB/database/common.html#get--db
        """
        return self.request('GET', endpoint(self.url, '/'+database)).json()
