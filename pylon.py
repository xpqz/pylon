"""A minimal client library for Cloudant

Author: Stefan Kruger, IBM Cloudant
"""
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

    def __init__(self, url, username, password):
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.url = urlsplit(url)

    def request(self, method, urlstr, params={}, headers={}, json={}, throw=True):
        r = self.session.request(method, urlstr, params=params, headers=headers, json=json)
    
        if throw:
            r.raise_for_status()

        return r

    def read_doc(self, database, docid, **kwargs):
        return self.request('GET', endpoint(self.url, path(database, '/'+docid)), params=kwargs).json()

    def bulk_docs(self, database, data=[], **kwargs):
        return self.request('POST', endpoint(self.url, path(database, '/_bulk_docs')), json={"docs": data}, params=kwargs).json()

    def create_doc(self, database, data={}):
        if isinstance(data, list):
            return self.bulk_docs(database, data)

        response = self.bulk_docs(database, [data])
        return response[0]

    def update_doc(self, database, docid, revid, body={}):
        body.update({'_id':docid, '_rev':revid})
        return self.bulk_docs(database, [body])[0]

    def delete_doc(self, database, docid, revid):
        return self.update_doc(database, docid, revid, {'_deleted':True})

    def view_query(self, database, ddoc, viewname, **kwargs):
        urlstr = endpoint(self.url, path(database, "/_design/{0}/_view/{1}".format(ddoc, viewname)))

        keys = kwargs.pop('keys', None)
        if keys:    
            return self.request('POST', urlstr, json={'keys': keys}, **kwargs).json()

        return self.request('GET', urlstr, kwargs).json()

    def all_docs(self, database, **kwargs):
        urlstr = endpoint(self.url, path(database, "/_all_docs"))
        keys = kwargs.pop('keys', None)
        if keys:    
            return self.request('POST', urlstr, json={'keys': keys}, **kwargs).json()

        key = kwargs.pop('key', None)
        if key:    
            return self.request('POST', urlstr, json={'keys': [key]}, **kwargs).json()

        return self.request('GET', urlstr, kwargs).json()

    def create_database(self, database):
        r = self.request('PUT', endpoint(self.url, '/'+database), throw=False)

        if r.status_code == requests.codes.ok:
            return (r.json(), True)

        if r.status_code == 412: # Database already present
            return (r.json(), False)

        r.raise_for_status

    def list_databases(self):
        return self.request('GET', endpoint(self.url, '/_all_dbs')).json()

    def delete_database(self, database):
        return self.request('DELETE', endpoint(self.url, '/'+database)).json()

    def database_info(self, database):
        return self.request('GET', endpoint(self.url, '/'+database)).json()
