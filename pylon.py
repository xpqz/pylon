# -*- coding: utf-8 -*-

__docformat__ = 'reStructuredText'

"""
pylon
~~~~~

A minimal client library for Cloudant/CouchDB

:copyright: (c) 2017 by Stefan Kruger
:license: Apache2, see LICENSE for more details.
"""

import requests
import json
import re
import random
from time import sleep
from urlparse import urlsplit, urlunsplit
from contextlib import closing

def path(dbname, pathstr):
    return "/{0}{1}".format(dbname, pathstr)

def endpoint(url, path):
    new_url = list(url)
    new_url[2] = path

    return urlunsplit(new_url)

class Cloudant(requests.Session):
    """
    The Cloudant class represents an authenticated session to a remote CouchDB/Cloudant instance.

    It supports request retries on 429 responses from Cloudant. 
    """
    def __init__(self, url, username, password, **kwargs):
        requests.Session.__init__(self, **kwargs)
        self.auth = (username, password)
        self.url = urlsplit(url)
        self.max_retries = 5
        self.base_delay = 0.1

    def retry_config(self, max_retries=5, base_delay=0.1):
        """
        Tune the retry parameters for 429 errors. 

        The default is 5 retries with an accumulative delay. Each iteration adds a 
        fixed amount (default 0.1s) plus a little bit of noise (between 0 and 
        9 1/100th of a second) between retries.

        :param max_retries: integer - max attempts when receiving a 429
        :param base_delay: float (seconds) to add to the delay between attempts

        Usage::
            >>> remote.retry_config(10, 0.5)
        """
        self.max_retries = max_retries
        self.base_delay = base_delay

    def request(self, method, urlstr, **kwargs):
        """
        Perform an HTTP request, returning the repsonse. This overrides the method 
        from requests.Session.

        Cloudant will return HTTP status '429: Too Many Requests' if the reserved
        throughput capacity is exceeded. This method will retry a configurable 
        number of times if this happens.

        This method raises a requests.HTTPError for HTTP status code >= 400.

        See https://docs.cloudant.com/http.html#http-status-codes

        :param method: method for the new :class:`Request` object.
        :param url: URL for the new :class:`Request` object.
        :param params: (optional) Dictionary or bytes to be sent in the query string for the :class:`Request`.
        :param data: (optional) Dictionary, bytes, or file-like object to send in the body of the :class:`Request`.
        :param json: (optional) json data to send in the body of the :class:`Request`.
        :param headers: (optional) Dictionary of HTTP Headers to send with the :class:`Request`.
        :param cookies: (optional) Dict or CookieJar object to send with the :class:`Request`.
        :param files: (optional) Dictionary of ``'name': file-like-objects`` (or ``{'name': file-tuple}``) for multipart encoding upload.
            ``file-tuple`` can be a 2-tuple ``('filename', fileobj)``, 3-tuple ``('filename', fileobj, 'content_type')``
            or a 4-tuple ``('filename', fileobj, 'content_type', custom_headers)``, where ``'content-type'`` is a string
            defining the content type of the given file and ``custom_headers`` a dict-like object containing additional headers
            to add for the file.
        :param auth: (optional) Auth tuple to enable Basic/Digest/Custom HTTP Auth.
        :param timeout: (optional) How long to wait for the server to send data
            before giving up, as a float, or a :ref:`(connect timeout, read
            timeout) <timeouts>` tuple.
        :type timeout: float or tuple
        :param allow_redirects: (optional) Boolean. Enable/disable GET/OPTIONS/POST/PUT/PATCH/DELETE/HEAD redirection. Defaults to ``True``.
        :type allow_redirects: bool
        :param proxies: (optional) Dictionary mapping protocol to the URL of the proxy.
        :param verify: (optional) whether the SSL cert will be verified. A CA_BUNDLE path can also be provided. Defaults to ``True``.
        :param stream: (optional) if ``False``, the response content will be immediately downloaded.
        :param cert: (optional) if String, path to ssl client cert file (.pem). If Tuple, ('cert', 'key') pair.
        :return: :class:`Response <Response>` object
        :rtype: requests.Response

        Usage::
            >>> from pylon import Cloudant
            >>> remote = Cloudant('https://user.cloudant.com', 'username', 'password')
            >>> req = remote.request('GET', 'https://user.cloudant.com/database')
            <Response [200]>
        """
        r = None
        delay = self.base_delay
        for i in xrange(self.max_retries, 0, -1):
            try:
                r = super(Cloudant, self).request(method, urlstr, **kwargs)
                r.raise_for_status()
                return r
            except requests.HTTPError, e:
                if e.response.status_code == 429: # Going too fast -- back off
                    delay = delay + random.randint(0, 9)/100.0
                    sleep(delay)
                    next
                else:
                    raise e # A non-429 error

        # Max retries hit, surface the 429
        http_error_msg = u'%s Error: max retries limit: %d hit for url: %s' % (r.status_code, self.max_retries, urlstr)
        raise requests.HTTPError(http_error_msg, response=r) 

    def request_streamed(self, method, urlstr, **kwargs):
        """
        Convenience method for streaming a request. Takes the same parameters as Cloudant.request().

        Generator HTTP request using the authenticated session object. Not intended to be used directly.
        See convenience methods Cloudant.changes_streamed() and Cloudant.all_docs_streamed().

        Usage::
            >>> # Note: to stream the changes feed, use changes_streamed() instead.
            >>> from pylon import Cloudant
            >>> remote = Cloudant('https://user.cloudant.com', 'username', 'password')
            >>> ch = remote.request_streamed('GET', endpoint(self.url, path('database', '/_changes')), 
                params={'style':'all_docs', 'feed':'continuous', 'timeout':0})
            >>> changes = {doc['id']:True for doc in ch if 'id' in doc}
        """
        def parse_line(regex, line):
            decoded_line = line.rstrip().decode('utf-8')
            m = re.match(regex, decoded_line)
            if m:
                value = m.group(1)
                try:
                    data = json.loads(value) # Streaming _changes, normal lines in _all_docs
                except ValueError, e:
                    try:
                        data = json.loads(value + '}') # Last proper line in _all_docs
                    except ValueError, e:
                        return ''
                return data
            else:
                return ''
        
        valid = re.compile(r"^(\{.+\}?\}),?$") # Starts with '{', ends with either '}},' or '}'
        with closing(self.request(method, urlstr, stream=True, **kwargs)) as r:
            for line in r.iter_lines():
                if line:
                    parsed = parse_line(valid, line)
                    if parsed != '':
                        yield parsed

    def changes_streamed(self, database):
        """
        Convenience method for streaming a changes feed. Generator of dict.

        :param database: the name of the database we're interested in.

        Usage::
            >>> from pylon import Cloudant
            >>> remote = Cloudant('https://user.cloudant.com', 'username', 'password')
            >>> changes = {doc['id']:True for doc in remote.changes_streamed('database') if 'id' in doc}
        """
        return self.request_streamed('GET', endpoint(self.url, path(database, '/_changes')), params={'style':'all_docs', 'feed':'continuous', 'timeout':0})

    def read_doc(self, database, docid, **kwargs):
        """
        Fetch a document from the primary index by `docid` 

        :param database: the name of the database we're interested in.
        :param docid: the id of the document.
        :rtype: dict

        See http://docs.couchdb.org/en/1.6.1/api/document/common.html#get--db-docid

        Usage::
            >>> from pylon import Cloudant
            >>> remote = Cloudant('https://user.cloudant.com', 'username', 'password')
            >>> doc = remote.read_doc('animaldb', '7a36cbc16e43e362e1ae68861aa06c0f', 
                    rev='1-5a05c08a7a601c49c8bc344d77e23020')
        """
        return self.get(endpoint(self.url, path(database, '/'+docid)), params=kwargs).json()

    def bulk_docs(self, database, data=[], **kwargs):
        """
        Raw _bulk_docs.

        This is a function primarily intended for internal use, but can
        be used directly to create, update or delete documents in bulk,
        so as to save on the HTTP overhead.

        :param database: the name of the database we're interested in.
        :param data: the list of documents to send
        :rtype: list

        Note that many other methods are implemented in terms of `bulk_docs`.

            >>> result = remote.bulk_docs('directory', [{'name':'alice'}, {'name':'bob'}])

        See http://docs.couchdb.org/en/1.6.1/api/database/bulk-api.html#post--db-_bulk_docs
        """
        return self.post(endpoint(self.url, path(database, '/_bulk_docs')), json={"docs": data}, params=kwargs).json()

    def insert(self, database, data={}):
        """
        Insert one or more new documents. If `data` is a dict, this is considered to be the body
        of a single document. If `data` is a list of dicts, these are considered to be a document set.

        Note that this is implemented via the `_bulk_docs` endpoint, rather 
        than a `POST` to `/{database}`.

        :param database: the name of the database we're interested in.
        :param data: a document (represented by a dict), or a list of documents

        Usage::
            >>> single = remote.insert('directory', {'name':'alice'}) 
            >>> multiple = remote.insert('directory', [{'name':'bob'}, {'name':'carl'}])             

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

        :param database: the name of the database we're interested in.
        :param docid: document id
        :param revid: revision id
        :param body: new document body, as a dict
        :rtype: dict

        Usage::
            >>> result = remote.update_doc('directory', '7a36cbc16e43e362e1ae68861aa06c0f', 
                    '1-5a05c08a7a601c49c8bc344d77e23020', {'name':'alice', 'phone':'07865432236'})

        See http://docs.couchdb.org/en/1.6.1/api/database/bulk-api.html#post--db-_bulk_docs
        """
        body.update({'_id':docid, '_rev':revid})
        return self.bulk_docs(database, [body])[0]

    def delete_doc(self, database, docid, revid):
        """
        Delete a document revision. Implemented via the _bulk_docs endpoint.

        :param database: the name of the database we're interested in.
        :param docid: document id
        :param revid: revision id
        :rtype: dict

        Usage::
            >>> result = remote.delete_doc('directory', '7a36cbc16e43e362e1ae68861aa06c0f', 
                    '1-5a05c08a7a601c49c8bc344d77e23020')

        See http://docs.couchdb.org/en/1.6.1/api/database/bulk-api.html#post--db-_bulk_docs
        """
        return self.update_doc(database, docid, revid, {'_deleted':True})

    def view_query(self, database, ddoc, viewname, **kwargs):
        """
        Query a secondary index.

        :param database: the name of the database we're interested in.
        :param ddoc: design document name
        :param viewname: name of view function
        :rtype: dict

        Usage::
            >>> cloudant.view_query(db, "my_ddoc", "my_view", keys=["alice", "bob"])
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

        method = 'GET'
        keys = kwargs.pop('keys', None)
        if keys:
            kwargs['json'] = {'keys':keys}
            method = 'POST' 

        return self.request(method, urlstr, **kwargs).json()

    def all_docs(self, database, **kwargs):
        """
        Query the primary index.

        :param database: the name of the database we're interested in.
        :rtype: dict

        Usage::
            >>> docs = remote.all_docs('directory', keys=[
                    '7a36cbc16e43e362e1ae68861aa06c0f',
                    '7a36cbc16e43e362e1ae68861aa06da1'])

        See http://docs.couchdb.org/en/1.6.1/api/database/bulk-api.html#db-all-docs
        """
        urlstr = endpoint(self.url, path(database, "/_all_docs"))
        method = 'GET'
        keys = kwargs.pop('keys', None)
        if keys:    
            kwargs['json'] = {'keys':keys}
            method = 'POST'

        key = kwargs.pop('key', None)
        if key:
            kwargs['json'] = {'keys':[key]}
            method = 'POST'

        return self.request(method, urlstr, **kwargs).json()

    def all_docs_streamed(self, database, **kwargs):
        """
        Query the primary index, streaming the results. Generator of dicts.

        :param database: the name of the database we're interested in.
        :rtype: generator 

        Usage::
            >>> for doc in remote.all_docs_streamed('directory', keys=[
                    '7a36cbc16e43e362e1ae68861aa06c0f',
                    '7a36cbc16e43e362e1ae68861aa06da1'
                    ]):
                    print doc

        See http://docs.couchdb.org/en/1.6.1/api/database/bulk-api.html#db-all-docs
        """
        urlstr = endpoint(self.url, path(database, "/_all_docs"))
        keys = kwargs.pop('keys', None)
        if keys:    
            return self.request_streamed('POST', urlstr, json={'keys': keys}, **kwargs)

        key = kwargs.pop('key', None)
        if key:    
            return self.request_streamed('POST', urlstr, json={'keys': [key]}, **kwargs)

        return self.request_streamed('GET', urlstr, **kwargs)

    def create_database(self, database):
        """
        Create a new database on the remote end called `database`. Returns the response body
        and a boolean which is true if a database was created, false if it already existed.

        :param database: the name of the database we're interested in.
        :rtype: tuple

        Usage::
            >>> (result, created) = remote.create_database('directory')

        See http://docs.couchdb.org/en/1.6.1/CouchDB/database/common.html#put--db
        """
        try:
            r = self.put(endpoint(self.url, '/'+database))
            r.raise_for_status()
            return (r.json(), True)
        except requests.HTTPError, e:
            if e.response.status_code == 412:
                return ({'ok':True}, False) # Database already present
            else:
                raise e

    def list_databases(self):
        """
        Return a list of all databases under the authenticated user.

        :rtype: list

        Usage::
            >>> result = remote.list_databases()

        See http://docs.couchdb.org/en/1.6.1/CouchDB/server/common.html#all-dbs
        """
        return self.get(endpoint(self.url, '/_all_dbs')).json()

    def delete_database(self, database):
        """
        Delete a database.

        :param database: the name of the database we're deleting.
        :rtype: dict

        Usage::
            >>> result = remote.delete_database('directory')

        See http://docs.couchdb.org/en/1.6.1/CouchDB/database/common.html#delete--db
        """
        return self.delete(endpoint(self.url, '/'+database)).json()

    def database_info(self, database):
        """
        Return the metadata about a database.

        :param database: the name of the database we're interested in.
        :rtype: dict

        Usage::
            >>> result = remote.database_info('directory')

        See http://docs.couchdb.org/en/1.6.1/CouchDB/database/common.html#get--db
        """
        return self.get(endpoint(self.url, '/'+database)).json()
