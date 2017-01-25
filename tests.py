#!/usr/bin/env python

import unittest
import requests
import os
import uuid

from pylon import Cloudant

username = os.environ['COUCH_USER']
password = os.environ['COUCH_PASSWORD']
host = os.environ['COUCH_HOST_URL']

class TestBasics(unittest.TestCase):
    database = 'pylon-'+uuid.uuid4().hex
    cdt = Cloudant(host, username, password)

    @classmethod
    def setUpClass(cls):
        cls.cdt.create_database(cls.database)

    @classmethod
    def tearDownClass(cls):
        cls.cdt.delete_database(cls.database)

    def test_createdoc(self):
        result = self.cdt.insert(self.database, data={'name':'adam'})
        self.assertTrue(result['ok'])

    def test_createdb(self):
        database = 'pylon-'+uuid.uuid4().hex
        (result, created) = self.cdt.create_database(database)
        self.assertTrue(created)
        (result, created) = self.cdt.create_database(database)
        self.assertFalse(created)
        cdt = Cloudant(host, username, 'not-this-passw')
        with self.assertRaises(requests.HTTPError):
            cdt.create_database(database)

    def test_createdocs(self):
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        result = self.cdt.insert(self.database, data=data)
        self.assertEquals(len(result), len(data))

    # Health warning: reading your writes NOT guaranteed to work for consistency reasons
    def test_readdoc(self): 
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.insert(self.database, data=data)

        for doc in written_docs:
            read_doc = self.cdt.read_doc(self.database, doc['id'])
            self.assertEqual(read_doc['_rev'], doc['rev'])

    # Health warning: reading your writes NOT guaranteed to work for consistency reasons
    def test_updatedoc(self):
        doc = self.cdt.insert(self.database, data={'name':'bob'})
        new_doc = self.cdt.update_doc(self.database, doc['id'], doc['rev'], body={'name':'bob', 'city':'folkstone'})
        self.assertTrue(new_doc['rev'].startswith('2-'))

    # Health warning: reading your writes NOT guaranteed to work for consistency reasons
    def test_deletedoc(self):
        doc = self.cdt.insert(self.database, data={'name':'bob'})
        result = self.cdt.delete_doc(self.database, doc['id'], doc['rev'])
        self.assertTrue(result['rev'].startswith('2-'))

    def test_alldocs_get(self):
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.insert(self.database, data=data)
        docs = self.cdt.all_docs(self.database)
        self.assertTrue(len(docs['rows']) >= len(data))

    def test_alldocs_get_streamed(self):
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.insert(self.database, data=data)
        docs = [doc for doc in self.cdt.all_docs_streamed(self.database)]
        self.assertTrue(len(docs) >= len(data))

    def test_alldocs_keys(self):
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.insert(self.database, data=data)
        data2 = [{'name':'david'}, {'name':'eric'}, {'name':'frances'}]
        self.cdt.insert(self.database, data=data2)

        docs = self.cdt.all_docs(self.database, keys=[doc['id'] for doc in written_docs])
        self.assertTrue(len(docs['rows']) == len(data))

    def test_alldocs_keys_streamed(self):
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.insert(self.database, data=data)
        data2 = [{'name':'david'}, {'name':'eric'}, {'name':'frances'}]
        self.cdt.insert(self.database, data=data2)

        docs = [d for d in self.cdt.all_docs_streamed(self.database, keys=[doc['id'] for doc in written_docs])]
        self.assertTrue(len(docs) == len(data))

    def test_alldocs_key(self):
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.insert(self.database, data=data)
        docs = self.cdt.all_docs(self.database, key=written_docs[0]['id'])
        self.assertTrue(len(docs['rows']) == 1)

    def test_alldocs_key_streamed(self): # really..
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.insert(self.database, data=data)
        docs = [d for d in self.cdt.all_docs_streamed(self.database, key=written_docs[0]['id'])]
        self.assertTrue(len(docs) == 1)

    def test_listdbs(self):
        dbs = self.cdt.list_databases()
        self.assertTrue(self.database in dbs)

    def test_streamed_changes(self): 
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = [row['id'] for row in self.cdt.insert(self.database, data=data)]
        changes = {doc['id']:True for doc in self.cdt.changes_streamed(self.database) if 'id' in doc}

        for docid in written_docs:
            self.assertTrue(docid in changes)

    def test_429(self):
        mock429 = Cloudant('http://mock429.eu-gb.mybluemix.net/', 'username', 'password')
        try:
            mock429.create_database('database')
        except requests.HTTPError, e:
            self.assertEqual(e.response.status_code, 429)
            self.assertTrue('max retries limit' in str(e))

if __name__ == '__main__':
    unittest.main()
