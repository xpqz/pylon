import unittest
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
        result = self.cdt.create_doc(self.database, data={'name':'adam'})
        self.assertTrue(result['ok'])

    def test_createdocs(self):
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        result = self.cdt.create_doc(self.database, data=data)
        self.assertEquals(len(result), len(data))

    # Health warning: reading your writes NOT guaranteed to work for consistency reasons
    def test_readdoc(self): 
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.create_doc(self.database, data=data)

        for doc in written_docs:
            read_doc = self.cdt.read_doc(self.database, doc['id'])
            self.assertEqual(read_doc['_rev'], doc['rev'])

    # Health warning: reading your writes NOT guaranteed to work for consistency reasons
    def test_updatedoc(self):
        doc = self.cdt.create_doc(self.database, data={'name':'bob'})
        new_doc = self.cdt.update_doc(self.database, doc['id'], doc['rev'], body={'name':'bob', 'city':'folkstone'})
        self.assertTrue(new_doc['rev'].startswith('2-'))

    # Health warning: reading your writes NOT guaranteed to work for consistency reasons
    def test_deletedoc(self):
        doc = self.cdt.create_doc(self.database, data={'name':'bob'})
        result = self.cdt.delete_doc(self.database, doc['id'], doc['rev'])
        self.assertTrue(result['rev'].startswith('2-'))

    def test_alldocs_get(self):
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.create_doc(self.database, data=data)
        docs = self.cdt.all_docs(self.database)
        self.assertTrue(len(docs['rows']) >= len(data))

    def test_alldocs_keys(self):
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.create_doc(self.database, data=data)
        data2 = [{'name':'david'}, {'name':'eric'}, {'name':'frances'}]
        self.cdt.create_doc(self.database, data=data2)

        docs = self.cdt.all_docs(self.database, keys=[doc['id'] for doc in written_docs])
        self.assertTrue(len(docs['rows']) == len(data))

    def test_alldocs_key(self):
        data = [{'name':'adam'}, {'name':'bob'}, {'name':'charlotte'}]
        written_docs = self.cdt.create_doc(self.database, data=data)
        docs = self.cdt.all_docs(self.database, key=written_docs[0]['id'])
        self.assertTrue(len(docs['rows']) == 1)

    def test_listdbs(self):
        dbs = self.cdt.list_databases()
        self.assertTrue(self.database in dbs)

if __name__ == '__main__':
    unittest.main()
