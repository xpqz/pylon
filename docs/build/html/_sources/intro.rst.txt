*****************************
A Python library for Cloudant
*****************************

Pylon is a client library for talking to Cloudant (or CouchDB) via its HTTP API. Several such libraries already exist, including an officially supported offering from Cloudant itself. So why another? We can view Pylon as a demonstration of some sensible principles for making a client library wrapping a non-trivial HTTP API. It's not meant to offer complete coverage of the API, but instead being as thin and light as possible and simple enough for quick access via the Python REPL. 

1. Low abstraction level. Pylon hides the JSON and the HTTP, but makes no other attempts at abstraction. The responses from API calls are returned de-serialised, but otherwise 'as is'. Pylon is a foundational library that can be used to construct higher-level abstractions. A somewhat glorified URL constructor? Perhaps. The heavy lifing is done by Requests.

2. No hand-holding. You will need to already be (or prepared to put the work in to become) familar with the CouchDB API in order to make good use of Pylon. Pylon itself is documented, but to understand the underlying API you need to study the Cloudant/CouchDB documentation. Optional parameters aren't named.

3. Unapologetically opinionated. Let's face it, the CouchDB API is messy. There are at least three different ways you can create a document, which leads to complexity. Pylon follows the pattern used by PouchDB, picking the _bulk_docs endpoint for document creates, updates and deletes. 

4. Deliberately incomplete API coverage. Pylon skips huge chunks of the Cloudant API, mostly to do with index creation and replication. This is deliberate, as such tasks are rarely served by a foundational client library.

5. Easy access to the authenticated session. As the API coverage is incomplete, Pylon instead gives easy access to the authenticated session so that users can craft their own requests to endpoints not covered by the library. This approach is also taken by the Nano node.js library.

6. All HTTP requests goes through a single point, in order to make things like logging, but also extension easier.
