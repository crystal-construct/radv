**Radical Vertex (rad-v)**

radv is a triplestore capable of storing arbitrary
types as the subject predicate or object.


Each triple's IDs are stored in three combinations:

(subject, predicate, object)  
(predicate, object, subject)  
(subject, object, predicate)  

The triplestore then leverages the key-prefix
lookup of [badger](https://github.com/dgraph-io/badger)
db to return the result from a query.

Once the resulting IDs have been calculated,
[velocypack](https://github.com/arangodb/go-velocypack)
is used for quick materialization of the datatypes from the IDs.

Small values ( < 40 bytes) are stored in the database,
both as key/value and value/key for 2-way lookups.

Larger values are stored as key/value and sha256/key to ensure
that the data is only stored once, but can be still used for
2-way lookups.
