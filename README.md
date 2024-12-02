# Minimalistic database

The library allows to store a value size no larger than the page size minus 
the metadata size (4088 bytes) associated with 11-byte keys.

Partially guaranteed ACID.

TODO:
* Garbage collection.
* Proper write-ahead log unroll.
* Tests for ACID.
* Extent allocation.
* Unlimited value.
* Longer keys.
* B-Tree algorithms.
* Full encryption (Adiantum).
