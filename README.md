# Minimalistic database

The library allows to store a value size no larger than the page size minus 
the metadata size (4088 bytes) associated with 11-byte keys.

Partially guaranteed ACID.

## TODO:

### Difficult

* Extent allocation.
* Proper write-ahead log unroll.

### Moderate

* B-Tree algorithms.
* Full encryption (Adiantum).
* Unlimited value.

### Easy

* Tests for ACID.
* Longer keys.
