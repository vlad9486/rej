# Minimalistic database

The library allows to store a value associated with a key.

The size of the value must not be larger than the page size minus the metadata size (4088 bytes).

The size of the key can vary and is limited by to 1 kiB.

Partially guaranteed ACID.

## TODO:

### Difficult

* Extent allocation.
* Proper write-ahead log unroll.
* Better parallelism.

### Moderate

* B-Tree algorithms.
* Full encryption (Adiantum).
* Unlimited value.

### Easy

* Cache of free pages in WAL entry.
* Tests for ACID.
