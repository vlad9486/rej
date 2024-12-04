# Minimalistic database

The library allows to store a value associated with a key.

The size of the value must not be larger than the page size minus the metadata size (4088 bytes).

The size of the key can vary and is limited by to 1 kiB.

Partially guaranteed ACID.

## TODO:

* B-Tree algorithms.
* Full encryption (Adiantum).
* More tests.

### Difficult

* Extent allocation -> Unlimited value;
* Tests for ACID -> Proper write-ahead log unroll;
* Better parallelism.
