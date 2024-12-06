# Minimalistic database

The library allows to store a value associated with a key.

The size of the value must not be larger than the page size minus the metadata size (4088 bytes).

The size of the key can vary and is limited by to 1 kiB.

ACID is not tested well.

## TODO:

* B-Tree remove value, fix table scanning.
* Full encryption (Adiantum).
* More tests.
* More tests for ACID.

### Difficult

* Extent allocation -> Unlimited value;
* Better parallelism.
