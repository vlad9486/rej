# Minimalistic database

The library allows to store a value associated with a key.

The size of the value must not be larger than 1.5 MiB (1536 kiB).

The size of the key can vary and is limited by 1 kiB.

ACID is not tested well.

## TODO:

* Protect metadata page against hardware failure.
* Fix table scanning.
* More tests.
* More tests for ACID.

### Difficult

* Extent allocation -> Unlimited value;
* Better parallelism.
