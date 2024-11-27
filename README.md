# Minimalistic storage for database

The library encapsulates low-level details of storage on Unix-like and Windows.

Creates a single file, allows to allocate/deallocate persistent pages and read/write them.

Intended to be used as a low-level building block of the real full-featured database.

TODO:
* Write-ahead logging, for atomicity and crash recovery.
* Full encryption (Adiantum).
