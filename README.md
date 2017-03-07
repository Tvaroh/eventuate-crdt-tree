# eventuate-crdt-tree

Tree CRDT for Eventuate.

## Description

Currently, implements unordered tree CRDT that supports the following policies.

### Concurrent addition

* Zero - drop both nodes
* Custom - user-defined conflict resolution based on nodes payload and parent nodes ids

### Concurrent addition/removal

* Skip - drop orphan node

## Status

**Work in progress**
