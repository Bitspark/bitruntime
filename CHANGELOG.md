# Changelog

## Unreleased

- Adopt the maintainer's final primitive/tree names: addressless `Wire` and
  reading `Data`, with full `WireTree = DeixisNode<Wire>` and
  `DataTree = DeixisNode<Data>`. Document the common exact-byte-keyed structure,
  partial selection, decomposition/reconstruction and derived send/read laws.
  Mark earlier End/Bitdata naming proposals as superseded history.

- Name the old opaque addressed access `AddressedWire` and keep the
  Endpoint/return-capability boundary explicit. Update the charter, agent
  instructions and migration kickoff to follow Bitwire decision 0012 without
  claiming the runtime or consumer networking migration is implemented.

- Document the family component-first layout with two-letter language directories,
  command paths and explicit adoption notes for existing source. Add the interactive
  kickoff for the first runtime and bitsystem3 migration.

- Charter the repository (Bitwire decision 0010): what it owns, what it promises and how that is versioned, what independent evidence checks it, and which change its separation makes easier.
