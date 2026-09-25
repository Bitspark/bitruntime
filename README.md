# bitruntime

The Go and TypeScript implementation of the
[Bitwire](https://github.com/Bitspark/bitwire) contract. It provides:

- operators;
- transports and carriers;
- the protocol engine;
- the invocation lifecycle;
- dispatch;
- live references;
- tunnels.

**Status: chartered 26 September 2026 by
[Bitwire decision 0010](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0010-bitwire-holds-the-contract-and-bitruntime-implements-it.md);
no code yet.** Until bitruntime delivers, frozen Nightseam v0.6.0 is the
implementation in use.

## Where it sits

```text
bitruntime  →  Bitwire (the contract, the protocol and carrier specifications, conformance)
```

- **Bitwire** says what a Wire must do. bitruntime does it.
- **Bitwire's conformance cases** judge bitruntime as an external implementation,
  written from the specification and never recorded from this code.
- **What bitruntime depends on.** Bitwire, and in separate modules, the libraries a
  transport needs.
- **What it does not depend on.** The contract language (bittype), the adapters
  (Bitlink), or Nightseam.

## Read first

- [Charter](CHARTER.md): what this repository owns, promises and is checked by.
- [Working here as an agent](AGENTS.md).
- [Bitwire's carrier specification](https://github.com/Bitspark/bitwire/blob/main/docs/wire/carriers.md)
  and [the contract](https://github.com/Bitspark/bitwire/blob/main/docs/wire/contract.md).
