# Working here as an agent

Read the [charter](CHARTER.md), and in Bitwire read
[decision 0007](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0007-using-bitwire-never-requires-nightseam.md),
[decision 0010](https://github.com/Bitspark/bitwire/blob/main/docs/decisions/0010-bitwire-holds-the-contract-and-bitruntime-implements-it.md)
and the [carrier specification](https://github.com/Bitspark/bitwire/blob/main/docs/wire/carriers.md),
before changing this tree.

- Implement the Bitwire contract; never redefine it. If the contract or a
  specification looks wrong, raise it in Bitwire, don't work around it here.
- Depend on Bitwire and, in their own modules, on transport libraries. Never
  depend on Nightseam, bittype or Bitlink.
- When porting from Nightseam:
  - port from an identified commit;
  - keep the released v0.6.0 apart from its unreleased commits;
  - record the provenance in `NOTICE`;
  - add no aliases or re-exports.
- Test against Bitwire's independent cases and vectors. Never make an
  expectation match what the code happens to do.
- Treat Nightseam's recorded defects (nightseam#720–#724) as acceptance criteria
  for the ported code.
- After the initial bootstrap, use a branch or worktree and a pull request.
  Squash a green change onto `main`.
- Do not add private checkout dependencies, local orchestration state or
  credentials.
