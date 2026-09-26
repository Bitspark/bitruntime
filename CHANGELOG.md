# Changelog

## Unreleased

- Rename the addressless primitive to `End` in the kickoff and the Wire-underneath-Bitwire exploration (maintainer decision, 26 September 2026): `Bitwire = Deixis[End]`, and `bitwire.Wire` stays the addressed interface. The kickoff now points to the Bitwire session's claimed design deliverable on Bitwire #42.
- Record the chosen naming pair, Bitwire = Deixis[Wire] and Bitdata =
  Deixis[Bytes], in the exploration and migration kickoff; keep Bitstore's
  persistence role and the remaining API decisions explicit.

- Make the explored Wire/Bitwire split explicit within the same Bitwire contract
  repository, and preserve attenuation when selecting an origin's send access.

- Document the family component-first layout with two-letter language directories,
  command paths and explicit adoption notes for existing source. Add the interactive
  kickoff for the first runtime and bitsystem3 migration.

- Charter the repository (Bitwire decision 0010): what it owns, what it promises and how that is versioned, what independent evidence checks it, and which change its separation makes easier.
