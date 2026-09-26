/**
 * `@bitspark/bitruntime/engine`: the bitwire/1 protocol engine. A Peer speaks
 * the protocol over any frames duplex connection of the seam — a pipe, a
 * WebSocket it connects or is handed — and presents it only through its root
 * Endpoint, `wire()`.
 */
export { Peer, PEER_DEFAULTS, PROTOCOL, type PeerOptions, type PeerStatus } from './peer.ts';
