/**
 * `@bitspark/bitruntime/dispatch`: a Dispatcher owns one endpoint attachment
 * and an explicit exact/longest-prefix routing policy, whose route sets are
 * replaced atomically; serve routes a WireTree's positions; call and emit send
 * through any addressed access; handle, onEvent and register run a body per
 * request or event with the context its carrier established.
 */
export { Dispatcher, SelectedEndpoint, createDispatcher, type DispatcherOptions, type Registry, type Route, type RouteSet } from './dispatcher.ts';
export { serve, type Served } from './serve.ts';
export {
  call,
  emit,
  handle,
  onEvent,
  register,
  type CallOptions,
  type EmitOptions,
  type EventContext,
  type EventListener,
  type Handler,
  type Handlers,
  type RequestContext,
} from './dispatch.ts';
