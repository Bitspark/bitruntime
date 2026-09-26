/**
 * `@bitspark/bitruntime/dispatch`: a Dispatcher owns one endpoint attachment
 * and an explicit exact/longest-prefix routing policy; call and emit send
 * through any addressed access; handle, onEvent and register run a body per
 * request or event with the context its carrier established.
 */
export { Dispatcher, SelectedEndpoint, createDispatcher, type DispatcherOptions, type Registry } from './dispatcher.ts';
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
