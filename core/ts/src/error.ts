/** Public application errors may cross the wire; other handler errors are hidden. */
export class DuplexError extends Error {
  /** The error's code as it travels on the wire: the profile's own, or a family's public error by name. */
  readonly code: string;
  /** What a public error carries beside its message, validated as the family declares it. */
  readonly data?: unknown;

  constructor(code: string, message: string, data?: unknown) {
    super(message);
    this.name = 'DuplexError';
    this.code = code;
    this.data = data;
  }
}

/** A local refusal before a frame entered the queue or a local implementation dispatched. */
export class UnpublishedError extends DuplexError {
  override readonly cause: unknown;

  constructor(cause: unknown) {
    super(
      cause instanceof DuplexError ? cause.code : 'send_failed',
      cause instanceof Error ? cause.message : 'Duplex operation failed.',
      cause instanceof DuplexError ? cause.data : undefined,
    );
    this.name = 'UnpublishedError';
    this.cause = cause;
  }
}
