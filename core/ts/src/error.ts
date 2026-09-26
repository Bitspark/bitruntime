/** A requested structural path does not exist. No primitive was called. */
export class MissingPathError extends Error {
  readonly code = 'missing_path';
  constructor() {
    super('No node exists at the requested path');
    this.name = 'MissingPathError';
  }
}

/** A carrier path contains a segment outside the Unicode-scalar contract. */
export class InvalidPathError extends Error {
  readonly code = 'invalid_path';
  constructor() {
    super('Addressed paths require Unicode-scalar string segments');
    this.name = 'InvalidPathError';
  }
}

/** An endpoint already has its one owning receiver; a second attachment is refused. */
export class ReceiverExistsError extends Error {
  readonly code = 'receiver_exists';
  constructor() {
    super('The endpoint already has a receiver');
    this.name = 'ReceiverExistsError';
  }
}

/**
 * Public application errors may cross the wire; other handler errors are
 * hidden. A closed or ended carrier is reported as one with code
 * `disconnected`, the one closed classification; its `cause`, where it has
 * one, says what ended the carrier.
 */
export class PublicError extends Error {
  /** The error's code as it travels on the wire. */
  readonly code: string;
  /** What a public error carries beside its message. */
  readonly data?: unknown;

  constructor(code: string, message: string, data?: unknown) {
    super(message);
    this.name = 'PublicError';
    this.code = code;
    this.data = data;
  }
}

/**
 * A local refusal before a frame entered the queue or a local implementation
 * dispatched. The proof belongs to this send attempt: a remote response or a
 * queued write failure never carries it, even with the same code.
 */
export class UnpublishedError extends PublicError {
  override readonly cause: unknown;

  constructor(cause: unknown) {
    super(
      cause instanceof PublicError ? cause.code : 'send_failed',
      cause instanceof Error ? cause.message : 'Duplex operation failed.',
      cause instanceof PublicError ? cause.data : undefined,
    );
    this.name = 'UnpublishedError';
    this.cause = cause;
  }
}
