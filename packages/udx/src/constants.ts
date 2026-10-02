// Protocol constants. Values mirror go-udx constants.go (wire v3), which is the
// wire authority for this port.

export const VERSION_V1 = 0x00000001
export const VERSION_V2 = 0x00000002
export const VERSION_V3 = 0x00000003

/** The only version accepted on the data path; any other is dropped. */
export const VERSION_CURRENT = VERSION_V3

/** Versions this build reports in version negotiation, in preference order. */
export const SUPPORTED_VERSIONS: readonly number[] = [VERSION_V3, VERSION_V2, VERSION_V1]

export const MIN_CID_LENGTH = 0
export const MAX_CID_LENGTH = 20
export const DEFAULT_CID_LENGTH = 8

export const STATELESS_RESET_TOKEN_LENGTH = 16
export const MIN_STATELESS_RESET_PACKET_SIZE = 39

/** Error codes carried by RESET_STREAM, STOP_SENDING and CONNECTION_CLOSE. */
export const ErrorCode = {
  NoError: 0x00,
  InternalError: 0x01,
  StreamLimitError: 0x02,
  FlowControlError: 0x03,
  ProtocolViolation: 0x04,
  InvalidMigration: 0x05,
  ConnectionTimeout: 0x06
} as const

// --- Transport parameters (go-udx constants.go). Durations are milliseconds. ---

export const INITIAL_MAX_DATA = 1024 * 1024
export const INITIAL_MAX_STREAM_DATA = 65536
export const MAX_STREAM_RECV_WINDOW = 4 * 1024 * 1024
/** Out-of-order receive backstop; above the largest window so legitimate transfers never trip it. */
export const MAX_STREAM_RECV_OOO = 2 * MAX_STREAM_RECV_WINDOW
export const STREAM_BLOCKED_RETRY_INTERVAL = 500
export const INITIAL_MAX_STREAMS = 100

// --- Acknowledgement policy (RFC 9000 §13.2) ---

/** Acknowledge after this many unacknowledged data-bearing packets. */
export const ACK_ELICITING_THRESHOLD = 2
/** The ACK timer is SRTT/4 clamped to [MIN_ACK_DELAY, MAX_ACK_DELAY]. */
export const MIN_ACK_DELAY = 1
/** Also the most ACK delay a sender subtracts from an RTT sample. */
export const MAX_ACK_DELAY = 25
/** How many sequences below the largest received the ACK builder remembers. */
export const ACK_HISTORY = 512
/** Most extra ranges an ACK frame carries. */
export const MAX_ACK_RANGES = 5

// --- Timeouts ---

export const MAX_IDLE_TIMEOUT = 30_000

// --- Congestion control ---

export const MAX_DATAGRAM_SIZE = 1472
export const MIN_CWND = 2 * MAX_DATAGRAM_SIZE
export const INITIAL_CWND = 10 * MAX_DATAGRAM_SIZE
export const INITIAL_SSTHRESH = 65535
export const BETA_CUBIC = 0.7
export const CUBIC_C = 0.4
export const PACING_GAIN = 2.88

// --- RTT estimation (RFC 9002 §5) ---

/**
 * RFC 9002 kInitialRtt. go-udx declares this but starts its estimator at
 * 100 ms, which puts the first RTO (300 ms) below the RTT of long paths and
 * resends their whole first flight.
 */
export const INITIAL_SMOOTHED_RTT = 333
export const INITIAL_RTT_VAR = INITIAL_SMOOTHED_RTT / 2
export const INITIAL_MIN_RTT = 1000

// --- Retransmission and loss detection ---

export const MIN_RTO = 200
export const MAX_RTO = 5000
export const MIN_RETRANSMIT_TIMEOUT = 200
export const MAX_RETRANSMIT_BACKOFF = 2000
/** A gap is lost once this many sequences behind the largest acknowledged (RFC 9002 kPacketThreshold). */
export const LOSS_REORDER_THRESHOLD = 3
/** ...or once older than 9/8 of max(SRTT, latest RTT) (RFC 9002 kTimeThreshold). */
export const LOSS_TIME_THRESHOLD = 9 / 8
export const LOSS_TIMER_GRANULARITY = 1

// --- Path MTU discovery, anti-amplification ---

export const MIN_MTU = 1280
export const MAX_MTU = 1500
export const AMPLIFICATION_FACTOR = 3
