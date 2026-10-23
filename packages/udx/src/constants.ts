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

/** The peer's in-flight limit until it says otherwise (dart-udx's fixed advertisement). */
export const INITIAL_MAX_DATA = 1024 * 1024
/** The in-flight limit we advertise; per-stream windows bound the actual buffering. */
export const LOCAL_MAX_DATA = 16 * 1024 * 1024
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

/** The largest datagram path MTU discovery probes for, and the MSS CUBIC grows by. */
export const MAX_DATAGRAM_SIZE = 1472
export const MIN_CWND = 2 * MAX_DATAGRAM_SIZE
export const INITIAL_CWND = 10 * MAX_DATAGRAM_SIZE
/**
 * Slow start runs until the first loss (RFC 9002 §7.3.1). This was 65535, so
 * slow start ended at 64 KB and the window crawled up from there at CUBIC's
 * pace, as in go-udx before v0.3.0.
 */
export const INITIAL_SSTHRESH = Number.MAX_SAFE_INTEGER
export const BETA_CUBIC = 0.7
export const CUBIC_C = 0.4

// --- Pacing (RFC 9002 §7.7) ---

/** The gain over cwnd / smoothed RTT in congestion avoidance. */
export const PACING_GAIN = 1.25
/** The gain in slow start, where the window doubles each round trip. */
export const PACING_GAIN_SLOW_START = 2
/** Datagrams that may go out at once without waiting on the pacer. */
export const PACING_BURST_PACKETS = 10
/**
 * The shortest wait the pacer asks for, in ms. Node sets timers in whole
 * milliseconds and the event loop adds jitter, so the burst covers at least
 * this much of the rate and a wait that overshoots by that little costs no
 * throughput (dart-udx's value; go-udx uses 1 ms).
 */
export const MIN_PACING_DELAY = 2

// --- RTT estimation (RFC 9002 §5) ---

/** RFC 9002 kInitialRtt, the estimate until the first sample. */
export const INITIAL_SMOOTHED_RTT = 333
export const INITIAL_RTT_VAR = INITIAL_SMOOTHED_RTT / 2

// --- Loss detection and the probe timeout (RFC 9002 §6) ---

/** A packet is lost once this many sequences behind the largest acknowledged (kPacketThreshold). */
export const LOSS_REORDER_THRESHOLD = 3
/** ...or once older than 9/8 of max(SRTT, latest RTT) (kTimeThreshold). */
export const LOSS_TIME_THRESHOLD = 9 / 8
export const LOSS_TIMER_GRANULARITY = 1
/** How many of the oldest outstanding packets a PTO re-sends as probes. */
export const PTO_PROBES = 2
/** A loss spanning this many PTOs with nothing acknowledged is persistent congestion (§7.6). */
export const PERSISTENT_CONGESTION_THRESHOLD = 3

// --- Path MTU discovery (RFC 8899, RFC 9000 §14), anti-amplification ---

/**
 * The datagram size every path carries: what streams send until a probe
 * validates more. 1472 bytes (a full 1500-byte IPv4 packet) does not cross
 * every carrier: some mobile tunnels drop it without an ICMP message.
 */
export const SAFE_DATAGRAM_SIZE = 1200
/** Losses of one size before the search rules it out. */
export const MTU_PROBE_TRIES = 3
/** The search ends when the largest validated and the smallest ruled out are this close. */
export const MTU_SEARCH_STEP = 16
/** Full-size data packets lost in a row, none acknowledged between, that mean a black hole. */
export const BLACKHOLE_PACKETS = 3
/** PTO expirations in a row that mean a black hole. */
export const BLACKHOLE_PTOS = 3
/** How long after a black hole the search starts again, in ms. */
export const BLACKHOLE_RETRY = 60_000
export const MAX_MTU = 1500
/** Largest UDP payload in a 1500-byte IPv4 / IPv6 packet. */
export const MAX_UDP_PAYLOAD_IPV4 = MAX_MTU - 20 - 8
export const MAX_UDP_PAYLOAD_IPV6 = MAX_MTU - 40 - 8
export const AMPLIFICATION_FACTOR = 3
/** How long a path challenge waits for its response. */
export const PATH_VALIDATION_TIMEOUT = 5000
/** Stream-ID budget we advertise in MAX_STREAMS: the incoming streams we keep open at once. */
export const MAX_INCOMING_STREAMS = 1024
