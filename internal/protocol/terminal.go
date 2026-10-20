package protocol

// SessionClosedNormalMessage is the message of a 410 DBB_SESSION_CLOSED
// answer for a session that ended normally (upstream FIN applied, downstream
// FIN acknowledged). The
// ingress reads the message to finish as closed_normal instead of failing.
// Other closes use "session terminated: <category>".
const SessionClosedNormalMessage = "session terminated: normal"

// SessionClosedMessagePrefix prefixes every DBB_SESSION_CLOSED message that
// carries a terminal category.
const SessionClosedMessagePrefix = "session terminated: "
