package p2p


// handshake func is a function that is called after a connection is established
type HandshakeFunc func(Peer) error

func NOPHandshakeFunc(Peer) error { return nil }