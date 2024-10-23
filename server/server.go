package server

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/anmolrajsoni15/anmols_distributed_file_system/dht"
	"github.com/anmolrajsoni15/anmols_distributed_file_system/p2p"
	"github.com/anmolrajsoni15/anmols_distributed_file_system/store"
	"github.com/anmolrajsoni15/anmols_distributed_file_system/crypto"
)

type FileServerOpts struct {
	ID                string
	EncKey            []byte
	StorageRoot       string
	PathTransformFunc store.PathTransformFunc
	Transport         p2p.Transport
	BootstrapNodes    []string
	NodeID    				dht.NodeID
}

type FileServer struct {
	FileServerOpts

	peerLock sync.Mutex
	peers    map[string]p2p.Peer

	Stores  *store.Store
	dht       *dht.DHT
	nodeID    dht.NodeID
	quitch chan struct{}
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := store.StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	if len(opts.ID) == 0 {
		opts.ID = crypto.GenerateID()
	}

	if opts.NodeID == [20]byte{} {
		opts.NodeID = dht.NewNodeID(string(opts.EncKey))
	}

	s := &FileServer{
		FileServerOpts: opts,
		peers:          make(map[string]p2p.Peer),
		Stores:          store.NewStore(storeOpts),
		quitch:         make(chan struct{}),
		dht:            dht.NewDHT(opts.NodeID),
		nodeID:         opts.NodeID,
	}

	return s
}

func (s *FileServer) broadcast(nodes []string, msg *Message) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, addr := range nodes {
		fmt.Println("Peers:- ", s.peers)
		if peer, ok := s.peers[addr]; ok {
			fmt.Printf("[%s] broadcasting message to %s\n", s.Transport.Addr(), addr)
			peer.Send([]byte{p2p.IncomingMessage})
			if err := peer.Send(buf.Bytes()); err != nil {
				return err
			}
		}
	}

	return nil
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	ID   string
	Key  string
	Size int64
}

type MessageGetFile struct {
	ID  string
	Key string
}


// Add a new method to find nodes
func (s *FileServer) FindNodes(key string) []string {
	targetID := dht.NewNodeID(key)
	nodes := s.dht.FindClosestNodes(targetID)
	
	addresses := make([]string, len(nodes))
	for i, node := range nodes {
		addresses[i] = node.Address
	}
	
	return addresses
}

func (s *FileServer) Get(key string) (io.Reader, error) {
	if s.Stores.Has(s.ID, key) {
		fmt.Printf("[%s] serving file (%s) with id [%s] from local disk\n", s.Transport.Addr(), key, s.ID)
		_, r, err := s.Stores.Read(s.ID, key)
		return r, err
	}

	fmt.Printf("[%s] dont have file (%s) locally, fetching from network...\n", s.Transport.Addr(), key)

	// If not found locally, find nodes that might have it
	nodes := s.FindNodes(key)

	msg := Message{
		Payload: MessageGetFile{
			ID:  s.ID,
			Key: crypto.HashKey(key),
		},
	}

	if err := s.broadcast(nodes, &msg); err != nil {
		return nil, err
	}

	time.Sleep(time.Millisecond * 500)

	for _, addr := range nodes {
		if peer, ok := s.peers[addr]; ok {
		// First read the file size so we can limit the amount of bytes that we read
		// from the connection, so it will not keep hanging.
			var fileSize int64
			binary.Read(peer, binary.LittleEndian, &fileSize)

			n, err := s.Stores.WriteDecrypt(s.EncKey, s.ID, key, io.LimitReader(peer, fileSize))
			if err != nil {
				return nil, err
			}

			fmt.Printf("[%s] received (%d) bytes over the network from (%s)", s.Transport.Addr(), n, peer.RemoteAddr())

			peer.CloseStream()
		}
	}

	_, r, err := s.Stores.Read(s.ID, key)
	return r, err
}

func (s *FileServer) GetServerd(serverId string, key string, encKey []byte) (io.Reader, error) {
	if s.Stores.Has(serverId, key) {
		fmt.Printf("[%s] serving file (%s) with id [%s] from local disk\n", s.Transport.Addr(), crypto.HashKey(key), serverId)
		_, r, err := s.Stores.Read(serverId, key)
		return r, err
	}

	_, r, err := s.Stores.DecryptAndReadStream(encKey, serverId, s.ID, crypto.HashKey(key))
	return r, err
}

func (s *FileServer) Store(key string, r io.Reader) error {
	var (
		fileBuffer = new(bytes.Buffer)
		tee        = io.TeeReader(r, fileBuffer)
	)

	fmt.Println("tee: ", tee)
	size, err := s.Stores.Write(s.ID, key, tee)
	if err != nil {
		return err
	}

	// Find nodes to store the file
	nodes := s.FindNodes(string(s.EncKey))

	msg := Message{
		Payload: MessageStoreFile{
			ID:   s.ID,
			Key:  crypto.HashKey(key),
			Size: size + 16,
		},
	}

	if err := s.broadcast(nodes, &msg); err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 5)

	peers := []io.Writer{}
	for _, addr := range nodes {
		if peer, ok := s.peers[addr]; ok {
			fmt.Printf("[%s] sending file (%s) to peer %s\n", s.Transport.Addr(), key, addr)
			peers = append(peers, peer)
		}
	}
	mw := io.MultiWriter(peers...)
	mw.Write([]byte{p2p.IncomingStream})
	n, err := crypto.CopyEncrypt(s.EncKey, fileBuffer, mw)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] received and written (%d) bytes to disk\n", s.Transport.Addr(), n)

	return nil
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func (s *FileServer) OnPeer(p p2p.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	addr := p.RemoteAddr().String()
	s.peers[addr] = p

	// Add the peer to the DHT
	node := dht.Node{
		ID:       dht.NewNodeID(addr),
		Address:  addr,
		LastSeen: time.Now(),
	}
	s.dht.AddNode(node)

	log.Printf("connected with remote %s", p.RemoteAddr())

	return nil
}

func (s *FileServer) loop() {
	defer func() {
		log.Println("file server stopped due to error or user quit action")
		s.Transport.Close()
	}()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			// log.Println("received rpc: ", rpc)
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println("decoding error: ", err)
			}
			if err := s.handleMessage(rpc.From, &msg); err != nil {
				log.Println("handle message error: ", err)
			}

		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) handleMessage(from string, msg *Message) error {
	log.Printf("received message from %s: %v", from, msg)
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		log.Println("message store file")
		return s.handleMessageStoreFile(from, v)
	case MessageGetFile:
		log.Println("message get file")
		return s.handleMessageGetFile(from, v)
	}

	return nil
}

func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	if !s.Stores.Has(msg.ID, msg.Key) {
		return fmt.Errorf("[%s] need to serve file (%s) with messageid [%s] but it does not exist on disk", s.Transport.Addr(), msg.Key, msg.ID)
	}

	fmt.Printf("[%s] serving file (%s) over the network\n", s.Transport.Addr(), msg.Key)

	fileSize, r, err := s.Stores.Read(msg.ID, msg.Key)
	if err != nil {
		return err
	}

	if rc, ok := r.(io.ReadCloser); ok {
		fmt.Println("closing readCloser")
		defer rc.Close()
	}

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not in map", from)
	}

	// First send the "incomingStream" byte to the peer and then we can send
	// the file size as an int64.
	peer.Send([]byte{p2p.IncomingStream})
	binary.Write(peer, binary.LittleEndian, fileSize)
	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written (%d) bytes over the network to %s\n", s.Transport.Addr(), n, from)

	return nil
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}

	n, err := s.Stores.Write(msg.ID, msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written %d bytes to disk from loop\n", s.Transport.Addr(), n)

	peer.CloseStream()

	return nil
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}

		go func(addr string) {
			fmt.Printf("[%s] attemping to connect with remote %s\n", s.Transport.Addr(), addr)
			if err := s.Transport.Dial(addr); err != nil {
				log.Println("dial error: ", err)
			}
		}(addr)
	}

	return nil
}

func (s *FileServer) Start() error {
	fmt.Printf("[%s] starting fileserver...\n", s.Transport.Addr())

	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	s.bootstrapNetwork()

	s.loop()

	return nil
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
}