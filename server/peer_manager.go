package server

import (
    // "github.com/anmolrajsoni15/anmols_distributed_file_system/p2p"
)

// Peer management functionalities to update and notify peers
func (s *FileServer) AddPeer(address string) {
    s.AddPeer(address) // Adds a peer to DHT
}

// Method to notify peers when new files are uploaded
// func (s *FileServer) NotifyPeers(message p2p.Message) {
//     for _, addr := range s.peers.ListPeers() {
//         // Assuming we have a peer connection based on address
//         peer := getPeerFromAddress(addr) // Placeholder for getting peer
//         p2p.SendMessage(peer, &message)  // Send message to notify peers
//     }
// }
