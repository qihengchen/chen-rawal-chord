/*
 *  Brown University, CS138, Spring 2017
 *
 *  Purpose: Implementation of RPC_API functions, these are the functions
 *  that actually get executed locally on a destination Chord node when
 *  an RPC() function is called.
 */

package chord

import (
	"bytes"
	"errors"
	"fmt"
)

// Validate that we're executing this RPC on the intended node.
func validateRpc(node *Node, reqId []byte) error {
	if !bytes.Equal(node.Id, reqId) {
		errStr := fmt.Sprintf("Node ids do not match %v, %v", node.Id, reqId)
		return errors.New(errStr)
	}
	return nil
}

func (node *Node) GetPredecessorId(req *RemoteId) (*IdReply, error) {
	if err := validateRpc(node, req.Id); err != nil {
		return nil, err
	}
	// Predecessor may be nil, which is okay.
	node.pLock.RLock()
	defer node.pLock.RUnlock()
	if node.Predecessor == nil {
		return &IdReply{nil, "", false}, nil
	} else {
		return &IdReply{node.Predecessor.Id, node.Predecessor.Addr, true}, nil
	}
}

func (node *Node) GetSuccessorId(req *RemoteId) (*IdReply, error) {
	if err := validateRpc(node, req.Id); err != nil {
		return nil, err
	}
	node.sLock.RLock()
	defer node.sLock.RUnlock()
	if node.Successor == nil {
		// TODO: a way to report an error here. Extra error checking. THis will prolly never happen
		fmt.Println("Succ is nil inside GetSuccessorId")
		// TODO: Find a way to report an error here. Extra error checking. THis will prolly never happen
	} else {
		return &IdReply{node.Successor.Id, node.Successor.Addr, true}, nil
	}
	return nil, nil
}

func (node *Node) SetPredecessorId(req *UpdateReq) (*RpcOkay, error) {
	if err := validateRpc(node, req.FromId); err != nil {
		return nil, err
	}
	// First get a write lock on this node's predecessor and defer its unlock
	node.pLock.Lock()
	defer node.pLock.Unlock()
	// Set the predecessor's id to be the updateId from the req struct
	fmt.Println("Updating node with id ", node.Id)
	fmt.Println("To id: ", req.UpdateId)

	node.Predecessor = &RemoteNode{req.UpdateId, req.UpdateAddr}

	return &RpcOkay{true}, nil
}

func (node *Node) SetSuccessorId(req *UpdateReq) (*RpcOkay, error) {
	if err := validateRpc(node, req.FromId); err != nil {
		return nil, err
	}
	// First get a write lock on this node's successor and defer its unlock
	node.sLock.Lock()
	defer node.sLock.Unlock()
	// Set the successor's id to be the updateId from the req struct
	node.Successor = &RemoteNode{req.UpdateId, req.UpdateAddr}
	return &RpcOkay{true}, nil
}

func (node *Node) Notify(req *NotifyReq) (*RpcOkay, error) {
	if err := validateRpc(node, req.NodeId); err != nil {
		return nil, err
	}

	// Take care of this notation of using a remote node pointer. We are not yet sure about this.
	node.notify(&RemoteNode{req.UpdateId, req.UpdateAddr})

	return &RpcOkay{true}, nil
}

func (node *Node) FindSuccessor(query *RemoteQuery) (*IdReply, error) {
	if err := validateRpc(node, query.FromId); err != nil {
		return nil, err
	}

	fmt.Println("Inside FindSuccessor, before calling the small method")
	successor, err := node.findSuccessor(query.Id)

	//TODO students should implement this method
	if err != nil {
		return &IdReply{successor.Id, successor.Addr, true}, err
	}

	return &IdReply{successor.Id, successor.Addr, true}, nil
}

func (node *Node) ClosestPrecedingFinger(query *RemoteQuery) (*IdReply, error) {
	if err := validateRpc(node, query.FromId); err != nil {
		return nil, err
	}

	//TODO students should implement this method

	closestRemoteNode, err := node.closest_preceding_finger(query.Id)
	if err != nil {
		return &IdReply{closestRemoteNode.Id, closestRemoteNode.Addr, true}, err
	}

	return &IdReply{closestRemoteNode.Id, closestRemoteNode.Addr, true}, nil
}

// Check if node is alive
func nodeIsAlive(remoteNode *RemoteNode) bool {
	_, err := remoteNode.GetSuccessorIdRPC()
	return err == nil
}
