/*
 *  Brown University, CS138, Spring 2017
 *
 *  Purpose: RPC API used to communicate with other nodes in the Chord ring.
 */

package chord

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

type RemoteId struct {
	Id []byte
}

type RemoteQuery struct {
	FromId []byte
	Id     []byte
}

type IdReply struct {
	Id    []byte
	Addr  string
	Valid bool
}

type RpcOkay struct {
	Ok bool
}

type KeyValueReq struct {
	NodeId []byte
	Key    string
	Value  string
}

type KeyValueReply struct {
	Key   string
	Value string
}

type TransferReq struct {
	NodeId   []byte
	FromId   []byte
	FromAddr string
	PredId   []byte
}

type UpdateReq struct {
	FromId     []byte
	UpdateId   []byte
	UpdateAddr string
}

// NodeId: Intended node to received the request
// UpdateId: Node that you are wishing to notify about
type NotifyReq struct {
	NodeId     []byte
	NodeAddr   string
	UpdateId   []byte
	UpdateAddr string
}

// RPC connection map cache
var connByAddress = make(map[string]*grpc.ClientConn)
var connByAddressMutex = &sync.Mutex{}

/*
 * Chord Node RPC API
 *
 * RPC invocations for remote chord nodes
 * These functions are to be used on remote nodes to initiate a RPC function
 * invocation on them. You do not need to alter them in any way.
 */

// Get the predecessor ID of a remote node
func (remote *RemoteNode) GetPredecessorIdRPC() (*RemoteNode, error) {
	if remote == nil {
		return &RemoteNode{}, errors.New("RemoteNode is empty!")
	}
	cc, err := remote.ClientConnection()

	if err != nil {
		return &RemoteNode{}, err
	}

	resp, err := cc.GetPredecessorIdCaller(context.Background(), &RemoteNodeMsg{remote.Id, remote.Addr})

	if err != nil {
		return &RemoteNode{}, err
	}

	if !resp.Valid {
		return nil, err
	}

	return &RemoteNode{resp.Id, resp.Addr}, nil
}

// Get the successor ID of a remote node
func (remote *RemoteNode) GetSuccessorIdRPC() (*RemoteNode, error) {
	if remote == nil {
		return &RemoteNode{}, errors.New("RemoteNode is empty!")
	}

	cc, err := remote.ClientConnection()

	if err != nil {
		return &RemoteNode{}, err
	}

	resp, err := cc.GetSuccessorIdCaller(context.Background(), &RemoteNodeMsg{remote.Id, remote.Addr})

	if err != nil {
		return &RemoteNode{}, err
	}

	if !resp.Valid {
		return nil, err
	}

	return &RemoteNode{resp.Id, resp.Addr}, nil
}

// Set the predecessor ID of a remote node
func (remote *RemoteNode) SetPredecessorIdRPC(newPred *RemoteNode) error {
	if remote == nil {
		return errors.New("RemoteNode is empty!")
	}

	cc, err := remote.ClientConnection()

	if err != nil {
		return err
	}

	req := UpdateReqMsg{}
	req.FromId = remote.Id
	if newPred != nil {
		req.UpdateId = newPred.Id
		req.UpdateAddr = newPred.Addr
	}

	resp, err := cc.SetPredecessorIdCaller(context.Background(), &req)

	if err != nil {
		return err
	}

	if resp.Ok != true {
		return errors.New(fmt.Sprintf("RPC replied not valid from %v", remote.Id))
	}

	return err
}

// Set the successor ID of a remote node
func (remote *RemoteNode) SetSuccessorIdRPC(newPred *RemoteNode) error {
	if remote == nil {
		return errors.New("RemoteNode is empty!")
	}

	cc, err := remote.ClientConnection()

	if err != nil {
		return err
	}

	req := UpdateReqMsg{}
	req.FromId = remote.Id
	if newPred != nil {
		req.UpdateId = newPred.Id
		req.UpdateAddr = newPred.Addr
	}

	resp, err := cc.SetSuccessorIdCaller(context.Background(), &req)

	if resp.Ok != true {
		return errors.New(fmt.Sprintf("RPC replied not valid from %v", remote.Id))
	}

	return err
}

// Notify a remote node that we believe we are its predecessor
func (remote *RemoteNode) NotifyRPC(us *RemoteNode) error {
	if remote == nil {
		return errors.New("RemoteNode is empty!")
	}

	cc, err := remote.ClientConnection()

	if err != nil {
		return err
	}

	req := NotifyReqMsg{}
	req.NodeId = remote.Id
	req.NodeAddr = remote.Addr
	req.UpdateId = us.Id
	req.UpdateAddr = us.Addr

	resp, err := cc.NotifyCaller(context.Background(), &req)

	if err != nil {
		return err
	}

	if resp.Ok != true {
		return errors.New(fmt.Sprintf("RPC replied not valid from %v", remote.Id))
	}

	return err
}

// Find the successor node of a given ID in the entire ring
func (remote *RemoteNode) FindSuccessorRPC(id []byte) (*RemoteNode, error) {
	if remote == nil {
		return &RemoteNode{}, errors.New("RemoteNode is empty!")
	}

	cc, err := remote.ClientConnection()

	if err != nil {
		return &RemoteNode{}, err
	}

	fmt.Println("Inside find Succ rpc call, before calling the caller function")
	resp, err := cc.FindSuccessorCaller(context.Background(), &RemoteQueryMsg{remote.Id, id})

	if err != nil {
		return &RemoteNode{}, err
	}

	if !resp.Valid {
		return nil, err
	}

	return &RemoteNode{resp.Id, resp.Addr}, nil
}

// Find the closest preceding finger from a remote node for an ID
func (remote *RemoteNode) ClosestPrecedingFingerRPC(id []byte) (*RemoteNode, error) {
	if remote == nil {
		return &RemoteNode{}, errors.New("RemoteNode is empty!")
	}

	cc, err := remote.ClientConnection()

	if err != nil {
		return &RemoteNode{}, err
	}

	resp, err := cc.ClosestPrecedingFingerCaller(context.Background(), &RemoteQueryMsg{remote.Id, id})

	if err != nil {
		return &RemoteNode{}, err
	}

	return &RemoteNode{resp.Id, resp.Addr}, nil
}

// Get a value from a remote node's datastore for a given key
func (remote *RemoteNode) GetRPC(key string) (string, error) {
	if remote == nil {
		return "", errors.New("RemoteNode is empty!")
	}

	cc, err := remote.ClientConnection()

	if err != nil {
		return "", err
	}

	resp, err := cc.GetCaller(context.Background(), &KeyValueReqMsg{remote.Id, key, ""})

	if err != nil {
		return "", err
	}

	return resp.Value, nil
}

// Put a key/value into a datastore on a remote node
func (remote *RemoteNode) PutRPC(key, value string) error {
	if remote == nil {
		return errors.New("RemoteNode is empty!")
	}

	cc, err := remote.ClientConnection()

	if err != nil {
		return err
	}

	_, err = cc.PutCaller(context.Background(), &KeyValueReqMsg{remote.Id, key, value})

	return err
}

// Inform a successor node (succ) that we now take care of IDs between (predId : node.Id]
// this should trigger the successor node to transfer the relevant keys back 'node'
func (remote *RemoteNode) TransferKeysRPC(node *RemoteNode, predId []byte) error {

	if remote == nil {
		return errors.New("RemoteNode is empty!")
	}

	cc, err := remote.ClientConnection()

	if err != nil {
		return err
	}

	resp, err := cc.TransferKeysCaller(context.Background(), &TransferReqMsg{remote.Id, node.Id, node.Addr, predId})

	if err != nil {
		return err
	}

	if resp.Ok != true {
		return errors.New(fmt.Sprintf("RPC replied not valid from %v", remote.Id))
	}

	return nil
}

/*
 * Server Side RPC handlers.
 * These functions have been implemented by the TA's so you don't need to worry
 * about handling protobuff conversions.
 */

func (node *Node) GetPredecessorIdCaller(ctx context.Context, remote *RemoteNodeMsg) (*IdReplyMsg, error) {
	req := RemoteId{remote.Id}

	resp, err := node.GetPredecessorId(&req)

	if err != nil {
		return &IdReplyMsg{}, err
	}

	return &IdReplyMsg{resp.Id, resp.Addr, resp.Valid}, nil

}

func (node *Node) GetSuccessorIdCaller(ctx context.Context, remote *RemoteNodeMsg) (*IdReplyMsg, error) {
	req := RemoteId{remote.Id}

	resp, err := node.GetSuccessorId(&req)

	if err != nil {
		return &IdReplyMsg{}, err
	}

	return &IdReplyMsg{resp.Id, resp.Addr, resp.Valid}, nil
}

func (node *Node) SetPredecessorIdCaller(ctx context.Context, updateReq *UpdateReqMsg) (*RpcOkayMsg, error) {
	req := UpdateReq{updateReq.FromId, updateReq.UpdateId, updateReq.UpdateAddr}

	resp, err := node.SetPredecessorId(&req)

	if err != nil {
		return &RpcOkayMsg{}, err
	}

	return &RpcOkayMsg{resp.Ok}, nil
}

func (node *Node) SetSuccessorIdCaller(ctx context.Context, updateReq *UpdateReqMsg) (*RpcOkayMsg, error) {
	req := UpdateReq{updateReq.FromId, updateReq.UpdateId, updateReq.UpdateAddr}

	resp, err := node.SetSuccessorId(&req)

	if err != nil {
		return &RpcOkayMsg{}, err
	}

	return &RpcOkayMsg{resp.Ok}, nil
}

func (node *Node) NotifyCaller(ctx context.Context, notifyReq *NotifyReqMsg) (*RpcOkayMsg, error) {
	req := &NotifyReq{notifyReq.NodeId, notifyReq.NodeAddr, notifyReq.UpdateId, notifyReq.UpdateAddr}

	resp, err := node.Notify(req)

	if err != nil {
		return &RpcOkayMsg{}, err
	}

	return &RpcOkayMsg{resp.Ok}, nil
}

func (node *Node) FindSuccessorCaller(ctx context.Context, remoteQuery *RemoteQueryMsg) (*IdReplyMsg, error) {
	req := RemoteQuery{remoteQuery.FromId, remoteQuery.Id}

	resp, err := node.FindSuccessor(&req)

	if err != nil {
		return &IdReplyMsg{}, err
	}

	return &IdReplyMsg{resp.Id, resp.Addr, resp.Valid}, nil

}

func (node *Node) ClosestPrecedingFingerCaller(ctx context.Context, remoteQuery *RemoteQueryMsg) (*IdReplyMsg, error) {
	req := RemoteQuery{remoteQuery.FromId, remoteQuery.Id}

	resp, err := node.ClosestPrecedingFinger(&req)

	if err != nil {
		return &IdReplyMsg{}, err
	}

	return &IdReplyMsg{resp.Id, resp.Addr, resp.Valid}, nil

}

func (node *Node) GetCaller(ctx context.Context, reqMsg *KeyValueReqMsg) (*KeyValueReplyMsg, error) {
	req := KeyValueReq{reqMsg.NodeId, reqMsg.Key, reqMsg.Value}

	resp, err := node.GetLocal(&req)

	if err != nil {
		return &KeyValueReplyMsg{}, err
	}

	return &KeyValueReplyMsg{resp.Key, resp.Value}, nil
}

func (node *Node) PutCaller(ctx context.Context, reqMsg *KeyValueReqMsg) (*KeyValueReplyMsg, error) {
	req := KeyValueReq{reqMsg.NodeId, reqMsg.Key, reqMsg.Value}

	resp, err := node.PutLocal(&req)

	if err != nil {
		return &KeyValueReplyMsg{}, err
	}

	return &KeyValueReplyMsg{resp.Key, resp.Value}, nil
}

func (node *Node) TransferKeysCaller(ctx context.Context, reqMsg *TransferReqMsg) (*RpcOkayMsg, error) {
	req := TransferReq{reqMsg.NodeId, reqMsg.FromId, reqMsg.FromAddr, reqMsg.PredId}

	resp, err := node.TransferKeys(&req)

	if err != nil {
		return &RpcOkayMsg{}, err
	}

	return &RpcOkayMsg{resp.Ok}, nil
}

/*
 * Helper functions
 */

// To make a call to a remote node
func (remote *RemoteNode) ClientConnection() (ChordRPCClient, error) {
	connByAddressMutex.Lock()
	defer connByAddressMutex.Unlock()

	if cc, ok := connByAddress[remote.Addr]; ok && cc != nil {
		return NewChordRPCClient(cc), nil
	}

	cc, err := grpc.Dial(remote.Addr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithTimeout(RPC_TIMEOUT))

	if err != nil {
		return nil, err
	}

	connByAddress[remote.Addr] = cc
	return NewChordRPCClient(cc), err
}
