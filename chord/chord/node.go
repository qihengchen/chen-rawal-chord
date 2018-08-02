/*
 *  Brown University, CS138, Spring 2017
 *
 *  Purpose: Node struct definition and methods defined on it including
 *  initializing a node, joining a node to the Chord ring, and functions
 *  for a node to make calls to other nodes in the Chord ring.
 */

package chord

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/brown-csci1380/s17-nrawal-qc14/cs138"
	"google.golang.org/grpc"
)

// Non-local node representation
type RemoteNode struct {
	Id   []byte
	Addr string
}

// Local node representation
type Node struct {
	Id         []byte           /* Unique Node ID */
	Listener   *net.TCPListener /* Node listener socket */
	Server     *grpc.Server     /* RPC Server */
	Addr       string           /* String of listener address */
	RemoteSelf *RemoteNode      /* Remote node of our self */

	Successor   *RemoteNode  /* This Node's successor */
	Predecessor *RemoteNode  /* This Node's predecessor */
	sLock       sync.RWMutex /* RWLock for successor */
	pLock       sync.RWMutex /* RWLock for predecessor */

	IsShutdown bool         /* Is node in process of shutting down? */
	sdLock     sync.RWMutex /* RWLock for shutdown flag */

	FingerTable []FingerEntry /* Finger table entries */
	FtLock      sync.RWMutex  /* RWLock for finger table */

	dataStore map[string]string /* Local datastore for this node */
	DsLock    sync.RWMutex      /* RWLock for datastore */

	wg sync.WaitGroup /* WaitGroup of concurrent goroutines to sync before exiting */
}

// Initailize a Chord node, start listener, RPC server, and go routines.
func (node *Node) init(parent *RemoteNode, definedId []byte) error {
	if KEY_LENGTH > 128 || KEY_LENGTH%8 != 0 {
		log.Fatal(fmt.Sprintf("KEY_LENGTH of %v is not supported! Must be <= 128 and divisible by 8", KEY_LENGTH))
	}

	listener, _, err := cs138.OpenTCPListener()
	if err != nil {
		fmt.Println("openlistener in init node")
		return err
	}

	node.Id = HashKey(listener.Addr().String())
	Debug.Printf("The fuck is this")
	if definedId != nil {
		node.Id = definedId
	}

	node.Listener = listener
	node.Addr = listener.Addr().String()
	node.IsShutdown = false
	node.dataStore = make(map[string]string)

	// Populate RemoteNode that points to self
	node.RemoteSelf = new(RemoteNode)
	node.RemoteSelf.Id = node.Id
	node.RemoteSelf.Addr = node.Addr

	// Join this node to the same chord ring as parent
	node.wg.Add(1)
	go func(parent *RemoteNode) {
		defer node.wg.Done()
		err = node.join(parent)
	}(parent)
	if err != nil {
		fmt.Println("join routine in init node failed")
		return err
	}
	node.wg.Wait()

	// Populate finger table
	node.initFingerTable()

	// Thread 1: start RPC server on this connection

	// rpc.RegisterName(node.Addr, node)
	// node.spawn(func() { node.startRpcServer() })
	node.Server = grpc.NewServer()
	RegisterChordRPCServer(node.Server, node)
	go node.Server.Serve(node.Listener)

	// Thread 2: kick off timer to stabilize periodically

	ticker1 := time.NewTicker(time.Millisecond * 100) //freq
	node.spawn(func() { node.stabilize(ticker1) })

	// Thread 3: kick off timer to fix finger table periodically

	ticker2 := time.NewTicker(time.Millisecond * 90) //freq
	node.spawn(func() { node.fixNextFinger(ticker2) })

	return err
}

// Adds a new goroutine to the WaitGroup, spawns the go routine,
// and removes the goroutine from the WaitGroup on exit.
func (node *Node) spawn(fun func()) {
	go func() {
		node.wg.Add(1)
		fun()
		node.wg.Done()
	}()
}

// This node is trying to join an existing ring that a remote node is a part of (i.e., other)
func (node *Node) join(other *RemoteNode) error {
	if other == nil {
		fmt.Println("First node: ", node)
		node.sLock.Lock()
		node.Successor = node.RemoteSelf
		node.sLock.Unlock()
		return nil
		//Debug.Printf("join failed because other is nil")
	}
	fmt.Println("FindPredecessorRPC caleld in join")
	newNodesPredecessor, err0 := other.GetPredecessorIdRPC()
	fmt.Println("FindPredecessorRPC returned in join")
	fmt.Println(newNodesPredecessor, err0)

	fmt.Println("FindSuccessorRPC called on node: ", other)
	fmt.Println("Called by node: ", node.Id)

	fmt.Println("FindSuccessorRPC called in join")
	mySuccesor, err := other.FindSuccessorRPC(node.Id)
	fmt.Print("FindSuccessorRPC returned in join ")
	fmt.Println("mySuccesor ", mySuccesor)
	if err != nil {
		return err
	}
	if EqualIds(mySuccesor.Id, other.Id) {
		node.Successor = other
		node.Predecessor = other
		if err := other.SetPredecessorIdRPC(node.RemoteSelf); err != nil {
			fmt.Printf("SetPredecessorIdRPC in join failed")
		}
		if err := other.SetSuccessorIdRPC(node.RemoteSelf); err != nil {
			fmt.Println("SetSuccessorIdRPC in join failed")
		}

		fmt.Println("N2")
		fmt.Println(node.Successor)
		fmt.Println(node.Predecessor)
		fmt.Println("N1")
		if suc, err := other.GetSuccessorIdRPC(); err == nil {
			fmt.Println(suc)
		}
		if pre, err := other.GetPredecessorIdRPC(); err == nil {
			fmt.Println(pre)
		}
		return nil
	}
	// Lock the successor while writing new successor
	node.sLock.Lock()
	node.Successor = mySuccesor
	node.sLock.Unlock()

	return err
}

// Thread 2: Psuedocode from figure 7 of chord paper
func (node *Node) stabilize(ticker *time.Ticker) {
	for _ = range ticker.C {
		node.sdLock.RLock()
		sd := node.IsShutdown
		node.sdLock.RUnlock()
		if sd {
			Debug.Printf("[%v-stabilize] Shutting down stabilize timer\n", HashStr(node.Id))
			ticker.Stop()
			return
		}

		x, err := node.Successor.GetPredecessorIdRPC()
		if err != nil {
			fmt.Print("GetPredecessorIdRPC in stabilize failed ")
			fmt.Println(err)
		}

		node.sLock.Lock()

		if x != nil && Between(x.Id, node.Id, node.Successor.Id) {
			node.Successor = x
		}

		node.sLock.Unlock()

		// Tell the successor, "Hey I think you are my successor"
		err = node.Successor.NotifyRPC(node.RemoteSelf)
		if err != nil {
			Debug.Printf("NotifyRPC in stabilize failed")
		}
	}
}

// Psuedocode from figure 7 of chord paper
func (node *Node) notify(remoteNode *RemoteNode) {

	node.pLock.Lock()
	defer node.pLock.Unlock()
	if node.Predecessor == nil {
		node.Predecessor = remoteNode
	}
	if node.Predecessor == nil || Between(remoteNode.Id, node.Predecessor.Id, node.Id) {
		//exPredecessorId := node.Predecessor.Id
		node.Predecessor = remoteNode
		//remoteNode.TransferKeysRPC(remoteNode, exPredecessorId)
	}
}

// Psuedocode from figure 4 of chord paper
func (node *Node) findSuccessor(id []byte) (*RemoteNode, error) {
	//fmt.Println("findPredecessor in findPredecessor get called")
	newNodesPredecessor, err := node.findPredecessor(id)
	//fmt.Println(newNodesPredecessor)
	//fmt.Println(id)
	//fmt.Println(node)
	//fmt.Println("findPredecessor in findPredecessor returned")
	//fmt.Println("****")
	//fmt.Println(newNodesPredecessor) //should be nil
	// fmt.Println("Inside findSucc, node, id: ", node, id)
	if err != nil {
		return nil, err
	}
	if EqualIds(newNodesPredecessor.Id, node.RemoteSelf.Id) {
		//fmt.Println("equal ids")
		return newNodesPredecessor, nil
	}
	newNodesSuccessor, err1 := newNodesPredecessor.GetSuccessorIdRPC()

	if err1 != nil {
		return newNodesSuccessor, err1
	}

	return newNodesSuccessor, nil
}

/*
// Psuedocode from figure 4 of chord paper
func (node *Node) findPredecessor(id []byte) (*RemoteNode, error) {
	//TODO students should implement this method
	if !(BetweenRightIncl(id, node.Id, node.Successor.Id)) {
		return node.RemoteSelf, nil
	}
	nPrime, err := node.closest_preceding_finger(id)
	var nPrimeSuccessor *RemoteNode
	if nPrimeSuccessor, err = nPrime.GetSuccessorIdRPC(); err != nil {
		Debug.Printf("GetSuccessorIdRPC in findPredecessor failed")
	}
	for !(BetweenRightIncl(id, nPrime.Id, nPrimeSuccessor.Id)) {
		nPrime, err = nPrime.GetPredecessorIdRPC()
	}
	// for !(id > n.Id && id < n.Successor.Id) {
	// 	n, _ = n.closest_preceding_finger(id)
	// }
	return nPrime, nil
}*/

//If my successor is myself, that means I am the only node in the ring, and I am your predecessor
func (node *Node) findPredecessor(id []byte) (*RemoteNode, error) {
	if EqualIds(node.Id, node.Successor.Id) {
		return node.RemoteSelf, nil
	}
	// fmt.Println("Inside findPred")

	//In all other cases, there are multiple nodes in the ring
	nPrime := node.RemoteSelf
	// fmt.Println("nPrime: ", nPrime)
	nPrimeSuccessor, err := nPrime.GetSuccessorIdRPC()
	// fmt.Println("nPrimeSuccessor: ", nPrimeSuccessor)
	var err1, err2 error
	if err != nil {
		fmt.Println("Nprime's successor is fucked") //err
		return nil, err
	}

	// fmt.Println("Before the loop")

	for !(BetweenRightIncl(id, nPrime.Id, nPrimeSuccessor.Id)) {
		//fmt.Println("Inside the loop nPrime before finger call: ", nPrime)
		nPrime, err1 = nPrime.ClosestPrecedingFingerRPC(id)
		if err1 != nil {
			fmt.Println("Nprime's closest finger is fucked")
			return nil, err1
		}
		//fmt.Println("Inside the loop nPrimeSuccessor before succ call: ", nPrimeSuccessor)
		nPrimeSuccessor, err2 = nPrime.GetSuccessorIdRPC()
		//fmt.Println("Inside the loop nPrimeSuccessor after the getSucc call: ", nPrimeSuccessor)
		if err2 != nil {
			fmt.Println("NprimeSuccessor's successor is fucked")
			return nil, err2
		}
	}
	// fmt.Println("Outside the loop. Returning nPrime, err: ", nPrime, err)
	return nPrime, err
}

func (node *Node) closest_preceding_finger(id []byte) (*RemoteNode, error) {
	//ERROR FingerTable may be fucked up. is the lock necessary?
	// fmt.Println("Inside cpf, before the loop")
	for i := KEY_LENGTH; i >= 1; i-- {
		// fmt.Println("Inside loop, i=", i)
		entryStart := node.FingerTable[i].Start
		// fmt.Println("entryStart: ", entryStart)
		if Between(entryStart, node.Id, id) {
			// fmt.Println("Inside the if in cpf")
			return node.FingerTable[i].Node, nil
		}
		// if entryStart > node.Id && entryStart < id {
		// 	return node.FingerTable[i].Node, nil
		// }
	}
	// fmt.Println("Outside the loop, returning node's remoteSelf and nil")
	return node.RemoteSelf, nil
}
