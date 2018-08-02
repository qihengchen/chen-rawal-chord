/*
 *  Brown University, CS138, Spring 2017
 *
 *  Purpose: Finger table related functions for a given Chord node.
 */

package chord

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Global declaration for the next value
var next = 0

// A single finger table entry.
type FingerEntry struct {
	Start []byte      /* ID hash of (n + 2^i) mod (2^m)  */
	Node  *RemoteNode /* RemoteNode that Start points to */
}

// Create initial finger table that only points to itself (will be fixed later).
func (node *Node) initFingerTable() {

	//TODO students should implement this method
	node.FingerTable = make([]FingerEntry, KEY_LENGTH+1)
	for i := KEY_LENGTH; i >= 1; i-- {
		node.FingerTable[i] = FingerEntry{node.Id, node.RemoteSelf}
	}
}

// Called periodically (in a separate go routine) to fix entries in our finger table.
func (node *Node) fixNextFinger(ticker *time.Ticker) {

	for _ = range ticker.C {
		node.sdLock.RLock()
		sd := node.IsShutdown
		node.sdLock.RUnlock()
		if sd {
			Debug.Printf("[%v] Shutting down fixNextFinger timer\n", HashStr(node.Id))
			ticker.Stop()
			return
		}

		// fmt.Println("Inside fixNextFinger called by node: ", node)
		// fmt.Println("Next value: ", next)

		//TODO students should implement this method
		next_hash := fingerMath(node.Id, next, KEY_LENGTH)
		successor, err := node.findSuccessor(next_hash)
		if err != nil {
			Debug.Printf("Inside fixNextFinger, findSuccessor fucked up")
		}
		// fmt.Println("Inside fixNextFinger and after the call to findSuccessor")
		node.FingerTable[next].Node = successor
		node.FingerTable[next].Start = next_hash

		next = next + 1
		if next > KEY_LENGTH-1 {
			next = 1
		}

		// for i := KEY_LENGTH; i >= 1; i++ {
		// 	start := fingerMath(node.Id, i, KEY_LENGTH)
		// 	fingerNode, err := node.findSuccessor(start)
		// 	if err != nil {
		// 		Debug.Printf("[%v] Finding successor, fixing FingerTable", err)
		// 		return
		// 	}
		// 	node.FingerTable[i].Start = start
		// 	node.FingerTable[i].Node = fingerNode
		// }
	}
}

// Calculates: (n + 2^i) mod (2^m).
func fingerMath(n []byte, i int, m int) []byte {

	//TODO students should implement this function

	n_int, _ := strconv.Atoi(string(n))
	i_float := float64(i)
	m_float := float64(m)
	a := n_int + int(math.Pow(2, i_float))
	b := int(math.Pow(2, m_float))

	return []byte(strconv.Itoa(a % b))
}

// Print contents of a node's finger table.
func PrintFingerTable(node *Node) {
	node.FtLock.RLock()
	defer node.FtLock.RUnlock()

	fmt.Printf("[%v] FingerTable:\n", HashStr(node.Id))

	for _, val := range node.FingerTable {
		fmt.Printf("\t{start:%v\tnodeLoc:[%v] %v}\n",
			HashStr(val.Start), HashStr(val.Node.Id), val.Node.Addr)
	}
}

// Returns contents of a node's finger table as a string.
func FingerTableToString(node *Node) string {
	node.FtLock.RLock()
	defer node.FtLock.RUnlock()

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("[%v] FingerTable:\n", HashStr(node.Id)))

	for _, val := range node.FingerTable {
		buffer.WriteString(fmt.Sprintf("\t{start:%v\tnodeLoc:[%v] %v}\n",
			HashStr(val.Start), HashStr(val.Node.Id), val.Node.Addr))
	}

	return buffer.String()
}
