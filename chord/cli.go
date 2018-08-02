/*
 *  Brown University, CS138, Spring 2017
 *
 *  Purpose: CLI to start up a Chord instance and interact with it.
 */

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/big"
	"strings"

	"github.com/brown-csci1380/s17-nrawal-qc14/chord/chord"

	"gopkg.in/abiosoft/ishell.v1"
)

func printHelp(shell *ishell.Shell) {
	shell.Println("Commands:")
	shell.Println(" - help                    Prints this help message")
	shell.Println(" - node                    Prints the id, predecessor and successor of for node(s)")
	shell.Println(" - addr                    Display node listener address(es)")
	shell.Println(" - data                    Display datastore(s) for node(s)")
	shell.Println(" - table                   Display finger table information for node(s)")
	shell.Println(" - put <key> <value>       Stores the provided key-value in the Chord ring")
	shell.Println(" - get <key>               Looks up the value associated with key in the Chord ring")
	shell.Println(" - debug on|off            Toggles debug printing statements on or off (on by default)")
	shell.Println(" - quit                    Shutdown node(s), then quit this CLI")
}

func main() {
	countPtr := flag.Int("count", 1, "Total number of Chord nodes to start up in this process")
	addrPtr := flag.String("addr", "", "Address of a node in the Chord ring you wish to join")
	idPtr := flag.String("id", "", "ID of a node in the Chord ring you wish to join")
	debugPtr := flag.Bool("debug", true, "Start the CLI with debug statements turned on")
	flag.Parse()

	var parent *chord.RemoteNode
	if *addrPtr == "" {
		parent = nil
	} else {
		parent = new(chord.RemoteNode)
		bInt, succ := big.NewInt(int64(0)).SetString(*idPtr, 10)
		if succ {
			parent.Id = bInt.Bytes()
			for len(parent.Id) < chord.KEY_LENGTH/8 {
				parent.Id = append([]byte{0}, parent.Id...)
			}
			parent.Id = parent.Id[:chord.KEY_LENGTH/8]
		}
		parent.Addr = *addrPtr
		fmt.Printf("Attach this node to id:%v, addr:%v\n", parent.Id, parent.Addr)
	}

	var err error
	nodes := make([]*chord.Node, *countPtr)
	for i := range nodes {
		nodes[i], err = chord.CreateNode(parent)
		if err != nil {
			fmt.Println("Unable to create new node!")
			log.Fatal(err)
		}
		if parent == nil {
			parent = nodes[i].RemoteSelf
		}
		fmt.Printf("Created -id %v -addr %v\n", chord.HashStr(nodes[i].Id), nodes[i].Addr)
	}

	chord.SetDebug(*debugPtr)

	shell := ishell.New()

	printHelp(shell)

	shell.Register("node", func(args ...string) (string, error) {
		var buffer bytes.Buffer
		for _, node := range nodes {
			buffer.WriteString(NodeStr(node))
		}
		return buffer.String(), nil
	})

	shell.Register("table", func(args ...string) (string, error) {
		var buffer bytes.Buffer
		for _, node := range nodes {
			buffer.WriteString(chord.FingerTableToString(node))
		}
		return buffer.String(), nil
	})

	shell.Register("addr", func(args ...string) (string, error) {
		var buffer bytes.Buffer
		for _, node := range nodes {
			buffer.WriteString(fmt.Sprintf("Node %v: %v\n", node.Id, node.Addr))
		}
		return buffer.String(), nil
	})

	shell.Register("data", func(args ...string) (string, error) {
		var buffer bytes.Buffer
		for _, node := range nodes {
			buffer.WriteString(chord.DataStoreToString(node))
		}
		return buffer.String(), nil
	})

	shell.Register("get", func(args ...string) (string, error) {
		if len(args) > 0 {
			val, err := chord.Get(nodes[0], args[0])
			if err != nil {
				return "Error!", err
			}
			return val, nil
		}
		return "USAGE: get <key>", nil
	})

	shell.Register("put", func(args ...string) (string, error) {
		if len(args) > 1 {
			err := chord.Put(nodes[0], args[0], args[1])
			if err != nil {
				return "Error!", err
			}
			return "", nil
		}
		return "USAGE: put <key> <value>", nil
	})

	shell.Register("debug", func(args ...string) (string, error) {
		var response string
		if len(args) == 1 {
			val := strings.ToLower(args[0])
			if val == "on" || val == "true" {
				response = "Debug statements turned on"
				chord.SetDebug(true)
			} else if val == "off" || val == "false" {
				response = "Debug statements turned off"
				chord.SetDebug(false)
			} else {
				response = "USAGE: debug on|off"
			}
		} else {
			response = "USAGE: debug on|off"
		}
		return response, nil
	})

	shell.Register("help", func(args ...string) (string, error) {
		printHelp(shell)
		return "", nil
	})

	shell.Register("quit", func(args ...string) (string, error) {
		for _, node := range nodes {
			chord.ShutdownNode(node)
		}
		shell.Println("goodbye")
		shell.Stop()
		return "", nil
	})

	shell.Start()
}

func NodeStr(node *chord.Node) string {
	var succ []byte
	var pred []byte
	if node.Successor != nil {
		succ = node.Successor.Id
	}
	if node.Predecessor != nil {
		pred = node.Predecessor.Id
	}

	return fmt.Sprintf("Node %v: {succ:%v, pred:%v}", node.Id, succ, pred)
}
