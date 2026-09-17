package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"distributedkv/raft"
)

func main() {
	ids := []raft.ID{"a", "b", "c"}
	h := raft.NewHarness(ids, raft.Options{})
	h.Start()
	defer h.Stop()
	n, err := h.Leader(3 * time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("leader", n.ID(), "(commands: put k v | get k | delete k | quit)")
	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		p := strings.Fields(s.Text())
		if len(p) == 0 {
			continue
		}
		switch p[0] {
		case "put":
			if len(p) != 3 {
				fmt.Println("usage: put key value")
				continue
			}
			fmt.Println(n.Put(p[1], p[2]))
		case "get":
			if len(p) != 2 {
				fmt.Println("usage: get key")
				continue
			}
			v, ok, e := n.Get(p[1])
			fmt.Println(v, ok, e)
		case "delete":
			if len(p) != 2 {
				fmt.Println("usage: delete key")
				continue
			}
			fmt.Println(n.Delete(p[1]))
		case "quit":
			return
		default:
			fmt.Println("unknown command")
		}
	}
}
