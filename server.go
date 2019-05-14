package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

type OBJ struct {
	value string
	vlock sync.RWMutex
}

var objmap = make(map[string]*OBJ)

func init() {
	fmt.Println("RPC Server Code init:")
}

type Set struct {
	Objname, Objvalue string
}

type Sendto struct {
	Objname, Objvalue string
}

type Num int

func (n *Num) Set(args *Set, reply *Set) error {
	if _, ok := objmap[args.Objname]; ok {
		objmap[args.Objname].vlock.Lock()
		reply.Objvalue = objmap[args.Objname].value
		objmap[args.Objname].value = args.Objvalue
		reply.Objname = args.Objname
	} else {
		newobj := &OBJ{
			value: args.Objvalue}
		objmap[args.Objname] = newobj
		objmap[args.Objname].vlock.Lock()
		reply.Objname = args.Objname
		reply.Objvalue = "null"
	}
	return nil
}

func (n *Num) Get(args string, reply *Sendto) error {
	if _, ok := objmap[args]; ok {
		objmap[args].vlock.RLock()
		reply.Objname = args
		reply.Objvalue = objmap[args].value
	} else {
		reply.Objname = args
		reply.Objvalue = "not found"
	}
	return nil
}

func (n *Num) Upgrade(args *Set, reply *Set) error {
	if _, ok := objmap[args.Objname]; ok {
		objmap[args.Objname].vlock.RUnlock()
		objmap[args.Objname].vlock.Lock()
		reply.Objvalue = objmap[args.Objname].value
		objmap[args.Objname].value = args.Objvalue
		reply.Objname = args.Objname
		
	} /*else {
		newobj := &OBJ{
			value: args.Objvalue}
		objmap[args.Objname] = newobj
		objmap[args.Objname].vlock.Lock()
		reply.Objname = args.Objname
		reply.Objvalue = objmap[args.Objname].value
	}*/
	return nil
}

func (n *Num) WriteW(args *Set, reply *Set) error {
	if _, ok := objmap[args.Objname]; ok {
		objmap[args.Objname].vlock.Unlock()
		objmap[args.Objname].vlock.Lock()
		reply.Objvalue = objmap[args.Objname].value
		objmap[args.Objname].value = args.Objvalue
		reply.Objname = args.Objname
		
	} /*else {
		newobj := &OBJ{
			value: args.Objvalue}
		objmap[args.Objname] = newobj
		objmap[args.Objname].vlock.Lock()
		reply.Objname = args.Objname
		reply.Objvalue = objmap[args.Objname].value
	}*/
	return nil
}

func (n *Num) Abort(templist []string, reply *string) error {
	for i := range templist {
		slist := strings.Split(templist[i], " ")
		if slist[0] == "GET" {
			sslist := strings.Split(slist[1], ".")
			if _, ok := objmap[sslist[1]]; ok {
			objmap[sslist[1]].vlock.RUnlock()
			}
		}
		if slist[0] == "SET" {
			sslist := strings.Split(slist[1], ".")
			objmap[sslist[1]].vlock.Unlock()
			if slist[2]=="null"{
				delete(objmap,sslist[1])
				fmt.Println("delete successful")
			}else{
				objmap[sslist[1]].vlock.Lock()
				objmap[sslist[1]].value=slist[2]
				objmap[sslist[1]].vlock.Unlock()
			}
		}
	}
	*reply = "ABORTED"
	return nil
}

func (n *Num) Commit(templist []string, reply *string) error {
	for i := range templist {
		slist := strings.Split(templist[i], " ")
		if slist[0] == "GET" {
			sslist := strings.Split(slist[1], ".")
			defer objmap[sslist[1]].vlock.RUnlock()
		}
		if slist[0] == "SET" {
			sslist := strings.Split(slist[1], ".")
			defer objmap[sslist[1]].vlock.Unlock()
		}
	}
	*reply = "COMMIT OK"
	return nil
}

func add_receiver(conn net.Conn) {
	num := new(Num)
	rpc.Register(num)
	rpc.ServeConn(conn)
}

func main() {

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":8080")

	if err != nil {
		fmt.Println("resolvetcpaddres error")
		os.Exit(1)
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go add_receiver(conn)

	}
}
