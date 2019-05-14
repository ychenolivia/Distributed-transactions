package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"
)

var reader = bufio.NewReader(os.Stdin)

var connmap = make(map[string]*rpc.Client)

var optlist []string

var commitmap = make(map[string][]string)

var abortmap = make(map[string][]string)

var allflag bool

type SSet struct {
	Objname, Objvalue string
}
type SSendto struct {
	Objname, Objvalue string
}

func connect() {
	sA, err := rpc.Dial("tcp", "172.22.94.28:8080")
	if err != nil {
		log.Fatal("sA rpc dial error", err)
	}
	connmap["A"] = sA
	sB, err := rpc.Dial("tcp", "172.22.158.29:8080")
	if err != nil {
		//log.Fatal("sB rpc dial error", err)
		fmt.Println("sB rpc dial error",err)
	}
	connmap["B"]=sB
	sC, err := rpc.Dial("tcp", "172.22.156.29:8080")
	if err != nil {
		//log.Fatal("sC rpc dial error", err)
		fmt.Println("sC rpc dial error",err)
	}
	connmap["C"]=sC
	sD, err := rpc.Dial("tcp", "172.22.94.29:8080")
	if err != nil {
		//log.Fatal("sD rpc dial error", err)
		fmt.Println("sD rpc dial error",err)
	}
	connmap["D"]=sD
	sE, err := rpc.Dial("tcp", "172.22.156.30:8080")
	if err != nil {
		//log.Fatal("sE rpc dial error", err)
		fmt.Println("sE rpc dial error",err)
	}
	connmap["E"]=sE
}

func main() {
	connect()
	allflag = false
	fmt.Println("You can BEGIN a transaction now:\n")
	for {
		read_line, _, _ := reader.ReadLine()
		read_line_msg := string(read_line)
		sreader := strings.Split(read_line_msg, " ")
		flag := false
		uflag := false
		if read_line_msg == "BEGIN" {
			flag = true
			if len(optlist)>0{
				fmt.Println("Please Commit or abort your transaction before")
			}
			optlist = append(optlist, "BEGIN")
			fmt.Println("OK")
		}
		if len(optlist)>0{
		
				//SET OPERATION
				if sreader[0] == "SET" {
					if allflag == true {
						fmt.Println("Wait for RWlock, however you can abort")
					}
					flag = true
					objsplit := strings.Split(sreader[1], ".")
					if _, ok := connmap[objsplit[0]]; ok {
						setcmd := SSet{objsplit[1], sreader[2]}
						//judge past cmd
						for u := len(commitmap[objsplit[0]]) - 1; u >= 0; u-- {
							uobj := strings.Split(commitmap[objsplit[0]][u], " ")
							suobj := strings.Split(uobj[1], ".")
							if objsplit[1] == suobj[1] {
								//writew set
								if uobj[0] == "SET" {
									var reply SSet
									allflag = true
									err := connmap[objsplit[0]].Call("Num.WriteW", setcmd, &reply)
									if err != nil {
										log.Fatal("Call Set error", err)
									}
									allflag = false
									//fmt.Printf("SET result:%s,%s,%s,%s\n", setcmd.Objname, setcmd.Objvalue, reply.Objname, reply.Objvalue)
									fmt.Println("OK")
									optlist = append(optlist, read_line_msg)
									commitmap[objsplit[0]]=append(commitmap[objsplit[0]][:u],commitmap[objsplit[0]][u+1:]...)
									commitmap[objsplit[0]] = append(commitmap[objsplit[0]], read_line_msg)
									read_line_msg="SET "+objsplit[0]+"."+reply.Objname+" "+reply.Objvalue
									abortmap[objsplit[0]]=append(abortmap[objsplit[0]][:u],abortmap[objsplit[0]][u+1:]...)
									abortmap[objsplit[0]] = append(abortmap[objsplit[0]], read_line_msg)
									uflag = true
									break
								}
								//upgrade set
								if uobj[0] == "GET" {
									var reply SSet
									allflag = true
									err := connmap[objsplit[0]].Call("Num.Upgrade", setcmd, &reply)
									if err != nil {
										log.Fatal("Call Set error", err)
									}
									allflag = false
									//fmt.Printf("SET result:%s,%s,%s,%s\n", setcmd.Objname, setcmd.Objvalue, reply.Objname, reply.Objvalue)
									fmt.Println("OK")
									optlist = append(optlist, read_line_msg)
									commitmap[objsplit[0]]=append(commitmap[objsplit[0]][:u],commitmap[objsplit[0]][u+1:]...)
									commitmap[objsplit[0]] = append(commitmap[objsplit[0]], read_line_msg)
									read_line_msg="SET "+objsplit[0]+"."+reply.Objname+" "+reply.Objvalue
									abortmap[objsplit[0]]=append(abortmap[objsplit[0]][:u],abortmap[objsplit[0]][u+1:]...)
									abortmap[objsplit[0]] = append(abortmap[objsplit[0]], read_line_msg)
									uflag = true
									break
								}
							}
						}
						//normal set
						if uflag == false {
							var reply SSet
							allflag = true
							err := connmap[objsplit[0]].Call("Num.Set", setcmd, &reply)
							if err != nil {
								log.Fatal("Call Set error", err)
							}
							allflag = false
							//fmt.Printf("SET result:%s,%s,%s,%s\n", setcmd.Objname, setcmd.Objvalue, reply.Objname, reply.Objvalue)
							fmt.Println("OK")
							optlist = append(optlist, read_line_msg)
							commitmap[objsplit[0]] = append(commitmap[objsplit[0]], read_line_msg)							
							read_line_msg="SET "+objsplit[0]+"."+reply.Objname+" "+reply.Objvalue
							abortmap[objsplit[0]] = append(abortmap[objsplit[0]], read_line_msg)
							
						}
					} else {
						fmt.Printf("���������server")
					}
				}
				//GET OPERATION
				if sreader[0] == "GET" {
					if allflag == true {
						fmt.Println("Wait for RWlock, however you can abort")
					}
					flag = true
					objsplit := strings.Split(sreader[1], ".")
					getcmd := objsplit[1]
					if _, ok := connmap[objsplit[0]]; ok {
						abortmap[objsplit[0]] = append(abortmap[objsplit[0]], read_line_msg)
						var reply SSendto
						allflag = true
						err := connmap[objsplit[0]].Call("Num.Get", getcmd, &reply)
						if err != nil {
							log.Fatal("Call Get error", err)
						}
						allflag = false
						fmt.Printf("GET result:%s,%s,%s\n", getcmd, reply.Objname, reply.Objvalue)
						optlist = append(optlist, read_line_msg)
						commitmap[objsplit[0]] = append(commitmap[objsplit[0]], read_line_msg)
						if reply.Objvalue=="not found"{
							fmt.Println("ABORTED")
							var restr string
							allflag = true
							for key := range abortmap {
								err := connmap[key].Call("Num.Abort", abortmap[key], &restr)
								if err != nil {
									log.Fatal("call abort error", err)
								}
								fmt.Printf("abort info:%s,%s\n", read_line_msg, restr)
								commitmap[key]=commitmap[key][:0]
								abortmap[key]=abortmap[key][:0]
							}
							allflag = false
							optlist = append(optlist, read_line_msg)
							optlist=optlist[:0]
						}
						
					} else {
						fmt.Printf("���������server")
					}

				}
				if read_line_msg == "COMMIT" {
					if allflag == true {
						fmt.Println("You can not Commit when last operation not finished")
					}
					flag = true
					var restr string
					allflag = true
					for key := range commitmap {
						err := connmap[key].Call("Num.Commit", commitmap[key], &restr)
						if err != nil {
							log.Fatal("call commit error", err)
						}
						//fmt.Printf("commit info:%s,%s\n", read_line_msg, restr)
						fmt.Println("COMMIT OK")
						if restr=="COMMIT OK"{
							commitmap[key]=commitmap[key][:0]
							abortmap[key]=abortmap[key][:0]
						}else{
							//abort
						}
					}
					allflag = false
					/*templist=append("",optlist[1:])
					for i :=range templist{
						slist= strings.Split(templist[i], " ")
						if slist[0]=="GET"{
							sslist=strings.Split(templist[i], ".")
						}
					}*/

					optlist = append(optlist, read_line_msg)
					optlist=optlist[:0]
				}
				if read_line_msg == "ABORT" {
					flag = true
					var restr string
					allflag = true
					fmt.Println("ABORTED")
					for key := range abortmap {
						err := connmap[key].Call("Num.Abort", abortmap[key], &restr)
						if err != nil {
							log.Fatal("call abort error", err)
						}
						//fmt.Printf("abort info:%s,%s\n", read_line_msg, restr)
						commitmap[key]=commitmap[key][:0]
						abortmap[key]=abortmap[key][:0]
					}
					allflag = false
					optlist = append(optlist, read_line_msg)
					optlist=optlist[:0]
				}
				if flag == false {
					fmt.Printf("The command is writing in wrong way\n")
				}
				
		}else{
			fmt.Println("Please BEGIN a new Transaction")
		}

	}

}
