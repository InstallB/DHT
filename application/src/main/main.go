package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

var n DhtNode
var ip string
var in bool

func PrintHelp() {
	fmt.Println("Instructions: ")
	fmt.Println("")
	fmt.Println("join [string:IP Address]  join the network which contains the IP")
	fmt.Println("create                    create a new network")
	fmt.Println("upload [filename]         upload the file and generate .torrent file in current directory")
	fmt.Println("download [filename]       download file by the .torrent file")
	fmt.Println("quit                      quit the network ")
	fmt.Println("exit                      quit the network and exit the application ")
	fmt.Println("help                      show all the instructions ")
	fmt.Println("")
}

func main() {
	writer, err := os.OpenFile("log.txt", os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		logrus.Fatal("create file log.txt failed: %v", err)
	}
	logrus.SetOutput(io.MultiWriter(writer))

	fmt.Println("Enter your IP Adress to start")
	_, _ = fmt.Scanln(&ip)
	n = NewNode(ip)
	n.Run()
	in = false
	fmt.Println("Succesfully Set IP")
	fmt.Println("")
	PrintHelp()
	for {
		var s1, s2 string
		s1 = ""
		s2 = ""
		_, _ = fmt.Scanln(&s1, &s2)
		if s1 == "join" {
			if in {
				fmt.Println("Already in a Network, please first quit")
				continue
			}
			ok := n.Join(s2)
			if ok {
				in = true
				fmt.Println("Joined Successfully")
			} else {
				fmt.Println("Failed to Join Node")
			}
			continue
		}
		if s1 == "create" {
			if in {
				fmt.Println("Already in a Network, please first quit")
				continue
			}
			in = true
			n.Create()
			fmt.Println("New Network Created Successfully")
			continue
		}
		if s1 == "upload" {
			if !in {
				fmt.Println("Not in any network, please join a network or create one first")
				continue
			}
			err := Upload(n, s2)
			if err != nil {
				fmt.Println("Failed to Upload File, Error: ", err)
			}
			continue
		}
		if s1 == "download" {
			if !in {
				fmt.Println("Not in any network, please join a network or create one first")
				continue
			}
			err := Download(n, s2)
			if err != nil {
				fmt.Println("Failed to Download File, Error: ", err)
			}
			continue
		}
		if s1 == "quit" {
			if !in {
				fmt.Println("Not in any Network, can't quit")
				continue
			}
			in = false
			n.Quit()
			fmt.Println("Node Quit Successfully")
			continue
		}
		if s1 == "help" {
			PrintHelp()
			continue
		}
		if s1 == "exit" {
			if !in {
				n.Quit()
			}
			break
		}
	}
	fmt.Println("Program Exit")
}
