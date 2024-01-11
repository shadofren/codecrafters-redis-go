package main

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type Item struct {
	Value  any
	Expiry *time.Time
}

type RDB struct {
	Data map[string]*Item
}

var database *RDB = &RDB{make(map[string]*Item)}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	must(err)
	defer l.Close()

	fmt.Println("Redis Server is running on port 6379...")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil && err == io.EOF {
			fmt.Println("Client is done")
			break
		}
		command := strings.Split(string(buf[:n]), "\r\n")
		fmt.Println("Received: ", command)
		args := make([]string, 0)
		for i := 2; i < len(command); i += 2 {
			args = append(args, command[i])
		}
		fmt.Println("args", args)
		fmt.Println("args[0]", args[0])
		switch args[0] {
		case "ping":
			sendPong(conn)
		case "echo":
			sendEcho(conn, args[1:])
		case "set":
			setValue(conn, args[1:])
		case "get":
			getValue(conn, args[1:])

		default:
			fmt.Println("not implemented")
		}
	}
}

func setValue(conn net.Conn, data []string) {
	key, value := data[0], data[1]

	if len(data) == 4 && data[2] == "px" { // set with expiry
		fmt.Println("with expiry")
		mili, err := strconv.Atoi(data[3])
		must(err)
		cur := time.Now()
		fmt.Println("current time", cur)
    t := cur.Add(time.Duration(mili) * time.Millisecond)
		fmt.Println("expiry time", t)
		database.Data[key] = &Item{Value: value, Expiry: &t}
	} else {
		database.Data[key] = &Item{Value: value, Expiry: nil}
  }
	conn.Write(response("OK"))
}

func getValue(conn net.Conn, data []string) {
	cur := time.Now()
	res := "$-1\r\n"
	if item, ok := database.Data[data[0]]; ok {
		fmt.Printf("got item %v\n", item)
		fmt.Println("checking current time vs expiry time", cur, item)
		if item.Expiry == nil || cur.Before(*item.Expiry) {
			res = fmt.Sprintf("+%v\r\n", item.Value)
		}
	}
	conn.Write([]byte(res))
}

func sendEcho(conn net.Conn, data []string) {
	_, err := conn.Write(response(strings.Join(data, "")))
	must(err)
}

func sendPong(conn net.Conn) {
	resp := []byte("+PONG\r\n")
	_, err := conn.Write(resp)
	must(err)
}

func response(resp string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", resp))
}

func must(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
