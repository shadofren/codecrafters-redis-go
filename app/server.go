package main

import (
	"fmt"
	"io"
	"net"
	"strings"
)

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
    if args[0] == "ping" {
      sendPong(conn)
    } else if args[0] == "echo" {
      sendEcho(conn, args[1:])
    }
	}
}

func sendEcho(conn net.Conn, data []string) {
	resp := []byte("+" + strings.Join(data, ""))
  fmt.Println("sending", string(resp))
	_, err := conn.Write(resp)
	must(err)
}

func sendPong(conn net.Conn) {
	resp := []byte("+PONG\r\n")
	_, err := conn.Write(resp)
	must(err)
}

func must(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
