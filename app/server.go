package main

import (
	"fmt"
	"io"
	"net"
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
		fmt.Println("Received: ", buf[:n])
		fmt.Println("Received: ", string(buf[:n]))
    sendPong(conn)
	}
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
