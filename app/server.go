package main

import (
	"fmt"
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
	n, err := conn.Read(buf)
	must(err)
	fmt.Println("Received: ", buf[:n])
	fmt.Println("Received: ", string(buf[:n]))
  resp := []byte("+PONG\r\n")
  conn.Write(resp)
}

func must(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
