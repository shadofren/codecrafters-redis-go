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
	data := make(map[string]any)
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
			setValue(conn, data, args[1:])
		case "get":
			getValue(conn, data, args[1:])

		default:
			fmt.Println("not implemented")
		}
	}
}

func setValue(conn net.Conn, store map[string]any, data []string) {
	key, value := data[0], data[1]
	store[key] = value
	conn.Write(response("OK"))
}

func getValue(conn net.Conn, store map[string]any, data []string) {
  res := fmt.Sprintf("%v", store[data[0]])
  conn.Write(response(res))
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
