package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var rdb *RDB
var dir string
var dbfilename string

func main() {
	flag.StringVar(&dir, "dir", "", "Location of the rdb config")
	flag.StringVar(&dbfilename, "dbfilename", "", "Location of the rdb config")
	flag.Parse()

  filename := filepath.Join(dir, dbfilename)
	file, err := os.Open(filename)
	if err != nil {
    fmt.Println("file doesn't exist", filename) 
	}
	defer file.Close()
  rdb, err = parseRDB(file)
  must(err)

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
		switch args[0] {
		case "ping":
			sendPong(conn)
		case "echo":
			sendEcho(conn, args[1:])
		case "set":
			setValue(conn, args[1:])
		case "get":
			getValue(conn, args[1:])
		case "config":
			config(conn, args[1:])
    case "keys":
      getKeys(conn, args[1:])
		default:
			fmt.Println("not implemented")
		}
	}
}

func getKeys(conn net.Conn, data []string) {
  fmt.Println("data is", data)
  key := data[0]
  allKeys := &RESP{Type: Array, Data: make([]*RESP, 0)}
  if key == "*" {
    fmt.Println()
    for k, _ := range rdb.DB[0].Map {
      resp := &RESP{Type: Bulk, Count: len(k), Raw: []byte(k)}
      allKeys.Data = append(allKeys.Data, resp)
      allKeys.Count++
    }
    conn.Write(allKeys.Pack())
  } else {
    fmt.Println("not supported keys", data)
  }
}

func config(conn net.Conn, data []string) {
	// data[0] == get
	key := data[1]
	resp := RESP{Type: Array, Count: 2, Data: make([]*RESP, 0)}
	resp.Data = append(resp.Data, &RESP{Type: Bulk, Count: len(key), Raw: []byte(key)})
	switch key {
	case "dir":
		resp.Data = append(resp.Data, &RESP{Type: Bulk, Count: len(dir), Raw: []byte(dir)})
	case "dbfilename":
		resp.Data = append(resp.Data, &RESP{Type: Bulk, Count: len(dbfilename), Raw: []byte(dbfilename)})
	default:
	}
	conn.Write(resp.Pack())
}

func setValue(conn net.Conn, data []string) {
	key, value := data[0], data[1]
	if len(data) == 4 && data[2] == "px" { // set with expiry
		mili, err := strconv.Atoi(data[3])
		must(err)
		cur := time.Now()
		t := cur.Add(time.Duration(mili) * time.Millisecond)
		rdb.DB[0].Map[key] = &Item{Value: value, Expiry: &t}
	} else {
		rdb.DB[0].Map[key] = &Item{Value: value, Expiry: nil}
	}
	sendOk(conn)
}

func getValue(conn net.Conn, data []string) {
	cur := time.Now()
	resp := RESP{Type: Bulk, Count: -1}
  key := data[0]
	if item, ok := rdb.DB[0].Map[key]; ok {
		if item.Expiry == nil || cur.Before(*item.Expiry) {
			if value, ok := item.Value.(string); ok {
				resp = RESP{Type: String, Raw: []byte(value)}
			}
		}
	}
	conn.Write(resp.Pack())
}

func sendEcho(conn net.Conn, data []string) {
	resp := RESP{Type: String, Raw: []byte(strings.Join(data, ""))}
	_, err := conn.Write(resp.Pack())
	must(err)
}

func sendPong(conn net.Conn) {
	resp := RESP{Type: String, Raw: []byte("PONG")}
	_, err := conn.Write(resp.Pack())
	must(err)
}

func sendOk(conn net.Conn) {
	resp := RESP{Type: String, Raw: []byte("OK")}
	_, err := conn.Write(resp.Pack())
	must(err)
}

func must(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
