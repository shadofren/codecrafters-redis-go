package main

import (
	"fmt"
	"strconv"
)

// Type of RESP
type Type byte

const (
	Integer = ':'
	String  = '+'
	Bulk    = '$'
	Array   = '*'
	Error   = '-'
)

type RESP struct {
	Type  Type
	Data  []*RESP
	Raw   []byte
	Count int
}

func (r *RESP) Pack() []byte {
	data := make([]byte, 0)
	data = append(data, byte(r.Type))
	switch r.Type {
	case Array:
		data = append(data, intToByte(r.Count)...)
		data = append(data, '\r', '\n')
		for i := 0; i < r.Count; i++ {
			// pack the individual array element
			data = append(data, r.Data[i].Pack()...)
		}
	case Bulk:
		data = append(data, intToByte(r.Count)...)
		data = append(data, '\r', '\n')
		data = append(data, r.Raw...)
		data = append(data, '\r', '\n')
	case String:
		data = append(data, r.Raw...)
		data = append(data, '\r', '\n')
	case Integer:
	case Error:
	default:
		fmt.Println("not supported")
	}
	return data
}

func intToByte(num int) []byte {
	// Convert int to string
	strNum := strconv.Itoa(num)
	// Convert string to byte
	return []byte(strNum)
}
