package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Item struct {
	Value  any
	Expiry *time.Time
}

type Database struct {
	ID             uint8
	HashSize       uint64
	ExpireHashSize uint64
	Map            map[string]*Item // use a single map for now
}

type RDB struct {
	Magic   [5]byte
	Version [4]byte
	Aux     map[string]string
	DB      []Database
}

func NewRDB() *RDB {
	return &RDB{Aux: make(map[string]string), DB: []Database{*NewDatabase(0)}}
}

func NewDatabase(id uint8) *Database {
	return &Database{ID: id, Map: make(map[string]*Item)}
}

// OpCode
const (
	EOF          = byte(0xff)
	SELECTDB     = byte(0xfe)
	EXPIRETIME   = byte(0xfd)
	EXPIRETIMEMS = byte(0xfc)
	RESIZEDB     = byte(0xfb)
	AUX          = byte(0xfa)
)

/* ValueType */
const (
	StringEncoding           = 0
	ListEncoding             = 1
	SetEncoding              = 2
	SortedSetEncoding        = 3
	HashEncoding             = 4
	ZipmapEncoding           = 9
	ZiplistEncoding          = 10
	IntsetEncoding           = 11
	SortedSetZiplistEncoding = 12
	HashmapZiplistEncoding   = 13
	ListQuicklistEncoding    = 14
)

func parseRDB(file *os.File) (*RDB, error) {
	reader := bufio.NewReader(file)
	rdb := NewRDB()
	_, err := reader.Read(rdb.Magic[:])
	must(err)
	_, err = reader.Read(rdb.Version[:])
	must(err)
	fmt.Println(string(rdb.Magic[:]))
	fmt.Println(string(rdb.Version[:]))
loop:
	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			return NewRDB(), err
		}
		var dbIdx uint8

		switch opcode {
		case AUX:
			key := parseString(reader)
			value := parseString(reader)
			fmt.Printf("AUX - %s:%s\n", key, value)
			rdb.Aux[key] = value
		case SELECTDB:
			dbIdx, err = reader.ReadByte()
			must(err)
		case RESIZEDB:
			rdb.DB[dbIdx].HashSize = parseInt(reader)
			rdb.DB[dbIdx].ExpireHashSize = parseInt(reader)
		case EXPIRETIME:
      var timeInSeconds uint32
      binary.Read(reader, binary.LittleEndian, &timeInSeconds)
      expireTime := time.Unix(int64(timeInSeconds), 0)
			key, value, err := extractKey(reader)
			must(err)
			rdb.DB[dbIdx].Map[key] = &Item{Value: value, Expiry: &expireTime}
      break loop
		case EXPIRETIMEMS:
      var timeInMs uint64
      binary.Read(reader, binary.LittleEndian, &timeInMs)
      expireTime := time.UnixMilli(int64(timeInMs))
			key, value, err := extractKey(reader)
			must(err)
			rdb.DB[dbIdx].Map[key] = &Item{Value: value, Expiry: &expireTime}
		case EOF: // no more data
			fmt.Println("reach end of file")
			break loop
		default:
			// key extraction
			fmt.Printf("encounter code %x\n", opcode)
			reader.UnreadByte()
			key, value, err := extractKey(reader)
			must(err)
			rdb.DB[dbIdx].Map[key] = &Item{Value: value}
		}
	}
	return rdb, nil
}

func extractKey(reader *bufio.Reader) (string, any, error) {
	valueType, err := reader.ReadByte()
	must(err)
	key := parseString(reader) // key is always string
	switch valueType {
	case StringEncoding:
		value := parseString(reader)
    fmt.Printf("found data %s:%v\n", key, value)
		return key, value, nil
	default:
		fmt.Printf("type unsupported %x", valueType)
		return "", nil, fmt.Errorf("unsupported")
	}
}

func parseString(reader *bufio.Reader) string {
	/* 00 	The next 6 bits represent the length */
	/* 01 	Read one additional byte. The combined 14 bits represent the length */
	/* 10 	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length */
	/* 11 	The next object is encoded in a special format. The remaining 6 bits indicate the format. May be used to store numbers or Strings, see String Encoding */
	/**/
	/* As a result of this encoding: */
	/**/
	/*     Numbers up to and including 63 can be stored in 1 byte */
	/*     Numbers up to and including 16383 can be stored in 2 bytes */
	/*     Numbers up to 2^32 -1 can be stored in 4 bytes */

	firstByte, err := reader.ReadByte()
	must(err)
	switch firstByte >> 6 {
	case 0:
		length := int(firstByte)
		// read the string
		value := make([]byte, length)
		_, err := reader.Read(value)
		must(err)
		return string(value)
	case 1:
		fmt.Println("01 not implemented")
	case 2:
		fmt.Println("10 not implemented")
	case 3:
		// the next 6 bit indicate the format
		firstByte &= byte(0x3f)
		if firstByte == 0 {
			value, err := reader.ReadByte()
			must(err)
			return strconv.Itoa(int(value))
		} else if firstByte == 1 {
			var value uint16
			err := binary.Read(reader, binary.BigEndian, &value)
			must(err)
			return strconv.FormatUint(uint64(value), 10)
		} else if firstByte == 2 {
			var value uint32
			err := binary.Read(reader, binary.BigEndian, &value)
			must(err)
			return strconv.FormatUint(uint64(value), 10)
		} else {
			fmt.Println("is something else")
		}
	}
	return ""
}

func parseInt(reader *bufio.Reader) uint64 {
	/* If the first 2 bits are 00, the next 6 bits represent the integer directly (6 bits integer). */
	/* If the first 2 bits are 01, the next byte is the integer (14 bits integer). */
	/* If the first 2 bits are 10, the next 4 bytes represent the integer (32 bits integer). */
	/* If the first 2 bits are 11, the next 8 bytes represent the integer (64 bits integer). */
	firstByte, err := reader.ReadByte()
	must(err)
	switch firstByte >> 6 {
	case 0:
		return uint64(firstByte & 0x3F)
	case 1:
		secondByte, err := reader.ReadByte()
		must(err)
		return uint64(firstByte&0x3F)<<8 | uint64(secondByte)
	case 2:
		var value uint32
		binary.Read(reader, binary.BigEndian, &value)
		return uint64(value)
	case 3:
		var value uint64
		binary.Read(reader, binary.BigEndian, &value)
		return value
	default:
	}
	return 0
}
