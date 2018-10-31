// Package recordio implements simple var length record format
package recordio

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

var (
	// ErrIncompleteHeader indicates cannot read or write complete header
	ErrIncompleteHeader = errors.New("incomplete header")
	// ErrChecksumFailed indicated CRC checksum failed
	ErrChecksumFailed = errors.New("checksum failed")
	// ErrRecordTooLarge returned when reading a record larger than allowed, this often indicates data corrupted
	ErrRecordTooLarge = errors.New("record to large")
)

type recordHeader struct {
	bodyLen uint32
	bodyCRC uint32
}

type recordHeaderBytes [8]byte

// Writer writes record to a basic io.Writer with headers
type Writer struct {
	IO io.Writer
}

func (w *Writer) Write(b []byte) (int, error) {
	var hb recordHeaderBytes
	putHeader(hb[:], recordHeader{
		bodyLen: uint32(len(b)),
		bodyCRC: crc32.ChecksumIEEE(b),
	})

	if n, err := w.IO.Write(hb[:]); err != nil || n < len(hb) {
		if err == nil {
			err = ErrIncompleteHeader
		}
		return 0, err
	}
	return w.IO.Write(b)
}

// Reader reads records from io.Reader
type Reader struct {
	IO            io.Reader
	MaxRecordSize int
}

// Next reads next record from reader, if size of next record smaller than len(buf),
// memory of buf will be used in the returning slice, otherwise, new memory will be allocated.
// when there's no next record, io.EOF will be returned.
func (r *Reader) Next(buf []byte) ([]byte, error) {
	var hb recordHeaderBytes
	if _, err := io.ReadFull(r.IO, hb[:]); err != nil {
		return nil, err
	}
	h := getHeader(hb[:])
	if r.MaxRecordSize > 0 && int(h.bodyLen) > r.MaxRecordSize {
		return nil, ErrRecordTooLarge
	}
	if int(h.bodyLen) > len(buf) {
		buf = make([]byte, h.bodyLen)
	} else {
		buf = buf[:h.bodyLen]
	}
	if _, err := io.ReadFull(r.IO, buf); err != nil {
		return nil, err
	}
	if h.bodyCRC != crc32.ChecksumIEEE(buf) {
		return nil, ErrChecksumFailed
	}
	return buf, nil
}

func putHeader(dst []byte, header recordHeader) {
	binary.LittleEndian.PutUint32(dst, header.bodyLen)
	binary.LittleEndian.PutUint32(dst[4:], header.bodyCRC)
}

func getHeader(src []byte) recordHeader {
	var h recordHeader
	h.bodyLen = binary.LittleEndian.Uint32(src)
	h.bodyCRC = binary.LittleEndian.Uint32(src[4:])
	return h
}
