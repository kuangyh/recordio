package recordio

// Read/Write variable size record bytes to / from io.Reader / io.Writer, default
// to use snappy compression.

// Interface and implementation aims for minimize alloc and memory copy.

import (
	"encoding/binary"
	"errors"
	"github.com/golang/snappy"
	"hash"
	"hash/crc32"
	"io"
	//"log"
)

var (
	ReadBytesErr  = errors.New("Read bytes error")
	WriteBytesErr = errors.New("Write bytes error")
	ChecksumErr   = errors.New("Checksum Error")
)

const (
	// By default RecordIO uses snappy to compress content, NoCompression disable
	// this behavior on record level.
	NoCompression = Flags(1 << iota)
)

const DefaultFlags = Flags(0)

const (
	recordHeaderStorageSize = 12 // bodyLength + flags + headerChecksum
)

type Flags uint32

type recordHeader struct {
	bodyLength uint32
	flags      Flags
}

func (header *recordHeader) encode(buf []byte) {
	if len(buf) != 12 {
		panic("recordHeader.encode with incorrect buffer size")
	}
	binary.LittleEndian.PutUint32(buf[0:4], header.bodyLength)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(header.flags))
	binary.LittleEndian.PutUint32(buf[8:12], crc32.ChecksumIEEE(buf[:8]))
}

func (header *recordHeader) decode(buf []byte) error {
	if len(buf) != 12 {
		panic("recordHeader.decode with incorrect buffer size")
	}
	headerChecksum := crc32.ChecksumIEEE(buf[:8])
	if binary.LittleEndian.Uint32(buf[8:]) != headerChecksum {
		return ChecksumErr
	}
	header.bodyLength = binary.LittleEndian.Uint32(buf[0:4])
	header.flags = Flags(binary.LittleEndian.Uint32(buf[4:8]))
	return nil
}

type checksumWriter struct {
	writer io.Writer
	crc    hash.Hash32
}

func (cw *checksumWriter) Write(p []byte) (n int, err error) {
	if n, err := cw.crc.Write(p); err != nil {
		return n, err
	}
	return cw.writer.Write(p)
}

func (cw *checksumWriter) checksum() uint32 {
	return cw.crc.Sum32()
}

type Reader struct {
	// Last error
	Err error

	bytesReader   io.Reader
	uncompressBuf []byte
}

// Create RecordIO reader from underlering io.Reader
func NewReader(reader io.Reader) *Reader {
	return &Reader{
		bytesReader:   reader,
		uncompressBuf: make([]byte, 4096),
	}
}

func (rr *Reader) err(err error) error {
	rr.Err = err
	return err
}

func (rr *Reader) readBody(header recordHeader, dst []byte) (output []byte, err error) {
	bodyLength := int(header.bodyLength)
	if len(dst) < bodyLength+4 {
		dst = make([]byte, bodyLength+4)
	} else {
		dst = dst[:bodyLength+4]
	}
	if size, _ := rr.bytesReader.Read(dst); size != len(dst) {
		return nil, rr.err(ReadBytesErr)
	}
	if crc32.ChecksumIEEE(dst[:bodyLength]) != binary.LittleEndian.Uint32(dst[bodyLength:]) {
		return nil, rr.err(ChecksumErr)
	}
	return dst[:bodyLength], nil
}

// Read next record. The returned slice may be a sub-slice of dst if dst was
// large enough to hold the entire record. Otherwise, a newly allocated slice
// will be returned. It's valid to pass nil dst
func (rr *Reader) ReadRecord(dst []byte) (output []byte, err error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	headerBytes := [recordHeaderStorageSize]byte{}
	if _, err = rr.bytesReader.Read(headerBytes[:]); err != nil {
		return nil, rr.err(err)
	}
	header := recordHeader{}
	if err = header.decode(headerBytes[:]); err != nil {
		return nil, rr.err(err)
	}
	if header.flags&NoCompression != 0 {
		return rr.readBody(header, dst)
	}
	rawBuf, err := rr.readBody(header, rr.uncompressBuf)
	if err != nil {
		return nil, err
	}
	buf, err := snappy.Decode(dst, rawBuf)
	if err != nil {
		return nil, rr.err(ReadBytesErr)
	}
	return buf, nil
}

type Writer struct {
	Err   error
	Flags Flags

	bytesWriter io.Writer
	compressBuf []byte
}

// Create RecordIO wrier that writes to underling io.Writer
func NewWriter(writer io.Writer, flags Flags) *Writer {
	return &Writer{
		Flags:       flags,
		bytesWriter: writer,
		compressBuf: make([]byte, 4096),
	}
}

func (rw *Writer) err(err error) error {
	rw.Err = err
	return err
}

// Write a record
func (rw *Writer) WriteRecord(data []byte, flags Flags) error {
	if rw.Err != nil {
		return rw.Err
	}
	flags = flags | rw.Flags
	if flags&NoCompression == 0 {
		data = snappy.Encode(rw.compressBuf, data)
	}
	header := recordHeader{bodyLength: uint32(len(data)), flags: flags}
	var headerBuf [recordHeaderStorageSize]byte
	header.encode(headerBuf[:])
	if size, _ := rw.bytesWriter.Write(headerBuf[:]); size != recordHeaderStorageSize {
		return rw.err(WriteBytesErr)
	}
	bodyWriter := checksumWriter{writer: rw.bytesWriter, crc: crc32.NewIEEE()}
	if size, _ := bodyWriter.Write(data); size != len(data) {
		return rw.err(WriteBytesErr)
	}
	var checksumBuf [4]byte
	binary.LittleEndian.PutUint32(checksumBuf[:], bodyWriter.checksum())
	if size, _ := rw.bytesWriter.Write(checksumBuf[:]); size != len(checksumBuf) {
		return rw.err(WriteBytesErr)
	}
	return nil
}
