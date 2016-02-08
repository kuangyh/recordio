package recordio

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"github.com/golang/snappy"
	"hash/crc32"
	"io"
)

var (
	ErrReadBytes  = errors.New("Read bytes error")
	ErrWriteBytes = errors.New("Write bytes error")
	ErrChecksum   = errors.New("Checksum Error")
)

const (
	GzipCompress   = 1 << iota
	SnappyCompress = 1 << iota
	BodyChecksum   = 1 << iota
)

const DefaultFlags = BodyChecksum | SnappyCompress

const (
	recordHeaderSize = 16
)

type Flags uint32

type recordHeader struct {
	bodyLength   uint32
	flags        Flags
	bodyChecksum uint32
}

func (header *recordHeader) MarshalBinary() (data []byte, err error) {
	output := [16]byte{}
	binary.LittleEndian.PutUint32(output[:4], header.bodyLength)
	binary.LittleEndian.PutUint32(output[4:8], uint32(header.flags))
	binary.LittleEndian.PutUint32(output[8:12], header.bodyChecksum)
	binary.LittleEndian.PutUint32(output[12:16], crc32.ChecksumIEEE(output[:12]))
	return output[:], nil
}

func (header *recordHeader) UnmarshalBinary(data []byte) error {
	if len(data) < recordHeaderSize {
		return ErrReadBytes
	}
	headerChecksum := binary.LittleEndian.Uint32(data[12:16])
	if headerChecksum != crc32.ChecksumIEEE(data[:12]) {
		return ErrChecksum
	}
	header.bodyLength = binary.LittleEndian.Uint32(data[:4])
	header.flags = Flags(binary.LittleEndian.Uint32(data[4:8]))
	header.bodyChecksum = binary.LittleEndian.Uint32(data[8:12])
	return nil
}

type Reader struct {
	bytesReader      io.Reader
	Options          Flags
	BytesReaderError error
	LastError        error
}

func NewReader(reader io.Reader, options Flags) *Reader {
	return &Reader{
		bytesReader: reader,
		Options:     options,
	}
}

func (rr *Reader) err(err error, bytesReaderError error) error {
	rr.LastError = err
	rr.BytesReaderError = bytesReaderError
	return err
}

func (rr *Reader) ReadRecord() ([]byte, error) {
	if rr.LastError != nil {
		return nil, rr.LastError
	}
	headerBytes := [recordHeaderSize]byte{}
	if _, err := rr.bytesReader.Read(headerBytes[:]); err != nil {
		if err == io.EOF {
			return nil, rr.err(io.EOF, io.EOF)
		} else {
			return nil, rr.err(ErrReadBytes, err)
		}
	}
	header := recordHeader{}
	if err := header.UnmarshalBinary(headerBytes[:]); err != nil {
		return nil, rr.err(err, nil)
	}
	rawBytes := make([]byte, header.bodyLength)
	if size, err := rr.bytesReader.Read(rawBytes); err != nil || uint32(size) != header.bodyLength {
		return nil, rr.err(ErrReadBytes, err)
	}

	if rr.Options&BodyChecksum == BodyChecksum && header.flags&BodyChecksum == BodyChecksum {
		if header.bodyChecksum != crc32.ChecksumIEEE(rawBytes) {
			return nil, rr.err(ErrChecksum, nil)
		}
	}

	if header.flags&GzipCompress == GzipCompress {
		gzipReader, err := gzip.NewReader(bytes.NewReader(rawBytes))
		if err != nil {
			return nil, rr.err(ErrReadBytes, err)
		}
		defer gzipReader.Close()
		buf := &bytes.Buffer{}
		buf.Grow(int(header.bodyLength * 2))
		_, err = io.Copy(buf, gzipReader)
		if err != nil {
			return nil, rr.err(ErrReadBytes, err)
		}
		return buf.Bytes(), nil
	} else if header.flags&SnappyCompress == SnappyCompress {
		uncompressed, err := snappy.Decode(nil, rawBytes)
		if err != nil {
			return nil, rr.err(ErrReadBytes, err)
		}
		return uncompressed, nil
	} else {
		return rawBytes, nil
	}
}

type Writer struct {
	bytesWriter      io.Writer
	Options          Flags
	BytesWriterError error
	LastError        error
}

func NewWriter(writer io.Writer, options Flags) *Writer {
	return &Writer{
		bytesWriter: writer,
		Options:     options,
	}
}

func (rw *Writer) err(err error, bytesWriterError error) error {
	rw.LastError = err
	rw.BytesWriterError = bytesWriterError
	return err
}

func (rw *Writer) WriteRecord(data []byte, additionalFlags Flags) (size int, err error) {
	if rw.LastError != nil {
		return 0, rw.LastError
	}
	compressedData := data
	flags := additionalFlags | rw.Options
	if flags&GzipCompress == GzipCompress {
		buf := bytes.NewBuffer(make([]byte, 0, len(data)))
		gzipWriter := gzip.NewWriter(buf)
		defer gzipWriter.Close()
		if _, err := gzipWriter.Write(data); err != nil {
			return 0, rw.err(ErrWriteBytes, err)
		}
		if err = gzipWriter.Close(); err != nil {
			return 0, rw.err(ErrWriteBytes, err)
		}
		compressedData = buf.Bytes()
	} else if flags&SnappyCompress == SnappyCompress {
		compressedData = snappy.Encode(nil, data)
	} else {
		compressedData = data
	}

	header := recordHeader{
		bodyLength: uint32(len(compressedData)),
		flags:      flags,
	}
	if flags&BodyChecksum == BodyChecksum {
		header.bodyChecksum = crc32.ChecksumIEEE(compressedData)
	}
	headerBin, err := header.MarshalBinary()
	if err != nil {
		return 0, rw.err(err, nil)
	}

	totalSize := 0
	if size, err = rw.bytesWriter.Write(headerBin); size != len(headerBin) || err != nil {
		return totalSize + size, rw.err(ErrWriteBytes, err)
	}
	totalSize += size
	if size, err = rw.bytesWriter.Write(compressedData); size != len(compressedData) || err != nil {
		return totalSize + size, rw.err(ErrWriteBytes, err)
	}
	totalSize += size
	return totalSize, nil
}

// io.Writer
func (rw *Writer) Write(data []byte) (n int, err error) {
	return rw.WriteRecord(data, 0)
}
