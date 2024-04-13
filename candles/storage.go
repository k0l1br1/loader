package candles

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	DefaultFilePerm = 0644
	DefaultDirPerm  = 0744
	DefaultDataDir  = "candles"
	DefaultExt      = ".bin"
	CandleByteSize  = 5 * 4 // 4 bytes for any field
	flagNew         = os.O_RDWR | os.O_CREATE | os.O_TRUNC
	flagAppend      = os.O_RDWR | os.O_APPEND
)

type Candle struct {
	HPrice float32
	LPrice float32
	CPrice float32
	Volume float32
	CTime  uint32
}

type Storage struct {
	fd       *os.File
	readPos  int64
	readBuf  []byte
	writeBuf [CandleByteSize]byte
}

// Create new file with default path
func NewDefaultStorage(symbol string) (*Storage, error) {
	return defaultStorage(symbol, flagNew)
}

// Use an existing file with default path
func DefaultStorage(symbol string) (*Storage, error) {
	return defaultStorage(symbol, flagAppend)
}

// Create a new file from a path
func NewFileStorage(path string) (*Storage, error) {
	dir, file := filepath.Split(path)
	return fileStorage(dir, file, flagNew)
}

// Use an existin file from a path
func FileStorage(path string) (*Storage, error) {
	dir, file := filepath.Split(path)
	return fileStorage(dir, file, flagAppend)
}

// Create default dir and file in the current directory
func defaultStorage(symbol string, flag int) (*Storage, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(wd, DefaultDataDir)
	return fileStorage(dir, symbol+DefaultExt, flag)
}

// Creates a directory if necessary and open or create a file depending on the flag
func fileStorage(dir, file string, flag int) (*Storage, error) {
	if len(file) == 0 {
		return nil, errors.New("file name is required")
	}
	if dir != "" {
		if err := os.MkdirAll(dir, DefaultDirPerm); err != nil {
			return nil, err
		}
	}
	fd, err := os.OpenFile(filepath.Join(dir, file), flag, DefaultFilePerm)
	if err != nil {
		return nil, err
	}
	return &Storage{fd: fd}, nil
}

func (s *Storage) Close() error {
	return s.fd.Close()
}

// Convert candles to bytes and write them to a file to the data file
func (s *Storage) Save(b []Candle) error {
	if len(b) == 0 {
		return nil
	}
	bs := s.writeBuf[:]
	for i := range b {
		binary.LittleEndian.PutUint32(bs[:4], math.Float32bits(b[i].HPrice))
		binary.LittleEndian.PutUint32(bs[4:8], math.Float32bits(b[i].LPrice))
		binary.LittleEndian.PutUint32(bs[8:12], math.Float32bits(b[i].CPrice))
		binary.LittleEndian.PutUint32(bs[12:16], math.Float32bits(b[i].Volume))
		binary.LittleEndian.PutUint32(bs[16:20], b[i].CTime)
		// write one candle
		if _, err := s.fd.Write(bs); err != nil {
			return err
		}
	}

	return nil
}

// Returns length in bytes for the current data file
func (s *Storage) SizeBytes() (int64, error) {
	fi, err := s.fd.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Returns length in candles for the current data file
func (s *Storage) SizeCandles() (int64, error) {
	size, err := s.SizeBytes()
	if err != nil {
		return 0, err
	}
	if size%CandleByteSize != 0 {
		return 0, errors.New("get size from a corrupted candles file")
	}
	return int64(size / CandleByteSize), nil
}

func (s *Storage) ReadAll() ([]Candle, error) {
	size, err := s.SizeBytes()
	if err != nil {
		return nil, err
	}
	size++ // one byte for final read at EOF
	bs := make([]byte, 0, size)
	var n int64
	for {
		nr, err := s.fd.Read(bs[len(bs):cap(bs)])
		n += int64(nr)
		bs = bs[:len(bs)+nr]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		if len(bs) >= cap(bs) {
			d := append(bs[:cap(bs)], 0)
			bs = d[:len(bs)]
		}
	}

	if err != nil && err != io.EOF {
		return nil, err
	}
	if n%CandleByteSize != 0 {
		return nil, errors.New("read all from a corrupted candles file")
	}
	// cast n to candles len
	n = n / CandleByteSize
	cs := make([]Candle, n)
	// bytes to candles
	bs2cs(bs, cs, len(cs))
	return cs, nil
}

// Read bytes from the end of the current data file and convert them to candles
// Returns the number of candle read and the error
func (s *Storage) ReadBack(cs []Candle) (int, error) {
	size, err := s.SizeBytes()
	if err != nil {
		return 0, err
	}
	pos := size - int64(len(cs)*CandleByteSize) - s.readPos
	if pos < 0 {
		pos = 0
	}
	return s.read(cs, pos)
}

// Read bytes from the current data file and convert them to candles
// Returns the number of candle read and the error
func (s *Storage) Read(cs []Candle) (int, error) {
	return s.read(cs, s.readPos)
}

func (s *Storage) read(cs []Candle, pos int64) (int, error) {
	nb := len(cs) * CandleByteSize
	// nil slice also has cap 0
	if nb > cap(s.readBuf) {
		s.readBuf = make([]byte, nb)
	}

	// cut or stretch after previous use
	bs := s.readBuf[:nb]
	n, err := s.fd.ReadAt(bs, pos)
	if err != nil && err != io.EOF {
		return 0, err
	}
	if n%CandleByteSize != 0 {
		return 0, errors.New("read from a corrupted candles file")
	}
	s.readPos += int64(n)

	// cast n to candles len
	n = int(n / CandleByteSize)
	// bytes to candles
	bs2cs(bs, cs, n)

	// err may be io.EOF
	return n, err
}

// return timestamp as milli seconds of the first candle
func (s *Storage) FirstCandleCloseTime() (int64, error) {
	size, err := s.SizeBytes()
	if size < CandleByteSize || err != nil {
		return 0, err
	}
	return s.readCandleCloseTime(int64(CandleByteSize))
}

// return timestamp as milli seconds of the last candle
func (s *Storage) LastCandleCloseTime() (int64, error) {
	size, err := s.SizeBytes()
	if size < CandleByteSize || err != nil {
		return 0, err
	}
	return s.readCandleCloseTime(size)
}

func (s *Storage) readCandleCloseTime(offset int64) (int64, error) {
	at := offset - 4 // 4 bytes for int32
	b := make([]byte, 4)
	n, err := s.fd.ReadAt(b, at)
	if err != nil && err != io.EOF {
		return 0, err
	}
	if n != len(b) {
		panic("BUG: length must be equal to the number of bytes read")
	}
	t := binary.LittleEndian.Uint32(b)
	return SecToMilli(t), nil
}

// convert seconds to milliseconds
func SecToMilli(t uint32) int64 {
	return int64(t) * 1000
}

func bs2cs(bs []byte, cs []Candle, n int) {
	var off int
	for i := 0; i < n; i++ {
		off = i * CandleByteSize
		cs[i].HPrice = math.Float32frombits(binary.LittleEndian.Uint32(bs[off : 4+off]))
		cs[i].LPrice = math.Float32frombits(binary.LittleEndian.Uint32(bs[4+off : 8+off]))
		cs[i].CPrice = math.Float32frombits(binary.LittleEndian.Uint32(bs[8+off : 12+off]))
		cs[i].Volume = math.Float32frombits(binary.LittleEndian.Uint32(bs[12+off : 16+off]))
		cs[i].CTime = binary.LittleEndian.Uint32(bs[16+off : 20+off])
	}
}
