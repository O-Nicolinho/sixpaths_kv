package sixpaths_kvs

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
)

type WAL struct {
	f      *os.File
	path   string
	offset int64
	bw     *bufio.Writer
	hdrLen int
}

var walHeader = []byte("WALv1-BE\x00")

type Record struct {
	LogIndex uint64
	Cmd      Command
}

func NewWAL(path string) (*WAL, error) {

	// we create the skeleton of the WAL we're going to return
	var newWAL *WAL = new(WAL)

	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0o755)
	// checks whether directory specified by path exists.
	if err != nil {
		return nil, err
	}

	// if the file doesn't exist, we create one.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)

	if err != nil {
		return nil, err
	}

	// big buffer
	bw := bufio.NewWriterSize(f, 64<<10)

	info, err := f.Stat()

	if err != nil {
		return nil, err
	}

	// Check if the file is empty
	if info.Size() <= 0 {
		// If it is, we write the header to it.
		offset, err := f.Write(walHeader)

		if err != nil {
			return nil, err
		}

		err = f.Sync()
		if err != nil {
			return nil, err
		}
		// Update the offset for our new file.
		newWAL.offset = int64(offset)

	} else {
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}

		hdr := make([]byte, len(walHeader))
		_, err = io.ReadFull(f, hdr)
		if err != nil {
			return nil, err
		}

		if !bytes.Equal(hdr, walHeader) {
			return nil, fmt.Errorf("bad WAL header: expected %q", walHeader)
		}
		// placeholder, offset = curr file size
		newWAL.offset = info.Size()

		// put cursor at the end to facilitate appends
		_, err = f.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
	}

	// Fill in our new WAL struct
	newWAL.f = f
	newWAL.path = path
	newWAL.bw = bw
	newWAL.hdrLen = len(walHeader)

	return newWAL, nil
}

func (w *WAL) Close() error {

	err := w.bw.Flush()
	if err != nil {
		return err
	}

	err = w.f.Sync()
	if err != nil {
		return err
	}

	err = w.f.Close()
	if err != nil {
		return err
	}

	return nil
}

func Encode(rec *Record) ([]byte, error) {
	// This function takes a record and encodes it into a
	// []byte in a format that our WAL can understand.

	var enc []byte

	// First, we validate the record entries.

	if rec.Cmd.Instruct != CmdPut && rec.Cmd.Instruct != CmdDelete {
		return nil, errors.New("invalid Command, neither Put nor Delete")
	}

	// clientID must fit in u8.
	if len(rec.Cmd.ClientID) > math.MaxUint8 {
		return nil, errors.New("invalid ClientID length, exceeds 8 bits")
	}

	// Key must fit in u16
	if len(rec.Cmd.Key) > math.MaxUint16 {
		return nil, errors.New("invalid key, length exceeds 16 bits")
	}
	// value must fit in u32
	if uint64(len(rec.Cmd.Value)) > math.MaxUint32 {
		return nil, errors.New("invalid value, length exceeds 32 bits")
	}

	// encode the LogIndex to bytes and append it to our enc []byte
	enc = binary.BigEndian.AppendUint64(enc, rec.LogIndex)

	// we do not need to encode since it's only 1 byte
	// endianness doesn't matter for 1 byte
	// We append the instruction
	enc = append(enc, uint8(rec.Cmd.Instruct))

	// we append the length of clientID as 1 byte
	enc = append(enc, uint8(len(rec.Cmd.ClientID)))

	// we append the actual client ID
	enc = append(enc, rec.Cmd.ClientID...)

	// we append SEQ (since it's an int of more than one byte, endianness matters)
	enc = binary.BigEndian.AppendUint64(enc, rec.Cmd.Seq)

	// we append the length of the key
	enc = binary.BigEndian.AppendUint16(enc, uint16(len(rec.Cmd.Key)))

	// we append the key itself
	enc = append(enc, rec.Cmd.Key...)

	// we append the length of the value
	enc = binary.BigEndian.AppendUint32(enc, uint32(len(rec.Cmd.Value)))

	// we append the value itself
	enc = append(enc, rec.Cmd.Value...)

	if enc == nil {
		return nil, errors.New("nil rec")
	}

	// All the main record entries are now added to our enc byte slice.
	// Time to add the relevant metadata

	// computes crc in order to have a way to verify data integrity
	crc := crc32.ChecksumIEEE(enc)

	// we create the frame which is an empty byte slice of len(enc)
	// plus 4 bytes for the crc plus the length (4 bytes)
	frame := make([]byte, 0, 8+len(enc))

	// we calculate the length of the frame which is the len(enc)
	// plus 4 bytes for the crc
	frameLen := uint32(4 + len(enc))

	// we append the length of the crc as a value to our frame
	frame = binary.BigEndian.AppendUint32(frame, frameLen)

	// we append the crc to our frame
	frame = binary.BigEndian.AppendUint32(frame, crc)

	// now we add the enc byte slice to the frame
	frame = append(frame, enc...)

	// Now the record encoded into our WAL as bytes is in the following format:
	// frame = [u32 framelen][u32 crc32][enc]
	// [enc] = [u64 logIndex][u8 cmdType][u8 clientIDlen][clientID bytes][u64 seq]
	// cont. [u16 keyLen][key bytes][u32 valLen][value bytes]

	return frame, nil

}
