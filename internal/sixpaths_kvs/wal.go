package sixpaths_kvs

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"time"
)

// wal.go implements a write-ahead log for data durability
// Our wal encodes all the applications of commands to our store
// so that we can recreate this node's store by simply
// applying the commands on the wal in order.

// The wal is written in bytes so all of our records are as well
// translating our record and command structs into bytes is a big part
// of this file's logic.

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

var errCorrupt = errors.New("wal: corrupt")

func isCorrupt(err error) bool {
	return errors.Is(err, errCorrupt)
}

func NewWAL(path string) (*WAL, error) {
	//This function creates a new WAL using the path provided

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

func (wal *WAL) Append(rec *Record) error {
	// checks record integrity
	// calls encode to get the frame
	// Flushes, Syncs, and updates the offset

	fr, err := Encode(rec)
	if err != nil {
		return err
	}

	// We attempt to append the frame at the offset
	// to our WAL file. If successful, we will get the incr
	_, err = wal.bw.Write(fr)
	if err != nil {
		return err
	}

	err = wal.bw.Flush()
	if err != nil {
		return err
	}

	start := time.Now()
	err = wal.f.Sync()
	if err != nil {
		return err
	}
	log.Printf("wal_append bytes=%d fsync_ms=%d", len(fr), time.Since(start).Milliseconds())

	// if successful write, we update our offset
	wal.offset += int64(len(fr))

	return nil
}

func Decode(payload []byte) (Record, error) {

	newrec := Record{}
	newcom := Command{}

	// we make a copy to not modify the original
	paycopy := append([]byte(nil), payload...)

	off := 0
	// the need() func allows us to continuously verify if the payload is large enough
	need := func(n int) error {
		if len(paycopy)-off < n {
			return fmt.Errorf("payload is too short, %d more bytes are needed. (off=%d, len=%d)", n, off, len(payload))
		} else {
			return nil
		}
	}

	// first extract the logIndex
	err := need(8)
	if err != nil {
		return Record{}, err
	}
	newrec.LogIndex = binary.BigEndian.Uint64(paycopy[off : off+8])
	off += 8

	// then we extract the cmdType
	err = need(1)
	if err != nil {
		return Record{}, err
	}

	if !validType(CommandType(paycopy[off])) {
		return Record{}, fmt.Errorf("wrong Commandtype, expected 1 or 2 but got: %d", CommandType(paycopy[off]))
	}
	newcom.Instruct = CommandType(paycopy[off])
	off += 1

	// extract clientIDlen
	err = need(1)
	if err != nil {
		return Record{}, err
	}
	clientidlen := int(paycopy[off])
	off += 1

	// extract clientID
	err = need(clientidlen)
	if err != nil {
		return Record{}, err
	}
	newcom.ClientID = string(paycopy[off : off+clientidlen])
	off += clientidlen

	// extract seq
	err = need(8)
	if err != nil {
		return Record{}, err
	}
	newcom.Seq = binary.BigEndian.Uint64(paycopy[off : off+8])
	off += 8

	//extract keylen
	err = need(2)
	if err != nil {
		return Record{}, err
	}
	keylen := binary.BigEndian.Uint16(paycopy[off : off+2])
	off += 2

	// extract key itself using keylen
	err = need(int(keylen))
	if err != nil {
		return Record{}, err
	}
	k := make([]byte, int(keylen))
	copy(k, paycopy[off:off+int(keylen)])
	newcom.Key = k
	off += int(keylen)

	// extract value len
	err = need(4)
	if err != nil {
		return Record{}, err
	}
	valuelen := int(binary.BigEndian.Uint32(paycopy[off : off+4]))
	off += 4

	// extract value using valuelen
	err = need(valuelen)
	if err != nil {
		return Record{}, err
	}
	v := make([]byte, valuelen)
	copy(v, paycopy[off:off+valuelen])
	newcom.Value = v
	off += valuelen

	// now every relevant field is filled out so we return our decoded record
	newrec.Cmd = newcom
	return newrec, nil
}

func (w *WAL) readFrameAt(offset int64) ([]byte, int, error) {

	// This func tries to read the frame at the given offset
	// It returns the frame, the amount of bytes consumed (0 if failure)
	// and a potential error.

	var hdr [4]byte

	// we try reading from the WAL bytefile at the given offset
	n, err := w.f.ReadAt(hdr[:], offset)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			//not enough bytes to read off the tail
			return nil, 0, io.EOF
		}
		return nil, 0, err
	}

	if n < 4 {
		// This means we do not have enough bytes to determine length
		// Which means this data must be corrupted
		return nil, 0, io.ErrUnexpectedEOF
	}

	frameLen := binary.BigEndian.Uint32(hdr[:])
	// we make some frameLen checks to see if the data seems legit
	if frameLen < 4 {
		return nil, 0, fmt.Errorf("%w: bad framelen = %d", errCorrupt, frameLen)
	}
	if uint32(math.Pow(2, 30)) < frameLen {
		return nil, 0, fmt.Errorf("%w: frameLen overflows", errCorrupt)
	}

	// we create a byteslice to store the frame body
	frameBody := make([]byte, frameLen)
	// we read our byte file at the offset plus 4 bytes to get the frame itself
	n, err = w.f.ReadAt(frameBody, offset+4)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, 0, io.ErrUnexpectedEOF
		}
		return nil, 0, err
	}

	if uint32(n) < frameLen {
		return nil, 0, io.ErrUnexpectedEOF
	}

	// Now we get the crc part from the frame
	body := frameBody
	// crcCheck contains the crc we parsed
	crcCheck := binary.BigEndian.Uint32(body[:4])
	// enc contains the rest of the payload
	enc := body[4:]

	// Then we verify the correctness of the crc by comparing it to
	// the crc32 we get from the frame we parsed
	got := crc32.ChecksumIEEE(enc)
	// if the crc we get and the one we parsed are not equal,
	// we know our data has been corrupted.
	if got != crcCheck {
		return nil, 0, errCorrupt
	}

	//otherwise, everything looks good and we return our payload
	return enc, int(4 + frameLen), nil

}

func (w *WAL) ReplayAll() (recs []Record, lastIndex uint64, err error) {

	// we begin right after our header
	off := int64(w.hdrLen)
	// the last good offset is presumably what goes right after our header, so in case
	// of early failure we can always return there.
	lastGood := off

	// out will contain the record slice we're going to build up and return
	var out []Record

	var repairNeeded bool = true

	// this loop reads frames and decodes them until it reaches an error
	for {
		// we (attempt) to read the next frame
		enc, n, rerr := w.readFrameAt(off)
		// If rerr is not nil then our loop must end
		if rerr != nil {
			// if rerr is just io.EOF then we do not need to repair anything
			// if rerr is anything else then we keep repairNeeded true
			if rerr == io.EOF {
				repairNeeded = false
			}
			if rerr != io.EOF && rerr != io.ErrUnexpectedEOF && !isCorrupt(rerr) {
				return out, lastIndex, rerr
			}
			break
		}

		// We attempt to decode the frame we got from readFrameAt
		currRec, derr := Decode(enc)
		// if the decoding fails, we break
		if derr != nil {
			repairNeeded = true
			break
		}

		// If we've made it to this point then we successfully decoded the frame,
		//so we add the record to our record slice "out"
		out = append(out, currRec)
		// we update the lastidx
		lastIndex = currRec.LogIndex
		// since our decode was successful, we
		off = off + int64(n)
		lastGood = off
	}

	if repairNeeded {
		// we truncate to the lastGood and we reset the writer state.
		err = w.f.Truncate(lastGood)
		if err != nil {
			return out, lastIndex, err
		}
		err = w.f.Sync()
		if err != nil {
			return out, lastIndex, err
		}

		_, err = w.f.Seek(lastGood, io.SeekStart)
		if err != nil {
			return out, lastIndex, err
		}

		// our WAL's offset is set to the last good offset
		w.offset = lastGood
		w.bw.Reset(w.f)
		return out, lastIndex, nil
	}

	//otherwise repair is not needed and we just clean up and return out
	_, err = w.f.Seek(lastGood, io.SeekStart)
	if err != nil {
		return out, lastIndex, err
	}

	w.offset = lastGood
	w.bw.Reset(w.f)
	return out, lastIndex, nil

}
