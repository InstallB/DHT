package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/jackpal/bencode-go"
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

const PieceSize = 256 * 1024

type BencodeInfo struct {
	Pieces      string `bencode:"pieces"`
	PieceLength int    `bencode:"piece length"`
	Length      int    `bencode:"length"`
	Name        string `bencode:"name"`
}

type BencodeTorrent struct {
	Announce string      `bencode:"announce"`
	Info     BencodeInfo `bencode:"info"`
}

type TorrentFile struct {
	Announce    string
	InfoHash    [20]byte
	PieceHashes [][20]byte
	PieceLength int
	Length      int
	Name        string
}

type PieceInfo struct {
	Index int
	Data  []byte
}

// Open parses a torrent file
func Open(r io.Reader) (*BencodeTorrent, error) {
	bto := BencodeTorrent{}
	err := bencode.Unmarshal(r, &bto)
	if err != nil {
		logrus.Error("Open File Failed, Error: ", err)
		return nil, err
	}
	return &bto, nil
}

func (n *BencodeInfo) BencodeInfoHash() ([20]byte, error) {
	var buf bytes.Buffer
	err := bencode.Marshal(&buf, *n)
	if err != nil {
		logrus.Error("BencodeInfoHash Failed, Error: ", err)
		return [20]byte{}, err
	}
	ret := sha1.Sum(buf.Bytes())
	return ret, nil
}

func (n *PieceInfo) PieceInfoHash() ([20]byte, error) {
	var buf bytes.Buffer
	err := bencode.Marshal(&buf, *n)
	if err != nil {
		logrus.Error("BencodeInfoHash Failed, Error: ", err)
		return [20]byte{}, err
	}
	ret := sha1.Sum(buf.Bytes())
	return ret, nil
}

func (n *BencodeInfo) Split() [][20]byte {
	buf := []byte(n.Pieces)
	num := len(buf) / 20
	ret := make([][20]byte, num, num)
	for i := 0; i < num; i++ {
		copy(ret[i][:], buf[i*20:(i+1)*20])
	}
	return ret
}

func (bto *BencodeTorrent) ToTorrentFile() (TorrentFile, error) {
	infohash, err := bto.Info.BencodeInfoHash()
	if err != nil {
		logrus.Error("ToTorrentfile Failed, Error: ", err)
		return TorrentFile{}, err
	}
	pieceshash := bto.Info.Split()
	t := TorrentFile{
		Announce:    bto.Announce,
		InfoHash:    infohash,
		PieceHashes: pieceshash,
		PieceLength: bto.Info.PieceLength,
		Length:      bto.Info.Length,
		Name:        bto.Info.Name,
	}
	return t, nil
}

func MakeTorrentFile(FileName string, chcontent chan string) error {
	FileState, err := os.Stat(FileName)
	if err != nil {
		logrus.Error("Get File Stat Failed, Error: ", err)
		fmt.Println("Get File Stat Failed, Error: ", err)
		return err
	}
	b := BencodeTorrent{
		Announce: "DHT",
		Info: BencodeInfo{
			Pieces:      "",
			PieceLength: PieceSize,
			Length:      (int)(FileState.Size()),
			Name:        FileState.Name(),
		},
	}
	b.Info.Pieces = <-chcontent
	FileName = FileState.Name() + ".torrent"
	var torrentfile *os.File
	torrentfile, err = os.Create(FileName)
	if err != nil {
		logrus.Info("Failed to Create .torrent File, Error: ", err)
		fmt.Println("Failed to Create .torrent File, Error: ", err)
		return err
	}
	err = bencode.Marshal(torrentfile, b)
	if err != nil {
		logrus.Info("Failed to Marshal Content, Error: ", err)
		fmt.Println("Failed to Marshal Content, Error: ", err)
		return err
	}
	fmt.Println("Successfully Created .torrent File ", FileName)
	return nil
}
