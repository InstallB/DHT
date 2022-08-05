package main

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"time"
)

const UploadWaitTime = time.Second * 10

type UploadPackage struct {
	index int
	hash  [20]byte
}

type DownloadPackage struct {
	index int
	data  []byte
}

func UploadToNetwork(n DhtNode, index int, data []byte, chindex chan int, chpackage chan UploadPackage) error {
	pi := PieceInfo{index, data}
	keydata, err := pi.PieceInfoHash()
	if err != nil {
		logrus.Error("PieceInfoHash Failed in UploadToNetwork, Error: ", err)
		chindex <- index
		return err
	}
	key := string(keydata[:])
	value := string(data)
	ok := n.Put(key, value)
	if !ok {
		logrus.Error("Failed to Put File in UploadToNetWork")
		chindex <- index
		return errors.New("failed to put file")
	}
	chpackage <- UploadPackage{index, keydata}
	return nil
}

func Upload(n DhtNode, FileName string) error {
	c, err := ioutil.ReadFile(FileName)
	if err != nil {
		logrus.Error("Failed to Open File in Upload, Error: ", err)
		fmt.Println("Failed to Open File, Error: ", err)
		return err
	}
	FileSize := len(c)
	PieceNum := (FileSize-1)/PieceSize + 1
	chindex := make(chan int, PieceNum+5)
	chpackage := make(chan UploadPackage, PieceNum+5)
	chcontent := make(chan string, 2)

	for i := 0; i < PieceNum; i++ {
		chindex <- i
	}
	flag1 := true
	for flag1 {
		select {
		case i := <-chindex:
			l := i * PieceSize
			r := l + PieceSize
			if r > FileSize {
				r = FileSize
			}
			go func() {
				err := UploadToNetwork(n, i, c[l:r], chindex, chpackage)
				if err != nil {
					logrus.Error(i, " UploadToNetwork Failed, Error: ", err)
				}
			}()
		case <-time.After(UploadWaitTime):
			flag1 = false
			// Upload Wait Time Enough for 'Put' to Complete
		}
	}
	go func() {
		err := MakeTorrentFile(FileName, chcontent)
		if err != nil {
			logrus.Error("MakeTorrentFile Failed in Upload")
		}
	}()
	piece := make([]byte, PieceNum*20, PieceNum*20)
	flag1 = true
	for flag1 {
		select {
		case p := <-chpackage:
			i := p.index
			l := i * 20
			r := l + 20
			copy(piece[l:r], p.hash[:])
		default:
			flag1 = false
		}
	}
	chcontent <- string(piece)
	fmt.Println("Upload Complete")
	return nil
}

func DownloadFromNetwork(n DhtNode, index int, hash [20]byte, chindex chan int, chpackage chan DownloadPackage) error {
	key := string(hash[:])
	ok, value := n.Get(key)
	if !ok {
		logrus.Error("Failed to Get in DownloadFromNetwork")
		chindex <- index
		return errors.New("failed to get value")
	}
	data := []byte(value)
	pi := PieceInfo{index, data}
	verifyHash, err := pi.PieceInfoHash()
	if err != nil {
		logrus.Error("PieceInfoHash Failed in DownloadFromNetwork")
		chindex <- index
		return err
	}
	if verifyHash != hash {
		logrus.Error("Hash Value doesn't Match in DownloadFromNetwork")
		chindex <- index
		return errors.New("hash value doesnt match")
	}
	chpackage <- DownloadPackage{index, data}
	return nil
}

func Download(n DhtNode, FileName string) error {
	file, err := os.Open(FileName)
	if err != nil {
		logrus.Error("Failed to Open ", FileName, " in Download, Error: ", err)
		fmt.Println("Failed to Open ", FileName, " in Download, Error: ", err)
		return err
	}
	bt, err := Open(file)
	if err != nil {
		logrus.Error("Failed to Parse Torrent File in Download, Error: ", err)
		fmt.Println("Failed to Parse Torrent File in Download, Error: ", err)
		return err
	}
	torrent, err := bt.ToTorrentFile() // TorrentFile
	if err != nil {
		logrus.Error("ToTorrentFile Failed in Download, Error: ", err)
		fmt.Println("ToTorrentFile Failed in Download, Error: ", err)
		return err
	}

	PieceNum := (torrent.Length-1)/torrent.PieceLength + 1
	chindex := make(chan int, PieceNum+5)
	chpackage := make(chan DownloadPackage, PieceNum+5)

	for i := 0; i < PieceNum; i++ {
		chindex <- i
	}
	flag1 := true
	for flag1 {
		select {
		case i := <-chindex:
			go func() {
				err := DownloadFromNetwork(n, i, torrent.PieceHashes[i], chindex, chpackage)
				if err != nil {
					logrus.Error(i, " DownLoadFromNetWork Failed, Error: ", err)
				}
			}()
		case <-time.After(UploadWaitTime):
			flag1 = false
			// Upload Wait Time Enough for 'Put' to Complete
		}
	}
	c := make([]byte, PieceNum*PieceSize, PieceNum*PieceSize)
	flag1 = true
	for flag1 {
		select {
		case p := <-chpackage:
			i := p.index
			l := i * PieceSize
			r := l + PieceSize
			copy(c[l:r], p.data[:])
		default:
			flag1 = false
		}
	}
	err = ioutil.WriteFile(torrent.Name, c, 0664)
	if err != nil {
		logrus.Error("WriteFile Failed in Download, Error: ", err)
		fmt.Println("WriteFile Failed in Download, Error: ", err)
		return err
	}
	fmt.Println("Download Complete")
	return nil
}

// upload DHT_intro.pdf
// download DHT_intro.pdf.torrent
