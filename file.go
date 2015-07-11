package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/pellaeon/goas/v3/logger"
	"github.com/pellaeon/skicka/gdrive"
	"golang.org/x/net/context"
)

const CACHE_THRESHOLD_SIZE = 4096

var _ fs.Node = (*File)(nil)

type File struct {
	sk_file *gdrive.File
	fs      *FS
}

func (n File) Attr(ctx context.Context, attr *fuse.Attr) error {
	logger.Debugf("%s.Attr()", n.sk_file.Path)
	go SingleUpdateMetadataCache()
	sum := uint64(0)
	for _, c := range n.sk_file.Id {
		sum += uint64(c)
	}
	attr.Inode = sum
	attr.Size = uint64(n.sk_file.FileSize)
	attr.Blocks = uint64(n.sk_file.FileSize / 1024) // XXX: block size 1024 bytes
	attr.Atime = n.sk_file.ModTime
	attr.Mtime = n.sk_file.ModTime
	attr.Ctime = n.sk_file.ModTime
	attr.Crtime = n.sk_file.ModTime
	attr.Mode = 0600
	attr.Nlink = 0

	return nil
}

var _ fs.NodeOpener = (*File)(nil)

func (n File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	resp.Flags |= fuse.OpenNonSeekable
	return &FileHandle{
		sk_file:       n.sk_file,
		url:           "",
		urlLastUpdate: time.Unix(0, 0),
		cache:         NewFileCache(n.sk_file.Id),
	}, nil
}

var _ fs.Handle = (*FileHandle)(nil)

type FileHandle struct {
	sk_file       *gdrive.File
	url           string // file download URL
	urlLastUpdate time.Time
	cache         *FileCache
}

var _ fs.HandleReleaser = (*FileHandle)(nil)

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return nil
}

func (fh *FileHandle) GetContent() (io.ReadCloser, error) {
	if time.Since(fh.urlLastUpdate) > 3*time.Hour {
		logger.Debugf("fh.GetContent url expired: %s", fh.sk_file.Path)
		driveFile, err := gd.GetFileById(fh.sk_file.Id)
		if err != nil {
			return nil, err
		}

		url := driveFile.DownloadUrl
		if url == "" {
			// Google Docs files can't be downloaded directly via DownloadUrl,
			// but can be exported to another format that can be downloaded.

			// Docs, Sheets, and Slides can be downloaded into .docx, .xls,
			// and .pptx formats, respectively. This may be a bit confusing since
			// they won't have that suffix locally.
			for mt, u := range driveFile.ExportLinks {
				if strings.HasPrefix(mt, "application/vnd.openxmlformats-officedocument") {
					url = u
					break
				}
			}
			// Google Drawings can be downloaded in SVG form.
			if url == "" {
				if u, ok := driveFile.ExportLinks["image/svg+xml"]; ok {
					url = u
				}
			}
			// Otherwise we seem to be out of luck.
			if url == "" {
				return nil, fmt.Errorf("%s: unable to download Google Docs file", fh.sk_file.Path)
			}
		}
		fh.url = url
		fh.urlLastUpdate = time.Now()
	}
	for try := 0; ; try++ {
		request, err := http.NewRequest("GET", fh.url, nil)
		if err != nil {
			return nil, err
		}

		resp, err := gd.Client.Do(request)

		switch gd.HandleHTTPResponse(resp, err, try) {
		case gdrive.Success:
			// Rate-limit the download, if required.
			return gdrive.MakeLimitedDownloadReader(resp.Body), nil
		case gdrive.Fail:
			return nil, err
		case gdrive.Retry:
		}
	}
}

var _ fs.HandleReader = (*FileHandle)(nil)

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	go SingleUpdateMetadataCache()
	if req.Offset == 0 {
		doneFirstReq := make(chan bool)
		fh.doneReadToCache = make(chan bool)
		go fh.LaunchBackgroundGetContent(req.Size, doneFirstReq, fh.doneReadToCache)
		<-doneFirstReq
		logger.Debugf("========got first block")
		resp.Data = fh.fileCache[:req.Size]
		req.Respond(resp)
		return nil
	} else {
		<-fh.doneReadToCache
		logger.Debugf("====read from %d to %d", req.Offset, len(fh.fileCache))
		resp.Data = fh.fileCache[req.Offset:len(fh.fileCache)]
		req.Respond(resp)
		return nil
	}
}
func (fh *FileHandle) LaunchBackgroundGetContent(firstReqSize int, doneFirstReq, done chan<- bool) error {
	//var cacheSize int64
	//if CACHE_THRESHOLD_SIZE <= fh.sk_file.FileSize {
	//	cacheSize = CACHE_THRESHOLD_SIZE
	//} else {
	//	cacheSize = fh.sk_file.FileSize
	//}
	fh.fileCache = make([]byte, fh.sk_file.FileSize)
	reader, err := fh.GetContent()
	if err != nil {
		logger.Errorf("FileHandle LaunchBackgroundGetContent GetContent failed: %v", err)
		return err
	}
	n, err := io.ReadFull(reader, fh.fileCache[:firstReqSize])
	if err != nil && n != firstReqSize {
		logger.Errorf("Filehandle LaunchBackgroundGetContent read first block failed: %v", err)
		return err
	} else {
		doneFirstReq <- true
		close(doneFirstReq)
	}
	logger.Debugf("==========start reading %d", len(fh.fileCache[firstReqSize:]))
	n, err = io.ReadFull(reader, fh.fileCache[firstReqSize:])
	logger.Debugf("==========end reading %d", n)
	if err != nil && n != int(fh.sk_file.FileSize)-firstReqSize { //TODO possible int overflow
		logger.Errorf("Filehandle LaunchBackgroundGetContent read remaining failed: %v", err)
		return err
	} else if err != nil {
		logger.Debugf("======%+v", err)
	} else {
		done <- true
		close(done)
	}
	logger.Debugf("==============background getcontent terminating")
	return nil
}

type FileCache struct {
	fileId           string
	inMemorySegments []int64
	onDiskSegments   []int64
	memSegments      map[int64][]byte
}

func NewFileCache(id string) *FileCache {
	return &FileCache{
		fileId:           id,
		inMemorySegments: make([]int64, 100), //XXX
		onDiskSegments:   make([]int64, 100), //TODO: check segment files on disk and populate information
		memSegments:      make(map[int64][]byte),
	}
}

func (fc *FileCache) Read(start int64, p []byte) (int, error) {
	end, n, segmentno := int64(0)
	// search memory segments
	for i := 0; i <= len(fc.inMemorySegments)-1; i += 2 {
		if fc.inMemorySegments[i] <= start {
			if fc.inMemorySegments[i+1] > start+len(p) {
				logger.Warningf("MemorySegment smaller than requested")
				end = fc.inMemorySegments[i+1]
				n = fc.inMemorySegments[i+1] - fc.inMemorySegments[i]
			} else {
				end = start + len(p)
				n = len(p)
			}
			segmentno = fc.inMemorySegments[i]
			break
		}
	}
	if end != 0 {
		p = memSegments[segmentno][start-segmentno : end-segmentno]
		return n, nil
	} else {
		// TODO: search disk segments
		logger.Debugf("Reading not in memory segent")
		for i := 0; i <= len(fc.onDiskSegments)-1; i += 2 {
			if fc.onDiskSegments[i] <= start {
				if fc.onDiskSegments[i+1] > start+len(p) {
					logger.Warningf("MemorySegment smaller than requested")
					end = fc.onDiskSegments[i+1]
					n = fc.onDiskSegments[i+1] - fc.onDiskSegments[i]
				} else {
					end = start + len(p)
					n = len(p)
				}
				segmentno = fc.onDiskSegments[i]
				break
			}
		}
		// open on disk cache file
		cacheFileName := CACHEDIR + fc.fileId + "_" + string(segmentno)
		cacheFile, err := os.Open(cacheFileName)
		if err != nil {
			logger.Errorf("Opening cache file %s failed: %+v", cacheFileName, err)
			//TODO: handle error
		}
		n, err = cacheFile.ReadAt(p, start-segmentno) //XXX: len(p) != [start-segmentno : end-segmentno]
		if err != nil && n == 0 {
			logger.Errorf("Reading cache file %s failed: %+v", cacheFileName, err)
		}
		return n, nil
	}
	// no data found in mem nor disk, start download
	//TODO
}

func (fc *FileCache) Write(start int64, p []byte) (int, error) {
	//TODO
	append(fc.inMemorySegments, start)
	append(fc.inMemorySegments, start+len(p))
	memSegments[start] = p
}

var _ fs.NodeAccesser = (*File)(nil)

func (f *File) Access(ctx context.Context, req *fuse.AccessRequest) error {
	logger.Debugf(f.sk_file.Path + " Access()")
	return nil
}
