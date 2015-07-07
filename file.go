package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/pellaeon/goas/v3/logger"
	"github.com/pellaeon/skicka/gdrive"
	"golang.org/x/net/context"
)

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
	}, nil
}

var _ fs.Handle = (*FileHandle)(nil)

type FileHandle struct {
	sk_file       *gdrive.File
	url           string // file download URL
	urlLastUpdate time.Time
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
	logger.Debugf("FileHandle Read()")
	go SingleUpdateMetadataCache()
	buf := make([]byte, req.Size)
	reader, err := fh.GetContent()
	if err != nil {
		log.Panicf("FileHandle Read GetFileContents failed: %v", err)
	}
	n, err := reader.Read(buf)
	reader.Close()
	if err != nil && n == 0 {
		log.Panicf("Filehandle Read failed: %v", err)
		return err
	} else {
		resp.Data = buf
		return nil
	}
}

var _ fs.NodeAccesser = (*File)(nil)

func (f *File) Access(ctx context.Context, req *fuse.AccessRequest) error {
	logger.Debugf(f.sk_file.Path + " Access()")
	return nil
}
