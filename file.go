package main

import (
	"log"
	"os"

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
	attr.Mode = os.ModeDir | 0755
	attr.Nlink = 0

	return nil
}

var _ fs.NodeOpener = (*File)(nil)

func (n File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	resp.Flags |= fuse.OpenNonSeekable
	return &FileHandle{
		sk_file: n.sk_file,
	}, nil
}

var _ fs.Handle = (*FileHandle)(nil)

type FileHandle struct {
	sk_file *gdrive.File
}

var _ fs.HandleReleaser = (*FileHandle)(nil)

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return nil
}

var _ fs.HandleReader = (*FileHandle)(nil)

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	logger.Debugf("FileHandle Read()")
	buf := make([]byte, req.Size)
	reader, err := gd.GetFileContents(fh.sk_file)
	if err != nil {
		log.Panicf("FileHandle Read GetFileContents failed: %v", err)
	}
	n, err := reader.Read(buf)
	if err != nil {
		log.Panicf("Filehandle Read reader.Read failed: %v", err)
	}
	resp.Data = buf[:n]
	return err
}

var _ fs.NodeAccesser = (*File)(nil)

func (f *File) Access(ctx context.Context, req *fuse.AccessRequest) error {
	logger.Debugf(f.sk_file.Path + " Access()")
	return nil
}
