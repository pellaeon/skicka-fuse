package main

import (
	"log"
	"os"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/pellaeon/goas/v3/logger"
	"github.com/pellaeon/skicka/gdrive"
	"golang.org/x/net/context"
)

var _ fs.Node = (*Dir)(nil)

type Dir struct {
	sk_file *gdrive.File
	fs      *FS
}

func (n Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	logger.Debugf("%s .Attr()", n.sk_file.Path)
	var sum uint64
	for _, c := range n.sk_file.Id {
		sum += uint64(c)
	}
	logger.Debugf("%s .Attr() Id= %s", n.sk_file.Path, n.sk_file.Id)
	attr.Inode = sum
	attr.Size = 4096 // XXX
	attr.Blocks = 0
	attr.Atime = n.sk_file.ModTime
	attr.Mtime = n.sk_file.ModTime
	attr.Ctime = n.sk_file.ModTime
	attr.Crtime = n.sk_file.ModTime
	attr.Mode = os.ModeDir | 0755
	attr.Nlink = 0

	return nil
}

type DirHandle struct {
	sk_file *gdrive.File
}

var _ fs.HandleReadDirAller = (*DirHandle)(nil)

func (dh *DirHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	logger.Debugf("ReadDirAll %s", dh.sk_file.Path)
	files_in_folder, err := gd.GetFilesInFolder(dh.sk_file.Path)
	if err != nil {
		log.Fatalf("ReadDirAll failed: %v", err)
	}
	var res []fuse.Dirent
	for _, file := range files_in_folder {
		var de fuse.Dirent
		de.Name = file.Path[strings.LastIndex(file.Path, "/")+1:]
		logger.Debugf("ReadDirAll %s - %s", dh.sk_file.Path, de.Name)
		if file.IsFolder() {
			de.Type = fuse.DT_Dir
		} else {
			de.Type = fuse.DT_File
		}
		sum := uint64(0)
		for _, c := range file.Id {
			sum += uint64(c)
		}
		de.Inode = sum
		res = append(res, de)
	}
	return res, err
}

var _ fs.NodeRequestLookuper = (*Dir)(nil)

func (n Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	path := n.sk_file.Path + "/" + req.Name
	logger.Debugf("req.Name= " + req.Name)
	gd_file, err := n.fs.gd.GetFile(path)
	if err != nil {
		log.Panicf("Lookup GetFile failed: %v", err)
		return nil, fuse.ENOENT
	}
	if gd_file.IsFolder() {
		return Dir{
			sk_file: gd_file,
			fs:      n.fs,
		}, nil
	} else {
		return File{
			sk_file: gd_file,
			fs:      n.fs,
		}, nil
	}
}

var _ fs.NodeOpener = (*Dir)(nil)

func (n Dir) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	logger.Debugf("Dir.Open()")
	resp.Flags |= fuse.OpenNonSeekable
	return &DirHandle{
		sk_file: n.sk_file,
	}, nil
}

var _ fs.NodeAccesser = (*Dir)(nil)

func (d Dir) Access(ctx context.Context, req *fuse.AccessRequest) error {
	logger.Debugf(d.sk_file.Path + " Access()")
	return nil
}
