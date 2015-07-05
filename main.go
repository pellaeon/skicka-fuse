package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/net/context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	skicka "github.com/pellaeon/skicka"
	"github.com/pellaeon/skicka/gdrive"

	"github.com/tideland/goas/v3/logger"
)

///////////////////////////////////////////////////////////////////////////
// Global Variables

var progName = filepath.Base(os.Args[0])

type debugging bool

var (
	gd *gdrive.GDrive

	// The key is only set if encryption is needed (i.e. if -encrypt is
	// provided for an upload, or if an encrypted file is encountered
	// during 'download' or 'cat').
	key []byte

	debug   debugging
	verbose debugging
	quiet   bool

	// Configuration read in from the skicka config file.
	config struct {
		Google struct {
			ClientId     string
			ClientSecret string
			// If set, is appended to all http requests via ?key=XXX.
			ApiKey string
		}
		Encryption struct {
			Salt             string
			Passphrase_hash  string
			Encrypted_key    string
			Encrypted_key_iv string
		}
		Upload struct {
			Ignored_Regexp         []string
			Bytes_per_second_limit int
		}
		Download struct {
			Bytes_per_second_limit int
		}
	}

	// Various statistics gathered along the way. These all should be
	// updated using atomic operations since we often have multiple threads
	// working concurrently for uploads and downloads.
	stats struct {
		DiskReadBytes     int64
		DiskWriteBytes    int64
		UploadBytes       int64
		DownloadBytes     int64
		LocalFilesUpdated int64
		DriveFilesUpdated int64
	}

	// Smaller files will be handled with multiple threads going at once;
	// doing so improves bandwidth utilization since round-trips to the
	// Drive APIs take a while.  (However, we don't want too have too many
	// workers; this would both lead to lots of 403 rate limit errors...)
	nWorkers int
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", progName)
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", progName)
	flag.PrintDefaults()
}

func userHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}

func main() {
	l := logger.NewGoLogger()
	logger.SetLogger(l)
	logger.SetLevel(logger.LevelDebug)
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	// Initialize skicka
	home := userHomeDir()
	tokenCacheFilename := flag.String("tokencache",
		filepath.Join(home, ".skicka.tokencache.json"),
		"OAuth2 token cache file")
	configFilename := flag.String("config",
		filepath.Join(home, ".skicka.config"),
		"Configuration file")
	metadataCacheFilename := flag.String("metadata-cache-file",
		filepath.Join(home, "/.skicka.metadata.cache"),
		"Filename for local cache of Google Drive file metadata")
	nw := flag.Int("num-threads", 4, "Number of threads to use for uploads/downloads")
	vb := flag.Bool("verbose", false, "Enable verbose output")
	dbg := flag.Bool("debug", false, "Enable debugging output")
	qt := flag.Bool("quiet", false, "Suppress non-error messages")
	dumpHTTP := flag.Bool("dump-http", false, "Dump http traffic")
	flakyHTTP := flag.Bool("flaky-http", false, "Add flakiness to http traffic")
	noBrowserAuth := flag.Bool("no-browser-auth", false,
		"Don't try launching browser for authorization")
	flag.Usage = usage
	flag.Parse()

	var mountpoint string
	if flag.NArg() == 1 {
		mountpoint = flag.Arg(0)
	} else {
		mountpoint = *flag.String("mountpoint", "", "Mountpoint")
	}
	if mountpoint == "" {
		usage()
		os.Exit(2)
	}

	nWorkers = *nw

	debug = debugging(*dbg)
	verbose = debugging(*vb || bool(debug))
	quiet = *qt

	skicka.ReadConfigFile(*configFilename)

	// Set up the basic http.Transport.
	transport := http.DefaultTransport
	if tr, ok := transport.(*http.Transport); ok {
		// Increase the default number of open connections per destination host
		// to be enough for the number of goroutines we run concurrently for
		// uploads/downloads; this gives some benefit especially for uploading
		// small files.
		tr.MaxIdleConnsPerHost = 4
	} else {
		skicka.PrintErrorAndExit(fmt.Errorf("DefaultTransport not an *http.Transport?"))
	}
	if *flakyHTTP {
		transport = skicka.NewFlakyTransport(transport)
	}
	if *dumpHTTP {
		//transport = skicka.LoggingTransport{transport: transport}
		// TODO
	}

	// And now upgrade to the OAuth Transport *http.Client.
	client, err := skicka.GetOAuthClient(*tokenCacheFilename, !*noBrowserAuth,
		transport)
	if err != nil {
		skicka.PrintErrorAndExit(fmt.Errorf("error with OAuth2 Authorization: %v ", err))
	}

	// Choose the appropriate callback function for the GDrive object to
	// use for debugging output.
	var dpf func(s string, args ...interface{})
	if debug {
		dpf = skicka.DebugPrint
	} else {
		dpf = skicka.DebugNoPrint
	}

	gd, err := gdrive.New(config.Upload.Bytes_per_second_limit,
		config.Download.Bytes_per_second_limit, dpf, client,
		*metadataCacheFilename, quiet)
	if err != nil {
		log.Fatal(fmt.Errorf("error creating Google Drive "+
			"client: %v", err))
		os.Exit(3)
	}

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatalf("Mount failed: %v", err)
	}
	defer c.Close()

	filesys := &FS{
		gd: gd,
	}
	if err := fs.Serve(c, filesys); err != nil {
		log.Fatalf("Serve failed: %v", err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatalf("mount process error: %v", err)
	}
}

type FS struct {
	gd *gdrive.GDrive
}

var _ fs.FS = (*FS)(nil)

func (f *FS) Root() (fs.Node, error) {
	gd_file, err := f.gd.GetFile("/")
	return Dir{
		sk_file: gd_file,
		fs:      f,
	}, err
}

var _ fs.Node = (*Dir)(nil)

type Dir struct {
	sk_file *gdrive.File
	fs      *FS
}

func (n Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	var sum uint64
	for _, c := range n.sk_file.Id {
		sum += uint64(c)
	}
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

var _ fs.HandleReadDirAller = (*Dir)(nil)

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	files_in_folder, err := d.fs.gd.GetFilesInFolder(d.sk_file.Path)
	if err != nil {
		log.Fatalf("ReadDirAll failed: %v", err)
	}
	var res []fuse.Dirent
	for _, file := range files_in_folder {
		var de fuse.Dirent
		de.Name = file.Path[strings.LastIndex(file.Path, "/")+1:]
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

var _ fs.Node = (*File)(nil)

type File struct {
	sk_file *gdrive.File
	fs      *FS
}

func (n File) Attr(ctx context.Context, attr *fuse.Attr) error {
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

var _ fs.NodeRequestLookuper = (*Dir)(nil)

func (n Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	path := req.Name
	logger.Debugf("req.Name= " + req.Name)
	gd_file, err := n.fs.gd.GetFile(path)
	if err != nil {
		log.Fatalf("Lookup GetFile failed: %v", err)
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
	buf := make([]byte, req.Size)
	reader, err := gd.GetFileContents(fh.sk_file)
	if err != nil {
		log.Fatalf("FileHandle Read GetFileContents failed: %v", err)
	}
	n, err := reader.Read(buf)
	if err != nil {
		log.Fatalf("Filehandle Read reader.Read failed: %v", err)
	}
	resp.Data = buf[:n]
	return err
}
