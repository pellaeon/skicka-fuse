package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	skicka "github.com/pellaeon/skicka"
	"github.com/pellaeon/skicka/gdrive"

	"github.com/pellaeon/goas/v3/logger"
)

///////////////////////////////////////////////////////////////////////////
// Global Variables

const BLKSIZE = 4096

var progName = filepath.Base(os.Args[0])
var metadataCacheFilename *string

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
	l := logger.NewStandardTimeLogger(os.Stdout, time.StampMicro)
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
	metadataCacheFilename = flag.String("metadata-cache-file",
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

	gd, err = gdrive.New(config.Upload.Bytes_per_second_limit,
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
	logger.Debugf("Root(): %s", gd_file.Path)
	if err != nil {
		logger.Errorf("FS Root() : %v", err)
	}
	return Dir{
		sk_file: gd_file,
		fs:      f,
	}, err
}

var _ fs.FSStatfser = (*FS)(nil)

func (fs *FS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	usage, err := fs.gd.GetDriveUsage()
	if err != nil {
		logger.Errorf("Statfs() error: %v", err)
	}
	resp.Blocks = uint64(usage.Capacity / BLKSIZE)
	resp.Bfree = uint64((usage.Capacity - usage.Used) / BLKSIZE)
	resp.Bavail = uint64((usage.Capacity - usage.Used) / BLKSIZE)
	resp.Files = 9999 //XXX
	resp.Ffree = 9999 //XXX
	resp.Bsize = BLKSIZE
	resp.Namelen = 32767 //http://www.aurelp.com/2014/09/10/what-is-the-maximum-name-length-for-a-file-on-google-drive/
	resp.Frsize = BLKSIZE
	req.Respond(resp)
	return err
}

// Update metadata cache, if another call comes within 20 seconds it will be discarded
func SingleUpdateMetadataCache() {
	if updatingCache == 1 {
		//logger.Debugf("Skip metadata cache update")
		return
	} else {
		atomic.AddUint32(&updatingCache, 1)
		logger.Infof("Updating metadata cache in background")
		err := gd.UpdateMetadataCache(*metadataCacheFilename)
		if err != nil {
			logger.Errorf("Update metadata cache failed: %v", err)
		}
		time.Sleep(20 * time.Second)
		atomic.AddUint32(&updatingCache, ^uint32(0))
	}
}
