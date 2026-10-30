package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	kvfs "kvfs/internal/fs"
	"kvfs/internal/store"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/redis/rueidis"
)

type config struct {
	addrs      []string
	ns         string
	level      store.Level
	opTimeout  time.Duration
	allowOther bool
	debug      bool
	repair     string
	mountpoint string
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		log.Fatalf("kvfs: %v", err)
	}
}

// parseFlags reads the command line. A level the schema does not define is
// refused here, before any key is read (see SPEC.md, Initialization).
func parseFlags() config {
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis/Valkey host:port; a comma-separated list names the nodes of a cluster")
	ns := flag.String("ns", "default", "kvfs namespace")
	allowOther := flag.Bool("allow-other", false, "allow other users to access the mount")
	debug := flag.Bool("debug", false, "enable FUSE debug logging")
	opTimeout := flag.Duration("timeout", time.Second, "max Redis op duration")
	shardLevel := flag.String("shard-level", "namespace", "sharding level: namespace|dir|file")
	repair := flag.String("repair", "", "repair every component of this path (for example /conf/app.toml), print the outcome and exit; no mountpoint is needed")
	flag.Parse()

	level, err := store.ParseLevel(*shardLevel)
	if err != nil {
		log.Fatalf("-shard-level: %v", err)
	}
	cfg := config{
		addrs: strings.Split(*redisAddr, ","), ns: *ns, level: level, opTimeout: *opTimeout,
		allowOther: *allowOther, debug: *debug, repair: *repair,
	}
	if cfg.repair == "" {
		if flag.NArg() != 1 {
			fmt.Fprintf(os.Stderr, "usage: %s [flags] <mountpoint>\n", os.Args[0])
			flag.PrintDefaults()
			os.Exit(2)
		}
		cfg.mountpoint = flag.Arg(0)
	}
	return cfg
}

func run(cfg config) error {
	opt := rueidis.ClientOption{
		InitAddress:      cfg.addrs,
		DisableCache:     true,
		ConnWriteTimeout: cfg.opTimeout,
	}
	client, err := rueidis.NewClient(opt)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer client.Close()

	st, err := openStore(client, cfg)
	if err != nil {
		return err
	}
	if cfg.repair != "" {
		return runRepair(st, cfg)
	}
	// Separate connection for SUBSCRIBE so Dedicated MULTI never starves.
	subClient, err := rueidis.NewClient(opt)
	if err != nil {
		return fmt.Errorf("redis pubsub: %w", err)
	}
	defer subClient.Close()
	return mount(st, subClient, cfg)
}

// openStore pings the server, builds the store and bootstraps the namespace,
// which also verifies the level the namespace was written at.
func openStore(client rueidis.Client, cfg config) (*store.Store, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.opTimeout)
	defer cancel()
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	st, err := store.New(client, cfg.ns, cfg.level)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	st.SetOpTimeout(cfg.opTimeout)
	if err := st.BootstrapRoot(ctx, uint32(os.Getuid()), uint32(os.Getgid())); err != nil {
		return nil, fmt.Errorf("bootstrap: %w", err)
	}
	return st, nil
}

// runRepair is the operator's repair path: it settles or removes pending and
// dangling names along a path without mounting anything.
func runRepair(st *store.Store, cfg config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*cfg.opTimeout)
	defer cancel()
	repairs, err := st.RepairPath(ctx, cfg.repair)
	for _, r := range repairs {
		fmt.Printf("%s\t%s\n", r.Result, r.Path)
	}
	return err
}

func mount(st *store.Store, subClient rueidis.Client, cfg config) error {
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	root := kvfs.NewRoot(st, subClient, uid, gid)
	defer root.Close()

	timeout := time.Second
	negTimeout := 500 * time.Millisecond
	opts := &fs.Options{
		AttrTimeout:     &timeout,
		EntryTimeout:    &timeout,
		NegativeTimeout: &negTimeout,
		UID:             uid,
		GID:             gid,
	}
	opts.AllowOther = cfg.allowOther
	opts.FsName = "kvfs"
	opts.Name = "kvfs"
	opts.DisableXAttrs = true
	opts.Debug = cfg.debug
	opts.RootStableAttr = &fs.StableAttr{Ino: store.RootID, Mode: syscall.S_IFDIR}

	server, err := fs.Mount(cfg.mountpoint, root, opts)
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	log.Printf("kvfs mounted on %s (ns=%s redis=%s shard-level=%s timeout=%s)", cfg.mountpoint, cfg.ns, strings.Join(cfg.addrs, ","), cfg.level, cfg.opTimeout)

	go handleSignals(server, root, cfg.mountpoint)
	server.Wait()
	return nil
}

// handleSignals unmounts on SIGINT/SIGTERM and prints the health counters on
// SIGUSR1. It returns after the unmount.
func handleSignals(server *fuse.Server, root *kvfs.Root, mountpoint string) {
	sigc := make(chan os.Signal, 4)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)
	for sig := range sigc {
		if sig == syscall.SIGUSR1 {
			h := root.Health()
			log.Printf("kvfs health: unknown=%d pending=%d dangling=%d repaired=%d", h.Unknown, h.Pending, h.Dangling, h.Repaired)
			continue
		}
		log.Printf("unmounting %s", mountpoint)
		_ = server.Unmount()
		return
	}
}
