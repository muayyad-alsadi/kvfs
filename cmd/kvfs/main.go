package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	kvfs "kvfs/internal/fs"
	"kvfs/internal/store"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/redis/rueidis"
)

func main() {
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis/Valkey host:port")
	ns := flag.String("ns", "default", "kvfs namespace")
	allowOther := flag.Bool("allow-other", false, "allow other users to access the mount")
	debug := flag.Bool("debug", false, "enable FUSE debug logging")
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <mountpoint>\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{*redisAddr},
		DisableCache: true,
	})
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	st := store.New(client, *ns)
	if err := st.BootstrapRoot(ctx, uid, gid); err != nil {
		log.Fatalf("bootstrap: %v", err)
	}

	root := kvfs.NewRoot(st, uid, gid)
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
	opts.AllowOther = *allowOther
	opts.FsName = "kvfs"
	opts.Name = "kvfs"
	opts.DisableXAttrs = true
	opts.Debug = *debug
	opts.RootStableAttr = &fs.StableAttr{
		Ino:  store.RootID,
		Mode: syscall.S_IFDIR,
	}

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		log.Fatalf("mount: %v", err)
	}

	log.Printf("kvfs mounted on %s (ns=%s redis=%s)", mountpoint, *ns, *redisAddr)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigc
		log.Printf("unmounting %s", mountpoint)
		_ = server.Unmount()
	}()

	server.Wait()
}
