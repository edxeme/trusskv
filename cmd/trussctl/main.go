// Command trussctl is a thin CLI for a truss cluster.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/edxeme/trusskv/internal/client"
)

const usage = `usage: trussctl [-server url] <command> [args]

commands:
  set <key> <value>   write a key (must reach the leader)
  get <key>           read a key
  del <key>           delete a key
  keys                list live keys
  status              node status

flags:
  -server url         truss node base URL (default http://localhost:7000)
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		var nle *client.NotLeaderError
		if errors.As(err, &nle) {
			fmt.Fprintln(os.Stderr, "trussctl:", err)
			os.Exit(3) // distinct code: retry against the leader
		}
		fmt.Fprintln(os.Stderr, "trussctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("trussctl", flag.ContinueOnError)
	server := fs.String("server", "http://localhost:7000", "truss node base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("missing command")
	}

	c := client.New(*server, nil)
	switch rest[0] {
	case "set":
		if len(rest) != 3 {
			return errors.New("usage: trussctl set <key> <value>")
		}
		return c.Set(rest[1], rest[2])
	case "get":
		if len(rest) != 2 {
			return errors.New("usage: trussctl get <key>")
		}
		v, ok, err := c.Get(rest[1])
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("key %q not found", rest[1])
		}
		fmt.Println(v)
		return nil
	case "del":
		if len(rest) != 2 {
			return errors.New("usage: trussctl del <key>")
		}
		return c.Delete(rest[1])
	case "keys":
		keys, err := c.Keys()
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		fmt.Println(strings.Join(keys, "\n"))
		return nil
	case "status":
		st, err := c.Status()
		if err != nil {
			return err
		}
		printf := func(format string, args ...any) { fmt.Printf(format+"\n", args...) }
		printf("id:      %s", st.ID)
		printf("role:    %s", st.Role)
		printf("term:    %d", st.Term)
		printf("leader:  %s", st.Leader)
		printf("last:    index %d", st.LastIndex)
		printf("keys:    %d", st.Keys)
		printf("peers:   %s", strings.Join(st.Peers, ", "))
		printf("uptime:  %d ms", st.UptimeMS)
		return nil
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", rest[0])
	}
}
