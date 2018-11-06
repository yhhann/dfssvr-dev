package metadata

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2"
)

// This source file is copied from gopkg.in/mgo.v2, and changed it little
// for compatible with java driver.
type DialInfo struct {
	mgo.DialInfo
	SlaveOk bool
}

// For log usage.
func (i *DialInfo) String() string {
	return fmt.Sprintf(" slaveOk: %t, failfast %t, poolSize %d, timeout %v.", i.SlaveOk, i.FailFast, i.PoolLimit, i.Timeout)
}

// ParseURL parses a MongoDB URL as accepted by the Dial function and returns
// a value suitable for providing into DialWithInfo.
func ParseURL(url string) (*DialInfo, error) {
	uinfo, err := ExtractURL(url)
	if err != nil {
		return nil, err
	}
	direct := false
	mechanism := ""
	service := ""
	source := ""
	setName := ""
	poolLimit := 0
	timeout := 0
	slaveok := false
	for k, v := range uinfo.options {
		switch k {
		case "authSource":
			source = v
		case "authMechanism":
			mechanism = v
		case "gssapiServiceName":
			service = v
		case "replicaSet":
			setName = v
		case "maxpoolsize": // for compatible with java.
			fallthrough
		case "maxPoolSize":
			poolLimit, err = strconv.Atoi(v)
			if err != nil {
				return nil, errors.New("bad value for maxPoolSize: " + v)
			}
		case "connecttimeoutms": // compatible with java.
			timeout, err = strconv.Atoi(v)
			timeout /= 1000
			if err != nil {
				return nil, errors.New("bad value for connecttimeoutms: " + v)
			}
		case "timeout": // add for dfs2.0.
			timeout, err = strconv.Atoi(v)
			if err != nil {
				return nil, errors.New("bad value for timeout: " + v)
			}
		case "slaveOk": // add for dfs2.0.
			fallthrough
		case "slaveok":
			slaveok = "true" == v
		case "connect":
			if v == "direct" {
				direct = true
				break
			}
			if v == "replicaSet" {
				break
			}
			fallthrough
		default:
			glog.Warningf("unsupported connection URL option: %s=%s", k, v)
		}
	}
	info := DialInfo{
		DialInfo: mgo.DialInfo{
			Addrs:          uinfo.addrs,
			Direct:         direct,
			Database:       uinfo.db,
			Username:       uinfo.user,
			Password:       uinfo.pass,
			Mechanism:      mechanism,
			Service:        service,
			Source:         source,
			PoolLimit:      poolLimit,
			ReplicaSetName: setName,
			Timeout:        time.Duration(timeout) * time.Second,
		},
		SlaveOk: slaveok,
	}
	return &info, nil
}

type urlInfo struct {
	addrs   []string
	user    string
	pass    string
	db      string
	options map[string]string
}

func ExtractURL(s string) (*urlInfo, error) {
	if strings.HasPrefix(s, "mongodb://") {
		s = s[10:]
	}
	info := &urlInfo{options: make(map[string]string)}
	if c := strings.Index(s, "?"); c != -1 {
		for _, pair := range strings.FieldsFunc(s[c+1:], isOptSep) {
			l := strings.SplitN(pair, "=", 2)
			if len(l) != 2 || l[0] == "" || l[1] == "" {
				return nil, errors.New("connection option must be key=value: " + pair)
			}
			info.options[l[0]] = l[1]
		}
		s = s[:c]
	}
	if c := strings.Index(s, "@"); c != -1 {
		pair := strings.SplitN(s[:c], ":", 2)
		if len(pair) > 2 || pair[0] == "" {
			return nil, errors.New("credentials must be provided as user:pass@host")
		}
		var err error
		info.user, err = url.QueryUnescape(pair[0])
		if err != nil {
			return nil, fmt.Errorf("cannot unescape username in URL: %q", pair[0])
		}
		if len(pair) > 1 {
			info.pass, err = url.QueryUnescape(pair[1])
			if err != nil {
				return nil, fmt.Errorf("cannot unescape password in URL")
			}
		}
		s = s[c+1:]
	}
	if c := strings.Index(s, "/"); c != -1 {
		info.db = s[c+1:]
		s = s[:c]
	}
	info.addrs = strings.Split(s, ",")
	return info, nil
}

func isOptSep(c rune) bool {
	return c == ';' || c == '&'
}
