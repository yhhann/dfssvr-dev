package sql

import (
	"strings"

	"github.com/go-sql-driver/mysql"
)

func ConvertDSN(originDSN string) ([]string, error) {
	originCnf, err := mysql.ParseDSN(originDSN)
	if err != nil {
		return nil, err
	}

	addr := originCnf.Addr

	right := strings.Index(addr, "]")
	if right == -1 {
		right = len(addr)
	}
	addr = addr[strings.Index(addr, "[")+1 : right]

	addrs := strings.Split(addr, ",")

	dsns := make([]string, 0, len(addrs))
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		cnf := *originCnf
		cnf.Addr = a
		dsns = append(dsns, cnf.FormatDSN())
	}

	return dsns, nil
}
