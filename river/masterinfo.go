package river

import (
	"strings"

	"github.com/siddontang/go-mysql/mysql"
)

type masterInfo interface {
	Save(pos mysql.Position) error
	Position() mysql.Position
	Close() error
}

func loadMasterInfo(dataPath string) (masterInfo, error) {
	if strings.HasPrefix(dataPath, "es:") {
		return loadElasticsearchMasterInfo(dataPath)
	}

	return loadFileMasterInfo(dataPath)
}
