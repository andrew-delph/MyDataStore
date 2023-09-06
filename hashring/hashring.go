package hashring

import (
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
)

func testHashring() {
	logrus.Warn("HASRING")
}

type Hashring struct{ hashringConfig config.HashringConfig }

func CreateHashring(hashringConfig config.HashringConfig) Hashring {
	return Hashring{hashringConfig: hashringConfig}
}
