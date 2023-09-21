package hashring

import (
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
)

type TestMember string

func (m TestMember) String() string {
	return string(m)
}

func TestHashringLoad(t *testing.T) {
	c := config.GetConfig().Manager
	c.PartitionCount = 100
	c.ReplicaCount = 5
	hr1 := CreateHashring(c)
	hr1.AddNode(TestMember("test11"))
	hr1.AddNode(TestMember("test22"))

	hr2 := CreateHashring(c)
	hr2.AddNode(TestMember("test11"))
	hr2.AddNode(TestMember("test22"))
	logrus.Warn(hr1)
}
