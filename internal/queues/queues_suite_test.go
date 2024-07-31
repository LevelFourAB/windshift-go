package queues_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestQueues(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Queues Suite")
}
