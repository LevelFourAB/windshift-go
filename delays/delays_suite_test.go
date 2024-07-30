package delays_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDelays(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Delays Suite")
}
