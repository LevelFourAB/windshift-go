package events_test

import (
	"github.com/levelfourab/windshift-go/events"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Names", func() {
	It("IsValidSubject: No Wildcards", func() {
		Expect(events.IsValidSubject("", false)).To(BeFalse())

		Expect(events.IsValidSubject("a", false)).To(BeTrue())
		Expect(events.IsValidSubject("A", false)).To(BeTrue())
		Expect(events.IsValidSubject("0", false)).To(BeTrue())
		Expect(events.IsValidSubject("_", false)).To(BeTrue())
		Expect(events.IsValidSubject("-", false)).To(BeTrue())

		Expect(events.IsValidSubject("%", false)).To(BeFalse())
		Expect(events.IsValidSubject("a/b", false)).To(BeFalse())

		Expect(events.IsValidSubject(".", false)).To(BeFalse())
		Expect(events.IsValidSubject("a.b", false)).To(BeTrue())
		Expect(events.IsValidSubject("a..b", false)).To(BeFalse())
		Expect(events.IsValidSubject("a.b.c", false)).To(BeTrue())
		Expect(events.IsValidSubject("a.b.c.", false)).To(BeFalse())

		Expect(events.IsValidSubject("a.b.*", false)).To(BeFalse())
		Expect(events.IsValidSubject("a.b.>", false)).To(BeFalse())
	})

	It("IsValidSubject: Wildcards", func() {
		Expect(events.IsValidSubject("", true)).To(BeFalse())

		Expect(events.IsValidSubject("a", true)).To(BeTrue())
		Expect(events.IsValidSubject("A", true)).To(BeTrue())
		Expect(events.IsValidSubject("0", true)).To(BeTrue())
		Expect(events.IsValidSubject("_", true)).To(BeTrue())
		Expect(events.IsValidSubject("-", true)).To(BeTrue())

		Expect(events.IsValidSubject("%", true)).To(BeFalse())
		Expect(events.IsValidSubject("a/b", true)).To(BeFalse())

		Expect(events.IsValidSubject(".", true)).To(BeFalse())
		Expect(events.IsValidSubject("a.b", true)).To(BeTrue())
		Expect(events.IsValidSubject("a..b", true)).To(BeFalse())
		Expect(events.IsValidSubject("a.b.c", true)).To(BeTrue())
		Expect(events.IsValidSubject("a.b.c.", true)).To(BeFalse())

		Expect(events.IsValidSubject("a.b.c", true)).To(BeTrue())
		Expect(events.IsValidSubject("a.b.*", true)).To(BeTrue())
		Expect(events.IsValidSubject("a.b.>", true)).To(BeTrue())
		Expect(events.IsValidSubject("a.b.c.d", true)).To(BeTrue())
		Expect(events.IsValidSubject("a.b.*.d", true)).To(BeTrue())
		Expect(events.IsValidSubject("a.b.>.d", true)).To(BeFalse())
		Expect(events.IsValidSubject("a.b*", true)).To(BeFalse())
		Expect(events.IsValidSubject("a.b>", true)).To(BeFalse())
	})

	It("IsValidConsumerName", func() {
		Expect(events.IsValidConsumerName("")).To(BeFalse())

		Expect(events.IsValidConsumerName("a")).To(BeTrue())
		Expect(events.IsValidConsumerName("A")).To(BeTrue())
		Expect(events.IsValidConsumerName("0")).To(BeTrue())
		Expect(events.IsValidConsumerName("_")).To(BeTrue())
		Expect(events.IsValidConsumerName("-")).To(BeTrue())

		Expect(events.IsValidConsumerName(".")).To(BeFalse())
		Expect(events.IsValidConsumerName("a.b")).To(BeFalse())
		Expect(events.IsValidConsumerName("a..b")).To(BeFalse())
		Expect(events.IsValidConsumerName("a.b.c")).To(BeFalse())
		Expect(events.IsValidConsumerName("a.b.c.")).To(BeFalse())

		Expect(events.IsValidConsumerName("a.b.*")).To(BeFalse())
		Expect(events.IsValidConsumerName("a.b.>")).To(BeFalse())
	})
})
