package delays_test

import (
	"time"

	"github.com/levelfourab/windshift-go/delays"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Delays", func() {
	It("Never returns Stop", func() {
		decider := delays.Never()
		startTime := time.Now()
		Expect(decider(1, startTime)).To(Equal(delays.Stop))
	})

	It("Constant returns the same delay", func() {
		decider := delays.Constant(1 * time.Second)
		startTime := time.Now()
		Expect(decider(1, startTime)).To(Equal(1 * time.Second))
		Expect(decider(2, startTime)).To(Equal(1 * time.Second))
	})

	Describe("Exponential", func() {
		It("exponential delay with multiplier of 2", func() {
			decider := delays.Exponential(1*time.Second, 2)
			startTime := time.Now()
			Expect(decider(1, startTime)).To(Equal(1 * time.Second))
			Expect(decider(2, startTime)).To(Equal(2 * time.Second))
			Expect(decider(3, startTime)).To(Equal(4 * time.Second))
		})

		It("exponential delay with multiplier of 3", func() {
			decider := delays.Exponential(1*time.Second, 3)
			startTime := time.Now()
			Expect(decider(1, startTime)).To(Equal(1 * time.Second))
			Expect(decider(2, startTime)).To(Equal(3 * time.Second))
			Expect(decider(3, startTime)).To(Equal(9 * time.Second))
		})
	})

	Describe("Jitters", func() {
		It("WithJitter adds jitter to the delay", func() {
			decider := delays.WithJitter(delays.Constant(1*time.Second), 100*time.Millisecond)
			startTime := time.Now()

			Expect(decider(1, startTime)).To(BeNumerically("~", 1*time.Second, 100*time.Millisecond))
			Expect(decider(2, startTime)).To(BeNumerically("~", 1*time.Second, 100*time.Millisecond))
		})

		It("WithJitterFactor adds scaled jitter to the delay", func() {
			decider := delays.WithJitterFactor(delays.Exponential(1*time.Second, 2), 0.1)
			startTime := time.Now()
			Expect(decider(1, startTime)).To(BeNumerically("~", 1*time.Second, 100*time.Millisecond))
			Expect(decider(2, startTime)).To(BeNumerically("~", 2*time.Second, 200*time.Millisecond))
		})
	})

	Describe("WithMaxDelay", func() {
		It("returns the delay if it is less than the max delay", func() {
			decider := delays.WithMaxDelay(delays.Constant(1*time.Second), 2*time.Second)
			startTime := time.Now()
			Expect(decider(1, startTime)).To(Equal(1 * time.Second))
		})

		It("returns the max delay if the delay is greater", func() {
			decider := delays.WithMaxDelay(delays.Constant(3*time.Second), 2*time.Second)
			startTime := time.Now()
			Expect(decider(1, startTime)).To(Equal(2 * time.Second))
		})
	})

	Describe("StopAfterMaxAttempts", func() {
		It("returns Stop if the max attempts is reached", func() {
			decider := delays.StopAfterMaxAttempts(delays.Constant(1*time.Second), 2)
			startTime := time.Now()
			Expect(decider(1, startTime)).To(Equal(1 * time.Second))
			Expect(decider(2, startTime)).To(Equal(delays.Stop))
		})
	})

	Describe("StopAfterMaxDelay", func() {
		It("returns Stop if the max delay is reached", func() {
			decider := delays.StopAfterMaxDelay(delays.Exponential(1*time.Second, 2), 2*time.Second)
			startTime := time.Now()
			Expect(decider(1, startTime)).To(Equal(1 * time.Second))
			Expect(decider(2, startTime)).To(Equal(2 * time.Second))
			Expect(decider(3, startTime)).To(Equal(delays.Stop))
		})
	})

	Describe("StopAfterMaxTime", func() {
		It("returns Stop if the max time has passed", func() {
			decider := delays.StopAfterMaxTime(delays.Exponential(1*time.Second, 2), 2*time.Second)
			startTime := time.Now()
			Expect(decider(1, startTime)).To(Equal(1 * time.Second))
			Expect(decider(1, startTime.Add(-(2*time.Second + time.Millisecond)))).To(Equal(delays.Stop))
		})
	})
})
