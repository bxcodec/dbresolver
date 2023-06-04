package dbresolver

import (
	"fmt"
	"testing"

	fuzz "github.com/google/gofuzz"
)

func FuzzMultiWrite(f *testing.F) {
	func() { // generate corpus

		var rdbCount, wdbCount, lbPolicyID uint8 = 1, 1, 1

		if !testing.Short() {
			fuzzer := fuzz.New()
			fuzzer.Fuzz(&rdbCount)
			fuzzer.Fuzz(&wdbCount)
			fuzzer.Fuzz(&lbPolicyID)
		}

		f.Add(wdbCount, rdbCount, lbPolicyID)
	}()

	f.Fuzz(func(t *testing.T, wdbCount, rdbCount, lbPolicyID uint8) { //next-release: uint8 -> uint

		policyID := lbPolicyID % uint8(len(LoadBalancerPolicies))

		config := DBConfig{
			wdbCount, rdbCount, LoadBalancerPolicies[policyID],
		}

		if config.primaryDBCount == 0 {
			t.Skipf("skipping due to mising primary db")
		}

		t.Log("dbConf", config)

		t.Run(fmt.Sprintf("%v", config), func(t *testing.T) {

			dbConf := config

			testMW(t, dbConf)
		})

	})
}

/*func FuzzTest(f *testing.F) {

	f.Add(1)

	f.Fuzz(func(t *testing.T, dbCount int) {
		t.Fatal(dbCount)
	})

}
*/
