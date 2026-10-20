package resource

import "testing"

// M4: an over-release must not panic; it clamps at zero and is counted.
func TestFixBudgetOverReleaseClamps(t *testing.T) {
	b := New(100)
	if !b.Try(10) {
		t.Fatal("try")
	}
	before := UnbalancedReleases()
	b.Put(30)
	if b.Used() != 0 {
		t.Fatalf("used=%d, want 0", b.Used())
	}
	if b.Overreleases() != 1 || UnbalancedReleases() != before+1 {
		t.Fatalf("overreleases=%d global delta=%d", b.Overreleases(), UnbalancedReleases()-before)
	}
	if !b.Try(100) {
		t.Fatal("budget unusable after clamped over-release")
	}
}
