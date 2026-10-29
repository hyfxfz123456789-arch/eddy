package controller

import "testing"

func TestOriginGTIDs(t *testing.T) {
	set := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-12,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-3"
	got := originGTIDs(set, "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA")
	if got != "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-12" {
		t.Fatalf("source GTIDs = %q", got)
	}
	if got := originGTIDs(set, "cccccccc-cccc-cccc-cccc-cccccccccccc"); got != "" {
		t.Fatalf("unknown source GTIDs = %q", got)
	}
}
