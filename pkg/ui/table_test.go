package ui

import (
	"strings"
	"testing"
)

func TestTableRender(t *testing.T) {
	table := NewTable("TOPIC", "PARTITION", "STATUS")
	table.SetAlignment(AlignLeft, AlignRight, AlignCenter)
	table.AddRow("orders", "0", "[HEALTHY]")
	table.AddRow("payments", "12", "[STALLED]")

	out := table.Render()
	if !strings.Contains(out, "TOPIC") || !strings.Contains(out, "orders") || !strings.Contains(out, "[STALLED]") {
		t.Fatalf("Table render missing expected elements:\n%s", out)
	}

	// Verify em dash absence
	if strings.Contains(out, "\u2014") {
		t.Errorf("Table output contains em dash!")
	}
}

func TestBannerRender(t *testing.T) {
	meta := [][2]string{
		{"OPERATION", "TEST"},
		{"TARGET", "orders-group"},
	}
	b := Banner("TEST BANNER", meta)
	if !strings.Contains(b, "TEST BANNER") || !strings.Contains(b, "orders-group") {
		t.Fatalf("Banner render missing expected contents:\n%s", b)
	}
	if strings.Contains(b, "\u2014") {
		t.Errorf("Banner output contains em dash!")
	}
}
