package solace

import "testing"

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input float64
		want  string
	}{
		{0, "0"},
		{500, "500"},
		{999999, "999999"},
		{1024 * 1024, "1.0M"},
		{1024 * 1024 * 1024, "1.0G"},
		{123.4, "123.4"},
	}

	for _, tt := range tests {
		got := formatBytes(tt.input)
		if got != tt.want {
			t.Errorf("formatBytes(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
