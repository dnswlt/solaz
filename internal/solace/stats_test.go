package solace

import "testing"

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input float64
		want  string
	}{
		{0, "0"},
		{500, "500"},
		{99999, "99999"},
		{100000, "97.7 kiB"},
		{1024 * 1024, "1.0 MiB"},
		{1024 * 1024 * 1024, "1.0 GiB"},
		{123.4, "123.4"},
	}

	for _, tt := range tests {
		got := formatBytes(tt.input)
		if got != tt.want {
			t.Errorf("formatBytes(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
