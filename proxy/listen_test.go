package main

import "testing"

func TestIsLoopbackListenAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addr string
		want bool
	}{
		{addr: "127.0.0.1:8787", want: true},
		{addr: "127.12.34.56:0", want: true},
		{addr: "[::1]:8787", want: true},
		{addr: ":8787", want: false},
		{addr: "0.0.0.0:8787", want: false},
		{addr: "[::]:8787", want: false},
		{addr: "10.0.0.5:8787", want: false},
		{addr: "localhost:8787", want: false},
		{addr: "127.0.0.1", want: false},
	}

	for _, test := range tests {
		t.Run(test.addr, func(t *testing.T) {
			t.Parallel()
			if got := isLoopbackListenAddr(test.addr); got != test.want {
				t.Fatalf("isLoopbackListenAddr(%q) = %v, want %v", test.addr, got, test.want)
			}
		})
	}
}
