module github.com/malbeclabs/edge-multicast-ref/go/kernel-receiver

go 1.23.0

toolchain go1.23.7

require (
	github.com/BurntSushi/toml v1.5.0
	github.com/malbeclabs/edge-multicast-ref/go/internal v0.0.0
	golang.org/x/net v0.40.0
)

require golang.org/x/sys v0.33.0 // indirect

replace github.com/malbeclabs/edge-multicast-ref/go/internal => ../internal
