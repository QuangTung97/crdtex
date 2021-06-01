package crdtex

import "time"

type serviceOptions struct {
	callRemoteTimeout time.Duration
	remoteAddresses   []string
}

type Option func(opts *serviceOptions)

func defaultServiceOptions() serviceOptions {
	return serviceOptions{
		callRemoteTimeout: 5 * time.Second,
	}
}

func computeOptions(options ...Option) serviceOptions {
	opts := defaultServiceOptions()
	for _, o := range options {
		o(&opts)
	}
	return opts
}

// AddRemoteAddress adds a remote address
func AddRemoteAddress(addr string) Option {
	return func(opts *serviceOptions) {
		opts.remoteAddresses = append(opts.remoteAddresses, addr)
	}
}
