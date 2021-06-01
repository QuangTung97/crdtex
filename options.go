package crdtex

import "time"

type serviceOptions struct {
	callRemoteTimeout time.Duration
	remoteAddresses   []string
	syncDuration      time.Duration
	expireDuration    time.Duration
}

// Option ...
type Option func(opts *serviceOptions)

func defaultServiceOptions() serviceOptions {
	return serviceOptions{
		callRemoteTimeout: 5 * time.Second,
		syncDuration:      5 * time.Second,
		expireDuration:    60 * time.Second,
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

// WithSyncDuration ...
func WithSyncDuration(d time.Duration) Option {
	return func(opts *serviceOptions) {
		opts.syncDuration = d
	}
}

// WithExpireDuration ...
func WithExpireDuration(d time.Duration) Option {
	return func(opts *serviceOptions) {
		opts.expireDuration = d
	}
}
