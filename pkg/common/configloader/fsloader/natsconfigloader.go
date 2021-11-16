package fsloader

import (
	"context"
	"errors"
	"knative.dev/eventing-natss/pkg/common/configloader"
)

// Key is used as the key for associating information with a context.Context.
type Key struct{}

func WithLoader(ctx context.Context, loader configloader.Loader) context.Context {
	return context.WithValue(ctx, Key{}, loader)
}

// Get obtains the configloader.Loader contained in a Context, if present
func Get(ctx context.Context) (configloader.Loader, error) {
	untyped := ctx.Value(Key{})
	if untyped == nil {
		return nil, errors.New("configloader.Loader does not exist in context")
	}
	return untyped.(configloader.Loader), nil
	// TODO: test the above works and use below if not.
	// return untyped.(func(data string) (map[string]string, error)), nil
}
