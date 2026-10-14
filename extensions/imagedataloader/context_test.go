package imagedataloader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"
)

func TestImageContextGetCacheHitReleasesReadLock(t *testing.T) {
	data := &ImageData{}
	ctx := &imageContext{
		list: map[string]*ImageData{"example.com/image:latest": data},
	}

	got, err := ctx.Get(context.Background(), "example.com/image:latest", nil, nil)
	require.NoError(t, err)
	require.Same(t, data, got)

	acquired := make(chan struct{})
	go func() {
		ctx.Lock()
		defer ctx.Unlock()
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("write lock remained blocked after a cache hit")
	}
}

type fakeFetcher struct {
	mu    sync.Mutex
	calls map[string]int
}

func (f *fakeFetcher) FetchImageData(_ context.Context, image string, _ []remote.Option, _ []name.Option) (*ImageData, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[image]++
	if image == "example.com/broken:latest" {
		return nil, errors.New("fetch failed")
	}
	return &ImageData{ImageDescriptor: ImageDescriptor{ImageReference: ImageReference{Image: image}}}, nil
}

func TestImageContextAddImagesConcurrentWorkers(t *testing.T) {
	images := make([]string, 0, 2*workers)
	for i := range workers {
		img := fmt.Sprintf("example.com/image-%d:latest", i)
		images = append(images, img, img)
	}
	fetcher := &fakeFetcher{calls: map[string]int{}}
	ctx := &imageContext{
		f:    fetcher,
		list: map[string]*ImageData{},
	}

	require.NoError(t, ctx.AddImages(context.Background(), images, nil, nil))
	require.Len(t, ctx.list, workers)
	for img, data := range ctx.list {
		require.Equal(t, img, data.Image)
		require.Equal(t, 1, fetcher.calls[img])
	}
}

func TestImageContextAddImagesKeepsFetchedImagesOnError(t *testing.T) {
	fetcher := &fakeFetcher{calls: map[string]int{}}
	ctx := &imageContext{
		f:    fetcher,
		list: map[string]*ImageData{},
	}

	err := ctx.AddImages(context.Background(), []string{"example.com/image:latest", "example.com/broken:latest"}, nil, nil)
	require.Error(t, err)
	require.Contains(t, ctx.list, "example.com/image:latest")
	require.NotContains(t, ctx.list, "example.com/broken:latest")
}
