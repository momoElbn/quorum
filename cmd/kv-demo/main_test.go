package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestGetMissingKeyReturns404(t *testing.T) {
	srv := httptest.NewServer(newHandler(newStore()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/kv/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// Run with -race: the detector fails the test if the map is touched without the lock.
func TestConcurrentPutAndGet(t *testing.T) {
	srv := httptest.NewServer(newHandler(newStore()))
	defer srv.Close()

	const n = 100
	var wg sync.WaitGroup
	errs := make(chan error, 2*n)

	for i := 0; i < n; i++ {
		wg.Add(2)
		url := fmt.Sprintf("%s/kv/key-%d", srv.URL, i)
		want := fmt.Sprintf("value-%d", i)

		go func() { // writer
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPut, url, strings.NewReader(want))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errs <- err
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				errs <- fmt.Errorf("PUT %s: status %d", url, resp.StatusCode)
			}
		}()

		go func() { // reader, racing the writer for the same key
			defer wg.Done()
			resp, err := http.Get(url)
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			// Either the PUT has not landed yet (404) or we must see the exact value.
			switch resp.StatusCode {
			case http.StatusNotFound:
			case http.StatusOK:
				if string(body) != want {
					errs <- fmt.Errorf("GET %s = %q, want %q", url, body, want)
				}
			default:
				errs <- fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// After all writers finished, every key must be readable with its value.
	for i := 0; i < n; i++ {
		resp, err := http.Get(fmt.Sprintf("%s/kv/key-%d", srv.URL, i))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != fmt.Sprintf("value-%d", i) {
			t.Errorf("key-%d: status %d body %q", i, resp.StatusCode, body)
		}
	}
}
