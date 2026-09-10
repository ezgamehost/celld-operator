/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestUpdateRaceSharesSeedETag(t *testing.T) {
	var mu sync.Mutex
	heads, attempts := 0, 0
	etag := `"seed"`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			heads++
			w.Header().Set("ETag", etag)
		case http.MethodPut:
			match := r.Header.Get("If-Match")
			if match == "" {
				return
			} // Seed.
			attempts++
			if match != `"seed"` {
				t.Errorf("writer used a later ETag: %s", match)
			}
			if match != etag {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte("<Error><Code>PreconditionFailed</Code></Error>"))
				return
			}
			etag = `"updated"`
			w.Header().Set("ETag", etag)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method: %s", r.Method)
		}
	}))
	defer server.Close()
	cli := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})
	f := flags{bucket: "test-bucket", writers: 8, rounds: 1, prefix: "test"}
	if violations := runPhase(context.Background(), cli, f, "update", raceUpdate); violations != 0 {
		t.Fatalf("valid store reported %d violations", violations)
	}
	if heads != 1 || attempts != f.writers {
		t.Fatalf("got %d HEADs and %d attempts", heads, attempts)
	}
}
