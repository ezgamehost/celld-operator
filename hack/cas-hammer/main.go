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

// cas-hammer probes an S3-compatible object store for the property celld's
// fencing depends on and a sequential test cannot prove: that conditional
// PUTs are enforced ATOMICALLY under concurrent racers (docs/celld-behaviors.md).
//
// celld's own contract test (put_cas_contract in crates/celld/bucket.rs)
// verifies the four sequential transitions — create / reject-create /
// update / reject-stale. This tool adds the race: each round, N writers PUT
// the same key with the same precondition, and exactly one must win. A
// store that accepts the headers without enforcing them, or enforces them
// without atomicity, admits more than one winner — which in a celld fleet
// is two owners for one cell.
//
// Usage:
//
//	cas-hammer --bucket BUCKET [--endpoint URL] [--region REGION] \
//	           [--writers 8] [--rounds 32] [--prefix cas-hammer]
//
// Credentials come from the standard AWS chain. Exit status 0 means every
// round had exactly one winner; 1 means the store violated the contract;
// 2 means the run could not complete. Run it against every candidate store
// and again on every store upgrade.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type flags struct {
	bucket   string
	endpoint string
	region   string
	writers  int
	rounds   int
	prefix   string
}

func main() {
	var f flags
	flag.StringVar(&f.bucket, "bucket", "", "bucket name (required)")
	flag.StringVar(&f.endpoint, "endpoint", "", "S3-compatible endpoint URL; empty for AWS S3")
	flag.StringVar(&f.region, "region", "auto", "storage region")
	flag.IntVar(&f.writers, "writers", 8, "concurrent writers per round")
	flag.IntVar(&f.rounds, "rounds", 32, "rounds per phase")
	flag.StringVar(&f.prefix, "prefix", "cas-hammer", "key prefix for probe objects")
	flag.Parse()
	if f.bucket == "" {
		fmt.Fprintln(os.Stderr, "--bucket is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(f.region))
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading AWS config: %v\n", err)
		os.Exit(2)
	}
	cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if f.endpoint != "" {
			o.BaseEndpoint = aws.String(f.endpoint)
			// Most non-AWS S3 implementations want path-style addressing.
			o.UsePathStyle = true
		}
	})

	violations := 0
	violations += runPhase(ctx, cli, f, "create", raceCreate)
	violations += runPhase(ctx, cli, f, "update", raceUpdate)

	if violations > 0 {
		fmt.Printf("\nFAIL: %d rounds admitted more than one winner — this store cannot fence celld cells\n", violations)
		os.Exit(1)
	}
	fmt.Println("\nOK: every round had exactly one winner (atomicity holds for this run; rerun per store release)")
}

type racer func(ctx context.Context, cli *s3.Client, f flags, key string, writer int) (won bool, err error)

func runPhase(ctx context.Context, cli *s3.Client, f flags, name string, race racer) int {
	violations := 0
	for round := 0; round < f.rounds; round++ {
		key := fmt.Sprintf("%s/%s/%d-%d", f.prefix, name, time.Now().UnixNano(), round)

		// The update phase races If-Match against a seeded object; the
		// create phase races If-None-Match:* against an absent key.
		if name == "update" {
			if _, err := cli.PutObject(ctx, &s3.PutObjectInput{
				Bucket: &f.bucket, Key: &key, Body: bytes.NewReader([]byte("seed")),
			}); err != nil {
				fmt.Fprintf(os.Stderr, "round %d: seeding: %v\n", round, err)
				os.Exit(2)
			}
		}

		var mu sync.Mutex
		winners := 0
		var hardErr error
		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := 0; w < f.writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				won, err := race(ctx, cli, f, key, w)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					hardErr = err
					return
				}
				if won {
					winners++
				}
			}(w)
		}
		close(start)
		wg.Wait()

		if hardErr != nil {
			fmt.Fprintf(os.Stderr, "round %d (%s): %v\n", round, name, hardErr)
			os.Exit(2)
		}
		status := "ok"
		if winners != 1 {
			status = "VIOLATION"
			violations++
		}
		fmt.Printf("%s round %2d: winners=%d %s\n", name, round, winners, status)

		_, _ = cli.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &f.bucket, Key: &key})
	}
	return violations
}

// raceCreate: N writers race a conditional create (If-None-Match: *) on an
// absent key — celld's ownership-record acquisition and epoch seal.
func raceCreate(ctx context.Context, cli *s3.Client, f flags, key string, writer int) (bool, error) {
	body := fmt.Sprintf("writer-%d", writer)
	_, err := cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &f.bucket,
		Key:         &key,
		Body:        bytes.NewReader([]byte(body)),
		IfNoneMatch: aws.String("*"),
	})
	return classify(err)
}

// raceUpdate: N writers read the seeded object's ETag, then race a
// conditional overwrite (If-Match) — celld's ownership compare-and-swap.
// Every writer holds the SAME valid ETag, so atomicity alone decides.
func raceUpdate(ctx context.Context, cli *s3.Client, f flags, key string, writer int) (bool, error) {
	head, err := cli.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &f.bucket, Key: &key})
	if err != nil {
		return false, fmt.Errorf("head before CAS: %w", err)
	}
	body := fmt.Sprintf("writer-%d", writer)
	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  &f.bucket,
		Key:     &key,
		Body:    bytes.NewReader([]byte(body)),
		IfMatch: head.ETag,
	})
	return classify(err)
}

// classify maps a conditional-PUT result to won/lost. A clean loss is a 412
// PreconditionFailed (or 409 ConditionalRequestConflict, which S3 returns
// when concurrent conditional writes collide mid-flight). Anything else is
// a hard error: the store did not answer the protocol celld speaks.
func classify(err error) (bool, error) {
	if err == nil {
		return true, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return false, nil
		}
	}
	return false, err
}
