// Copyright (c) 2026 Kenneth Stott
// Canary: 9b1f4e2d-3c6a-4d8e-8f0b-6a2e7c4d1b93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Package orders is a Go source's code, as an organisation would write it: no import of
// Provisa's, tags in doc comments, types in its signatures (REQ-2200). The same API as the Node
// and Python fixtures', but for what Go's standard library has no type of (a decimal, a calendar
// date). A table or stream takes its inputs as one struct (REQ-2100).
package orders

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Tier is an order's tier.
type Tier string

const (
	Gold   Tier = "gold"
	Silver Tier = "silver"
)

type Line struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

type Order struct {
	ID     int64     `json:"id"`
	Placed time.Time `json:"placed"`
	Tier   Tier      `json:"tier"`
	Lines  []Line    `json:"lines"`
	// Note is what the customer wrote.
	Note *string `json:"note"`
}

type OrdersInputs struct {
	CustomerID string     `json:"customer_id"`
	Since      *time.Time `json:"since"`
}

// Orders of one customer.
//
// @table
func Orders(in OrdersInputs) ([]Order, error) {
	var note *string
	if in.Since != nil {
		text := in.Since.Format(time.RFC3339)
		note = &text
	}
	placed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return []Order{{1 << 60, placed, Gold, []Line{{in.CustomerID, 2}}, note}}, nil
}

type ManyInputs struct {
	Count int `json:"count"`
}

// Many is rows of no declared shape: typed by a preview.
//
// @table
func Many(in ManyInputs) []map[string]any {
	rows := make([]map[string]any, in.Count)
	for i := range rows {
		rows[i] = map[string]any{"i": i}
	}
	return rows
}

type PlacedInputs struct {
	Region string `json:"region"`
}

// Placed is orders as they are placed.
//
// @stream
func Placed(ctx context.Context, in PlacedInputs) iter.Seq2[Order, error] {
	return func(yield func(Order, error) bool) {
		region := in.Region
		for n := int64(0); ; n++ {
			if !yield(Order{n, time.Now().UTC(), Silver, []Line{}, &region}, nil) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
}

// @stream
func Ticks() iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		for n := 0; ; n++ {
			if !yield(map[string]any{"tick": n}, nil) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// @stream
func Forever(ctx context.Context) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		<-ctx.Done()
	}
}

// @command
func CancelOrder(orderID int64) (map[string]any, error) {
	return map[string]any{"cancelled": orderID}, nil
}

// @table
func Boom() ([]map[string]any, error) {
	return nil, errors.New("no such customer")
}

// @table
func Outside() ([]map[string]any, error) {
	hosts, err := os.ReadFile("/etc/hosts")
	if err != nil {
		return nil, err
	}
	return []map[string]any{{"length": len(hosts)}}, nil
}

type FetchedInputs struct {
	URL string `json:"url"`
}

// @table
func Fetched(ctx context.Context, in FetchedInputs) ([]map[string]any, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, in.URL, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return []map[string]any{{"status": response.StatusCode, "body": string(body)}}, nil
}

type DirectInputs struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// @table
func Direct(in DirectInputs) ([]map[string]any, error) {
	connection, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", in.Host, in.Port), 5*time.Second)
	if err != nil {
		return nil, err
	}
	connection.Close()
	return []map[string]any{{"connected": true}}, nil
}

type GreetInputs struct {
	Name        string  `json:"name"`
	Greeting    *string `json:"greeting"`
	Punctuation *string `json:"punctuation"`
}

// @table
func Greet(in GreetInputs) []map[string]any {
	greeting, punctuation := "hello", "!"
	if in.Greeting != nil {
		greeting = *in.Greeting
	}
	if in.Punctuation != nil {
		punctuation = *in.Punctuation
	}
	return []map[string]any{{"text": greeting + " " + in.Name + punctuation}}
}

type Address struct {
	Street  string `json:"street"`
	City    string `json:"city"`
	Country string `json:"country"`
}

// CleanAddress is a batch column transform (REQ-2131): each raw address of the batch, as its
// parts.
//
// @columns
func CleanAddress(rawAddress []string) []Address {
	out := make([]Address, len(rawAddress))
	for i, raw := range rawAddress {
		parts := strings.Split(raw, ",")
		for j := range parts {
			parts[j] = strings.TrimSpace(parts[j])
		}
		out[i] = Address{parts[0], strings.Title(parts[1]), strings.ToUpper(parts[2])}
	}
	return out
}

// Untagged is not offered.
func Untagged() []Line { return nil }
