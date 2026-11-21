// Copyright (c) 2026 Kenneth Stott
// Canary: 19161843-1d3a-4d96-ac31-72e733034fc1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Package provisa_services is Provisa's services to a Go code source's code (REQ-2104, REQ-2105,
// REQ-2106, REQ-2200).
//
// Always present where a Go source's code is compiled: Provisa puts the module in the build, so
// the code's go.mod need not name it (a go.mod that names it, for an author's own stub outside
// Provisa, has that replaced by this one). Outside Provisa it is not there, and code that imports
// it is tested by mocking it.
//
//	import ps "provisa_services"
//
//	key, err := ps.Secrets.Get(ctx, "openrouter")
//	region, err := ps.Settings.Get(ctx, "region")
//	err = ps.Cache.Set(ctx, "rates", rates, 3600)
//	rates, err := ps.Cache.Get(ctx, "rates")
//	n, err := ps.Vectors.Upsert(ctx, items, "text-embedding-3-small")
//	found, err := ps.Vectors.Search(ctx, "late payment", 5, "text-embedding-3-small")
//	answered, err := ps.HTTP.Get(ctx, "https://api.example.com/rates")
//	reply, err := ps.LLM.Chat(ctx, "provisa:sql_generation", messages)
//	err = ps.Log(ctx, "scored", "info", map[string]any{"accounts": 12})
//	err = ps.Metric(ctx, "scored", 12, "counter", nil)
//	err = ps.Audit(ctx, "spark.run", map[string]any{"application_id": id, "rows": 12})
//
// The same calls, by the same names, as Python's provisa_services and Node's @provisa/services.
// Each is made to $PROVISA_SERVICES through the worker's egress proxy.
//
// Every call takes the context first. A Go program has no per-goroutine state to keep the call
// in, so the context the function was given (a function that takes a context.Context first is
// given its call's) is what says which call this serves, so that what the service does is
// recorded against the request that called the code. Pass that context, or one derived from it;
// a call made with any other context serves no request.
//
// A refusal is returned as a *ServiceError, naming the service; with no $PROVISA_SERVICES every
// call returns one.
package provisa_services

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// any is the predeclared name of Go 1.18. This module says go 1.16, the version a go.mod with no
// go line means, so that it can be required by the code of any Go source.
type any = interface{}

// ServiceError is a service refusing or failing: Service names it, Status is its HTTP status (0
// when the services are not reachable at all), and Message says why.
type ServiceError struct {
	Service string
	Status  int
	Message string
}

func (e *ServiceError) Error() string { return e.Service + ": " + e.Message }

type callKey struct{}

// boundCall is the call a context serves: its id, and the request's traceparent.
type boundCall struct {
	id          uint64
	traceparent string
}

// BindCall is the host's: a context that serves call id of the worker, under the request's
// traceparent ("" when the request has no trace).
func BindCall(ctx context.Context, id uint64, traceparent string) context.Context {
	return context.WithValue(ctx, callKey{}, boundCall{id, traceparent})
}

// Traceparent is the W3C traceparent of the request this context serves, "" when it serves none
// or the request has no trace: what the code's own OpenTelemetry extracts to join that trace.
func Traceparent(ctx context.Context) string {
	held, _ := ctx.Value(callKey{}).(boundCall)
	return held.traceparent
}

func base(service string) (string, error) {
	value := os.Getenv("PROVISA_SERVICES")
	if value == "" {
		return "", &ServiceError{service, 0, "Provisa's services are reached only from a Provisa code source's worker ($PROVISA_SERVICES is not set)"}
	}
	if !strings.HasPrefix(value, "http://") {
		return "", &ServiceError{service, 0, fmt.Sprintf("$PROVISA_SERVICES is not an http:// address: %q", value)}
	}
	return value, nil
}

// The transport is the default one, which goes through the worker's egress proxy.
var client = &http.Client{Timeout: 300 * time.Second}

func send(ctx context.Context, service, method, path string, payload any) (int, []byte, error) {
	root, err := base(service)
	if err != nil {
		return 0, nil, err
	}
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, &ServiceError{service, 0, "what was given is not JSON: " + err.Error()}
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, root+"/"+path, body)
	if err != nil {
		return 0, nil, &ServiceError{service, 0, err.Error()}
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if held, ok := ctx.Value(callKey{}).(boundCall); ok {
		request.Header.Set("X-Provisa-Call", strconv.FormatUint(held.id, 10))
		if held.traceparent != "" {
			request.Header.Set("traceparent", held.traceparent)
		}
	}
	answered, err := client.Do(request)
	if err != nil {
		return 0, nil, &ServiceError{service, 0, err.Error()}
	}
	defer answered.Body.Close()
	text, err := io.ReadAll(answered.Body)
	if err != nil {
		return 0, nil, &ServiceError{service, 0, err.Error()}
	}
	return answered.StatusCode, text, nil
}

func reason(body []byte) string {
	var refusal struct {
		Error *struct {
			Message *string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &refusal) == nil && refusal.Error != nil && refusal.Error.Message != nil {
		return *refusal.Error.Message
	}
	return string(body)
}

// decode reads a service's JSON answer into out.
func decode(service string, body []byte, out any) error {
	if err := json.Unmarshal(body, out); err != nil {
		return &ServiceError{service, 200, "its answer is not JSON: " + err.Error()}
	}
	return nil
}

// ask makes a request whose answer is JSON, read into out (nil: not read).
func ask(ctx context.Context, service, method, path string, payload, out any) error {
	status, body, err := send(ctx, service, method, path, payload)
	if err != nil {
		return err
	}
	if status != 200 {
		return &ServiceError{service, status, reason(body)}
	}
	if out == nil {
		return nil
	}
	return decode(service, body, out)
}

// q is a name as one segment of a request's path.
func q(name string) string { return url.PathEscape(name) }

func orEmpty(fields map[string]any) map[string]any {
	if fields == nil {
		return map[string]any{}
	}
	return fields
}

// optional is a trailing optional argument as the wire has it: null when it was left out.
func optional(given []string) any {
	if len(given) == 0 {
		return nil
	}
	return given[0]
}

type secretsService struct{}

// Secrets are the source's declared secrets, each read from the org's vault when asked for.
var Secrets secretsService

// Get is the declared secret name, read from the org's vault now.
func (secretsService) Get(ctx context.Context, name string) (string, error) {
	status, body, err := send(ctx, "secrets", "GET", "secrets/"+q(name), nil)
	if err != nil {
		return "", err
	}
	if status != 200 {
		return "", &ServiceError{"secrets", status, string(body)}
	}
	return string(body), nil
}

type settingsService struct{}

// Settings are the source's declared non-secret settings.
var Settings settingsService

// Get is the source's setting name; fallback (at most one is read) when it declares none, and a
// *ServiceError when it declares none and no fallback is given.
func (settingsService) Get(ctx context.Context, name string, fallback ...any) (any, error) {
	status, body, err := send(ctx, "settings", "GET", "settings/"+q(name), nil)
	if err != nil {
		return nil, err
	}
	if status == 404 && len(fallback) > 0 {
		return fallback[0], nil
	}
	if status != 200 {
		return nil, &ServiceError{"settings", status, reason(body)}
	}
	var held struct {
		Value any `json:"value"`
	}
	if err := decode("settings", body, &held); err != nil {
		return nil, err
	}
	return held.Value, nil
}

// All is every setting the source declares.
func (settingsService) All(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	err := ask(ctx, "settings", "GET", "settings", nil, &out)
	return out, err
}

type cacheService struct{}

// Cache is shared by every worker of the source.
var Cache cacheService

// Get is the cached value of key; fallback (at most one is read, else nil) when none is cached.
func (cacheService) Get(ctx context.Context, key string, fallback ...any) (any, error) {
	status, body, err := send(ctx, "cache", "GET", "cache/"+q(key), nil)
	if err != nil {
		return nil, err
	}
	if status == 404 {
		if len(fallback) > 0 {
			return fallback[0], nil
		}
		return nil, nil
	}
	if status != 200 {
		return nil, &ServiceError{"cache", status, reason(body)}
	}
	var held struct {
		Value any `json:"value"`
	}
	if err := decode("cache", body, &held); err != nil {
		return nil, err
	}
	return held.Value, nil
}

// Set caches value (JSON) under key for every worker of this source, for ttl seconds (at most one
// is read; left out: until deleted or replaced).
func (cacheService) Set(ctx context.Context, key string, value any, ttl ...float64) error {
	var seconds any
	if len(ttl) > 0 {
		seconds = ttl[0]
	}
	return ask(ctx, "cache", "PUT", "cache/"+q(key), map[string]any{"value": value, "ttl": seconds}, nil)
}

// Delete removes key.
func (cacheService) Delete(ctx context.Context, key string) error {
	return ask(ctx, "cache", "DELETE", "cache/"+q(key), nil, nil)
}

type vectorsService struct{}

// Vectors is the source's own vector collection.
var Vectors vectorsService

// Embed is each text's vector, by model, one of the org's vector models.
func (vectorsService) Embed(ctx context.Context, texts []string, model string) ([][]float64, error) {
	var out struct {
		Vectors [][]float64 `json:"vectors"`
	}
	err := ask(ctx, "vectors", "POST", "vectors/embed", map[string]any{"texts": texts, "model": model}, &out)
	return out.Vectors, err
}

// Upsert adds or replaces items, each {id, text | vector, metadata}; a text is embedded by model
// (at most one is read). It answers how many.
func (vectorsService) Upsert(ctx context.Context, items []map[string]any, model ...string) (int, error) {
	var out struct {
		Upserted int `json:"upserted"`
	}
	err := ask(ctx, "vectors", "POST", "vectors/upsert", map[string]any{"items": items, "model": optional(model)}, &out)
	return out.Upserted, err
}

// Search is the k items nearest query (a string, embedded by model, of which at most one is read,
// or a []float64): each {id, score, text, metadata}, nearest first.
func (vectorsService) Search(ctx context.Context, query any, k int, model ...string) ([]map[string]any, error) {
	var out struct {
		Matches []map[string]any `json:"matches"`
	}
	err := ask(ctx, "vectors", "POST", "vectors/search", map[string]any{"query": query, "k": k, "model": optional(model)}, &out)
	return out.Matches, err
}

// Delete removes the items of ids.
func (vectorsService) Delete(ctx context.Context, ids []string) error {
	return ask(ctx, "vectors", "POST", "vectors/delete", map[string]any{"ids": ids}, nil)
}

// Response is what a fetch through Provisa answered.
type Response struct {
	Status  int
	Headers map[string]any
	Content []byte
}

// Text is the answer's body as text.
func (r *Response) Text() string { return string(r.Content) }

// JSON is the answer's body, parsed.
func (r *Response) JSON() (any, error) {
	var out any
	err := json.Unmarshal(r.Content, &out)
	return out, err
}

func response(ctx context.Context, request map[string]any) (*Response, error) {
	const service = "http"
	var out struct {
		Status     int            `json:"status"`
		Headers    map[string]any `json:"headers"`
		Body       string         `json:"body"`
		BodyBase64 *string        `json:"body_base64"`
	}
	if err := ask(ctx, service, "POST", "http", request, &out); err != nil {
		return nil, err
	}
	content := []byte(out.Body)
	if out.BodyBase64 != nil {
		decoded, err := base64.StdEncoding.DecodeString(*out.BodyBase64)
		if err != nil {
			return nil, &ServiceError{service, 200, "its answer's body is not base64: " + err.Error()}
		}
		content = decoded
	}
	return &Response{out.Status, out.Headers, content}, nil
}

// HTTPOptions are what a fetch may add. Timeout is in seconds (0: the service's own). For a
// post, JSON (encoded, with its content type unless Headers names one) is the body if given, else
// Body, a string or a []byte.
type HTTPOptions struct {
	Headers map[string]string
	Timeout float64
	Body    any
	JSON    any
}

func (o HTTPOptions) timeout() any {
	if o.Timeout == 0 {
		return nil
	}
	return o.Timeout
}

func (o HTTPOptions) headers() map[string]string {
	sent := make(map[string]string, len(o.Headers))
	for name, value := range o.Headers {
		sent[name] = value
	}
	return sent
}

func first(options []HTTPOptions) HTTPOptions {
	if len(options) == 0 {
		return HTTPOptions{}
	}
	return options[0]
}

type httpService struct{}

// HTTP is a fetch Provisa makes for the code: to a host on the egress allow-list, recorded.
var HTTP httpService

// Get fetches url (at most one options is read).
func (httpService) Get(ctx context.Context, url string, options ...HTTPOptions) (*Response, error) {
	o := first(options)
	return response(ctx, map[string]any{"method": "GET", "url": url, "headers": o.headers(), "timeout": o.timeout()})
}

// Post posts to url (at most one options is read).
func (httpService) Post(ctx context.Context, url string, options ...HTTPOptions) (*Response, error) {
	o := first(options)
	sent := o.headers()
	request := map[string]any{"method": "POST", "url": url, "timeout": o.timeout()}
	if o.JSON != nil {
		if _, named := sent["Content-Type"]; !named {
			sent["Content-Type"] = "application/json"
		}
		data, err := json.Marshal(o.JSON)
		if err != nil {
			return nil, &ServiceError{"http", 0, "what was given is not JSON: " + err.Error()}
		}
		request["body"] = string(data)
	} else {
		switch body := o.Body.(type) {
		case nil:
		case []byte:
			request["body_base64"] = base64.StdEncoding.EncodeToString(body)
		case string:
			request["body"] = body
		default:
			return nil, &ServiceError{"http", 0, fmt.Sprintf("a post's Body is a string or a []byte, not a %T", body)}
		}
	}
	request["headers"] = sent
	return response(ctx, request)
}

type llmService struct{}

// LLM is Provisa's OpenAI-compatible LLM service.
var LLM llmService

// BaseURL is the service's address: any OpenAI SDK also works at it.
func (llmService) BaseURL() (string, error) {
	root, err := base("llm")
	if err != nil {
		return "", err
	}
	return root + "/llm/v1", nil
}

// Chat is a chat completion: model is provisa:<role> or a model the org assigns; params (at most
// one is read) go beside them in the request.
func (llmService) Chat(ctx context.Context, model string, messages []map[string]any, params ...map[string]any) (map[string]any, error) {
	request := map[string]any{"model": model, "messages": messages}
	if len(params) > 0 {
		for name, value := range params[0] {
			request[name] = value
		}
	}
	var out map[string]any
	err := ask(ctx, "llm", "POST", "llm/v1/chat/completions", request, &out)
	return out, err
}

// Embeddings is the embeddings of input, a string or a []string.
func (llmService) Embeddings(ctx context.Context, model string, input any) (map[string]any, error) {
	var out map[string]any
	err := ask(ctx, "llm", "POST", "llm/v1/embeddings", map[string]any{"model": model, "input": input}, &out)
	return out, err
}

type dqService struct{}

// DQ is everything about the data quality of a dataset or a data product, as the meta domain
// shows it to the caller (REQ-2133).
var DQ dqService

// Dataset is a dataset's checker tables and profiles; name is its domain.table or its table id.
func (dqService) Dataset(ctx context.Context, name any) (map[string]any, error) {
	var out map[string]any
	err := ask(ctx, "dq", "GET", "dq/dataset/"+q(fmt.Sprint(name)), nil, &out)
	return out, err
}

// Product is a data product's promises, expectations, partner DQ and orphans (REQ-1443).
func (dqService) Product(ctx context.Context, productID string) (map[string]any, error) {
	var out map[string]any
	err := ask(ctx, "dq", "GET", "dq/product/"+q(productID), nil, &out)
	return out, err
}

// Log writes a log line, recorded against the request this call serves.
func Log(ctx context.Context, message, level string, fields map[string]any) error {
	return ask(ctx, "log", "POST", "log", map[string]any{"level": level, "message": message, "fields": orEmpty(fields)}, nil)
}

// Metric writes a metric point (kind is counter, gauge or histogram), named code_source.<name>.
func Metric(ctx context.Context, name string, value float64, kind string, attributes map[string]any) error {
	return ask(ctx, "metric", "POST", "metric", map[string]any{"name": name, "value": value, "kind": kind, "attributes": orEmpty(attributes)}, nil)
}

// Audit writes a structured event to the org's trail, attached to the request this call serves:
// event names it (lower-case letters, digits, _ and .), fields are JSON values, at most 64 KB. A
// secret value does not belong in it, and is redacted if it is there.
func Audit(ctx context.Context, event string, fields map[string]any) error {
	return ask(ctx, "audit", "POST", "audit", map[string]any{"event": event, "fields": orEmpty(fields)}, nil)
}
