package vllm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"llm-inference-platform/internal/inference"
)

// The error translation table (Phase 6a, docs/servicio/13-fase-6a-decisiones.md,
// decision 6). The rule: only what is the worker's fault is
// inference.ErrWorkerFailure, because that is what opens its circuit breaker
// and allows a retry on another worker (Phase 4). A request vLLM rejects is
// the client's problem and must not isolate a healthy worker.

// engineError extracts the message of a vLLM error body, in its newer
// {"error": {"message": ...}} or older {"object": "error", "message": ...}
// form.
func engineError(body []byte) string {
	var v struct {
		Message string `json:"message"`
		Error   *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &v) == nil {
		if v.Error != nil && v.Error.Message != "" {
			return v.Error.Message
		}
		if v.Message != "" {
			return v.Message
		}
	}
	msg := strings.TrimSpace(string(body))
	if len(msg) > 300 {
		msg = msg[:300] + "…"
	}
	return msg
}

// isContextLengthError recognises vLLM's "too many tokens" rejections. It is
// a heuristic on the message text, kept in one place and to be checked
// against the vLLM version fixed in Phase 6b.
func isContextLengthError(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "maximum context length") ||
		strings.Contains(m, "max_model_len") ||
		strings.Contains(m, "context length") ||
		strings.Contains(m, "too long")
}

// statusError translates a non-200 answer of the engine.
func statusError(worker string, status int, body []byte) error {
	msg := engineError(body)
	switch {
	case status == http.StatusBadRequest && isContextLengthError(msg):
		return fmt.Errorf("%s: %w: %s", worker, inference.ErrRequestTooLarge, msg)
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		return fmt.Errorf("%s: %w: %s", worker, inference.ErrInvalidRequest, msg)
	default:
		// 5xx, but also 404 (does not serve the model it announced), 401/403
		// (API key misconfigured), 429: none of them is the client's fault.
		return fmt.Errorf("%s: %w: HTTP %d: %s", worker, inference.ErrWorkerFailure, status, msg)
	}
}

// transportError wraps a network failure (refused, reset, DNS, timeout).
func transportError(worker string, err error) error {
	return fmt.Errorf("%s: %w: %v", worker, inference.ErrWorkerFailure, err)
}
