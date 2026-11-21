package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// Request fields fall into three groups (Phase 6a,
// docs/servicio/13-fase-6a-decisiones.md, decision 1 and conflict C1):
//
//   - controlled: the platform interprets them and sets them itself in the
//     request to the engine (they are the ledger reservation, the shape of
//     the response and the usage);
//   - rejected: they would ask for output the platform cannot reserve for
//     or relay (several choices, logprobs, tool calls), so the client gets a
//     clear 400 instead of a response without what it asked for;
//   - everything else is forwarded to the engine untouched
//     (inference.InferenceRequest.Params).
//
// This file is the single place to review when the scope grows.

var controlledFields = map[string]bool{
	"model": true, "messages": true, "max_tokens": true, "max_completion_tokens": true,
	"stream": true, "stream_options": true,
}

// rejectedField says why a field present in the request cannot be honoured,
// or "" if its value is acceptable.
func rejectedField(name string, value json.RawMessage) string {
	isNull := bytes.Equal(bytes.TrimSpace(value), []byte("null"))
	switch name {
	case "n", "best_of":
		var n int
		if !isNull && (json.Unmarshal(value, &n) != nil || n != 1) {
			return "only one choice per request is supported"
		}
	case "logprobs":
		var b bool
		if !isNull && (json.Unmarshal(value, &b) != nil || b) {
			return "logprobs are not supported"
		}
	case "top_logprobs", "prompt_logprobs":
		if !isNull {
			return "logprobs are not supported"
		}
	case "tools", "tool_choice", "functions", "function_call":
		if !isNull {
			return "tool and function calls are not supported"
		}
	}
	return ""
}

// forwardedParams splits the raw request into the parameters forwarded to
// the engine, or returns an error naming every rejected field.
func forwardedParams(raw map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	var params map[string]json.RawMessage
	var problems []string
	for name, value := range raw {
		if controlledFields[name] {
			continue
		}
		if why := rejectedField(name, value); why != "" {
			problems = append(problems, fmt.Sprintf("%s (%s)", name, why))
			continue
		}
		if params == nil {
			params = map[string]json.RawMessage{}
		}
		params[name] = value
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return nil, fmt.Errorf("unsupported parameters: %v", problems)
	}
	return params, nil
}

// errContentParts explains why a message with content parts is rejected.
var errContentParts = errors.New("message content must be a string: content parts (images, audio, multimodal) are not supported")

// UnmarshalJSON accepts content only as a string, with a clear error for the
// OpenAI content-parts form instead of a generic type mismatch.
func (m *chatMessage) UnmarshalJSON(b []byte) error {
	var wire struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
		Name    string          `json:"name"`
	}
	if err := json.Unmarshal(b, &wire); err != nil {
		return err
	}
	m.Role, m.Name = wire.Role, wire.Name
	content := bytes.TrimSpace(wire.Content)
	switch {
	case len(content) == 0 || bytes.Equal(content, []byte("null")):
		return errors.New("message content is required")
	case content[0] == '[':
		return errContentParts
	}
	return json.Unmarshal(content, &m.Content)
}
