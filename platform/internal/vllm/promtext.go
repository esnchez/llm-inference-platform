package vllm

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// sample is one line of the Prometheus text format.
type sample struct {
	name   string
	labels map[string]string
	value  float64
}

// parseMetrics reads the Prometheus text exposition format, enough for the
// handful of vLLM metrics the adapter uses. It skips comments and lines it
// cannot parse instead of failing: /metrics is best-effort data.
// (prometheus/common/expfmt would do, but its parser API changes between
// versions over name validation; this is 40 lines.)
func parseMetrics(r io.Reader) []sample {
	var out []sample
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		s, ok := parseLine(line)
		if ok {
			out = append(out, s)
		}
	}
	return out
}

func parseLine(line string) (sample, bool) {
	s := sample{labels: map[string]string{}}
	rest := line
	if i := strings.IndexAny(line, "{ "); i < 0 {
		return s, false
	} else {
		s.name = line[:i]
		rest = line[i:]
	}
	if strings.HasPrefix(rest, "{") {
		end := -1
		inQuote := false
		for i := 1; i < len(rest); i++ {
			switch c := rest[i]; {
			case c == '\\' && inQuote:
				i++
			case c == '"':
				inQuote = !inQuote
			case c == '}' && !inQuote:
				end = i
			}
			if end >= 0 {
				break
			}
		}
		if end < 0 {
			return s, false
		}
		parseLabels(rest[1:end], s.labels)
		rest = rest[end+1:]
	}
	fields := strings.Fields(rest) // value [timestamp]
	if len(fields) == 0 {
		return s, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return s, false
	}
	s.value = v
	return s, true
}

func parseLabels(s string, into map[string]string) {
	for len(s) > 0 {
		eq := strings.IndexByte(s, '=')
		if eq < 0 || eq+1 >= len(s) || s[eq+1] != '"' {
			return
		}
		key := strings.TrimSpace(strings.TrimPrefix(s[:eq], ","))
		var val strings.Builder
		i := eq + 2
		for ; i < len(s) && s[i] != '"'; i++ {
			if s[i] == '\\' && i+1 < len(s) {
				i++
				switch s[i] {
				case 'n':
					val.WriteByte('\n')
				default:
					val.WriteByte(s[i])
				}
				continue
			}
			val.WriteByte(s[i])
		}
		into[strings.TrimSpace(key)] = val.String()
		if i+1 >= len(s) {
			return
		}
		s = strings.TrimPrefix(s[i+1:], ",")
	}
}

// metricSet indexes samples by name for lookups.
type metricSet map[string][]sample

func indexMetrics(samples []sample) metricSet {
	m := metricSet{}
	for _, s := range samples {
		m[s.name] = append(m[s.name], s)
	}
	return m
}

// first returns the first sample found among names (metric names change
// between vLLM versions, FI-4; counters may or may not carry _total).
func (m metricSet) first(names ...string) (sample, bool) {
	for _, n := range names {
		if ss := m[n]; len(ss) > 0 {
			return ss[0], true
		}
	}
	return sample{}, false
}

// sum adds every sample of the first name present (e.g. a counter split by
// label).
func (m metricSet) sum(names ...string) (float64, bool) {
	for _, n := range names {
		if ss := m[n]; len(ss) > 0 {
			total := 0.0
			for _, s := range ss {
				total += s.value
			}
			return total, true
		}
	}
	return 0, false
}
