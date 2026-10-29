package prague

import (
	"fmt"
	"os"
	"strings"
)

// ResolveTopic expands a Prague topic template the way Prague (.NET) does:
// `[v:name]` from vars, `[e:NAME]` from the environment. A placeholder with no
// value is an error, since a half-resolved name silently consumes the wrong
// topic.
func ResolveTopic(template string, vars map[string]string) (string, error) {
	var b strings.Builder
	rest := template
	for {
		i := strings.Index(rest, "[")
		if i < 0 {
			b.WriteString(rest)
			return b.String(), nil
		}
		j := strings.Index(rest[i:], "]")
		if j < 0 {
			return "", fmt.Errorf("prague: topic %q: unclosed placeholder", template)
		}
		b.WriteString(rest[:i])
		ph := rest[i+1 : i+j]
		kind, name, ok := strings.Cut(ph, ":")
		if !ok {
			return "", fmt.Errorf("prague: topic %q: placeholder [%s] is not [v:name] or [e:NAME]", template, ph)
		}
		var val string
		var found bool
		switch kind {
		case "v":
			val, found = vars[name]
		case "e":
			val, found = os.LookupEnv(name)
		default:
			return "", fmt.Errorf("prague: topic %q: unknown placeholder kind %q", template, kind)
		}
		if !found {
			return "", fmt.Errorf("prague: topic %q: no value for [%s]", template, ph)
		}
		b.WriteString(val)
		rest = rest[i+j+1:]
	}
}
