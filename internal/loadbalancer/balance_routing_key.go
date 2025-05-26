package loadbalancer

import (
	"net/http"
	"strings"

	"github.com/substratusai/kubeai/internal/apiutils"
)

// getAddrRoutingKey selects an endpoint using consistent hashing based on the provided 'key'.
// It incorporates CHWBL logic using 'meanLoadFactor'.
// If 'key' is empty:
// - If 'fallbackToLeastLoad' is true, it delegates to getAddrLeastLoad.
// - If 'fallbackToLeastLoad' is false, it returns (endpoint{}, false)
//   which signals that no suitable endpoint was found.
func (g *group) getAddrRoutingKey(key string, meanLoadFactor float64, fallbackToLeastLoad bool, adapter string) (endpoint, bool) {
	if key == "" {
		if fallbackToLeastLoad {
			return g.getAddrLeastLoad(adapter)
		}
		// No key and no fallback, so no endpoint can be chosen based on routing key.
		return endpoint{}, false
	}
	// If a key is provided, use CHWBL logic (similar to PrefixHash)
	return g.chwblGetAddr(adapter+key, meanLoadFactor, adapter)
}

// extractRoutingKeyHeader extracts the Routing-Key header value from an HTTP request.
// It performs a case-insensitive search for the header.
// Returns an empty string if the header is not found.
func extractRoutingKeyHeader(req *http.Request) string {
	if req == nil {
		return ""
	}
	// HTTP headers are case-insensitive.
	// Iterate and find the first match for "Routing-Key".
	for name, values := range req.Header {
		if strings.EqualFold(name, "Routing-Key") {
			if len(values) > 0 {
				return values[0]
			}
			return "" // Header present but no value
		}
	}
	return "" // Header not found
}
