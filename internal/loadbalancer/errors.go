package loadbalancer

import "errors"

var ErrRoutingKeyMissingNoFallback = errors.New("routing key header absent and fallback disabled")
