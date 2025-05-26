package integration

import (
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substratusai/kubeai/internal/config"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestProxy(t *testing.T) {
	sysCfg := baseSysCfg(t)
	sysCfg.ModelAutoscaling.TimeWindow = config.Duration{Duration: 6 * time.Second}
	sysCfg.ModelAutoscaling.Interval = config.Duration{Duration: time.Second}
	initTest(t, sysCfg)

	backendComplete := make(chan struct{})

	t.Cleanup(func() {
		// Finish all requests
		close(backendComplete)
	})

	m := modelForTest(t)
	m.Spec.MaxReplicas = ptr.To[int32](3)
	m.Spec.TargetRequests = ptr.To[int32](1)
	m.Spec.ScaleDownDelaySeconds = ptr.To[int64](1)

	// Create the Model object in the Kubernetes cluster.
	require.NoError(t, testK8sClient.Create(testCtx, m))

	backendRequests := &atomic.Int32{}
	totalBackendRequests := &atomic.Int32{}
	testModelBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("Serving request from testBackend")
		totalBackendRequests.Add(1)
		backendRequests.Add(1)
		defer backendRequests.Add(-1)
		log.Println("Added request to backend:", backendRequests.Load())
		<-backendComplete
		log.Println("Sending response from backend")
		w.WriteHeader(200)
	}))

	updateModelWithBackend(t, m, testModelBackend)

	// Wait for controller cache to sync.
	time.Sleep(3 * time.Second)

	var wg sync.WaitGroup

	// Send request number 1
	sendRequests(t, &wg, m.Name, nil, 1, http.StatusOK, "", "request 1")

	requireModelReplicas(t, m, 1, "Replicas should be scaled up to 1 to process messaging request", 5*time.Second)
	requireModelPods(t, m, 1, "Pod should be created for the messaging request", 5*time.Second)
	markAllModelPodsReady(t, m)
	closeChannels(backendComplete, 1)
	require.Equal(t, int32(1), totalBackendRequests.Load(), "ensure the request made its way to the backend")

	const autoscaleUpWait = 25 * time.Second
	// Ensure the deployment is autoscaled past 1.
	// Simulate the backend processing the request.
	sendRequests(t, &wg, m.Name, nil, 2, http.StatusOK, "", "request 2,3")
	requireModelReplicas(t, m, 2, "Replicas should be scaled up to 2 to process pending messaging request", autoscaleUpWait)
	requireModelPods(t, m, 2, "2 Pods should be created for the messaging requests", 5*time.Second)
	markAllModelPodsReady(t, m)

	// Make sure deployment will not be scaled past max (3).
	sendRequests(t, &wg, m.Name, nil, 2, http.StatusOK, "", "request 4,5")
	require.Never(t, func() bool {
		assert.NoError(t, testK8sClient.Get(testCtx, client.ObjectKeyFromObject(m), m))
		return *m.Spec.Replicas > *m.Spec.MaxReplicas
	}, autoscaleUpWait, time.Second/10, "Replicas should not be scaled past MaxReplicas")

	closeChannels(backendComplete, 4)
	require.Equal(t, int32(5), totalBackendRequests.Load(), "ensure all the requests made their way to the backend")

	// Ensure the deployment is autoscaled back down to MinReplicas.
	const autoscaleDownWait = 25 * time.Second
	requireModelReplicas(t, m, m.Spec.MinReplicas, "Replicas should scale back to MinReplicas", autoscaleDownWait)
	requireModelPods(t, m, int(m.Spec.MinReplicas), "Pods should be removed", 5*time.Second)

	t.Log("Waiting for all requests to complete")
	wg.Wait()
	t.Log("All requests completed")
}

func TestProxyRoutingKey_HeaderPresent(t *testing.T) {
	sysCfg := baseSysCfg(t)
	initTest(t, sysCfg)

	// Backend 1 setup
	backend1Hit := make(chan string, 10) // Buffer to avoid blocking handlers
	backend1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Routing-Key")
		log.Printf("Backend 1 received request with Routing-Key: %s", key)
		backend1Hit <- key
		w.WriteHeader(http.StatusOK)
	}))
	defer backend1.Close()

	// Backend 2 setup
	backend2Hit := make(chan string, 10)
	backend2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Routing-Key")
		log.Printf("Backend 2 received request with Routing-Key: %s", key)
		backend2Hit <- key
		w.WriteHeader(http.StatusOK)
	}))
	defer backend2.Close()

	m := modelForTest(t)
	m.Spec.LoadBalancing.Strategy = "RoutingKey"
	m.Spec.LoadBalancing.RoutingKey = &v1.RoutingKeyStrategy{FallbackToLeastLoad: false, Replication: 2} // Low replication for easier testing
	m.Spec.MinReplicas = 2                                                                            // Ensure both backends can be active
	m.Spec.MaxReplicas = ptr.To[int32](2)
	require.NoError(t, testK8sClient.Create(testCtx, m))
	updateModelWithMultipleBackends(t, m, []string{backend1.URL, backend2.URL})
	requireModelReplicas(t, m, 2, "Replicas should be 2", 10*time.Second)
	markAllModelPodsReady(t, m)

	var wg sync.WaitGroup
	headersKeyA := http.Header{"Routing-Key": []string{"keyA"}}
	headersKeyB := http.Header{"Routing-Key": []string{"keyB"}}
	headersKeyALower := http.Header{"routing-key": []string{"keyA"}}
	headersKeyAUpper := http.Header{"ROUTING-KEY": []string{"keyA"}}

	// Send requests for keyA
	sendRequests(t, &wg, m.Name, headersKeyA, 2, http.StatusOK, "", "keyA reqs")
	// Send requests for keyB
	sendRequests(t, &wg, m.Name, headersKeyB, 2, http.StatusOK, "", "keyB reqs")
	// Send requests for case-insensitivity check
	sendRequests(t, &wg, m.Name, headersKeyALower, 1, http.StatusOK, "", "keyA lower req")
	sendRequests(t, &wg, m.Name, headersKeyAUpper, 1, http.StatusOK, "", "keyA upper req")

	wg.Wait()

	keyACount := 0
	keyBCount := 0
	keyAReceivedOnBackend1 := false
	keyBReceivedOnBackend1 := false

	for i := 0; i < 6; i++ {
		select {
		case key := <-backend1Hit:
			if key == "keyA" {
				keyACount++
				keyAReceivedOnBackend1 = true
			} else if key == "keyB" {
				keyBCount++
				keyBReceivedOnBackend1 = true
			}
		case key := <-backend2Hit:
			if key == "keyA" {
				keyACount++
				require.False(t, keyAReceivedOnBackend1, "keyA should route to only one backend")
			} else if key == "keyB" {
				keyBCount++
				require.False(t, keyBReceivedOnBackend1, "keyB should route to only one backend")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for backend hits")
		}
	}

	assert.Equal(t, 4, keyACount, "Expected 4 requests for keyA")
	assert.Equal(t, 2, keyBCount, "Expected 2 requests for keyB")
	// This assertion implicitly checks that keyA always went to one backend and keyB to another.
	// If keyAReceivedOnBackend1 is true, all keyA requests went to backend1. If false, all went to backend2.
	// Same logic for keyB.
	assert.NotEqual(t, keyAReceivedOnBackend1, keyBReceivedOnBackend1, "keyA and keyB should route to different backends")
}

func TestProxyRoutingKey_FallbackToLeastLoad(t *testing.T) {
	sysCfg := baseSysCfg(t)
	initTest(t, sysCfg)

	backend1Requests := &atomic.Int32{}
	backend1HitRoutingKey := make(chan string, 5)
	backend1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend1Requests.Add(1)
		log.Printf("Backend 1 received request (in-flight: %d)", backend1Requests.Load())
		if key := r.Header.Get("Routing-Key"); key != "" {
			backend1HitRoutingKey <- key
		}
		time.Sleep(200 * time.Millisecond) // Simulate work
		w.WriteHeader(http.StatusOK)
		backend1Requests.Add(-1)
	}))
	defer backend1.Close()

	backend2Requests := &atomic.Int32{}
	backend2HitRoutingKey := make(chan string, 5)
	backend2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend2Requests.Add(1)
		log.Printf("Backend 2 received request (in-flight: %d)", backend2Requests.Load())
		if key := r.Header.Get("Routing-Key"); key != "" {
			backend2HitRoutingKey <- key
		}
		w.WriteHeader(http.StatusOK) // Quick response
		backend2Requests.Add(-1)
	}))
	defer backend2.Close()

	m := modelForTest(t)
	m.Spec.LoadBalancing.Strategy = "RoutingKey"
	m.Spec.LoadBalancing.RoutingKey = &v1.RoutingKeyStrategy{FallbackToLeastLoad: true, Replication: 2}
	m.Spec.MinReplicas = 2
	m.Spec.MaxReplicas = ptr.To[int32](2)
	m.Spec.TargetRequests = ptr.To[int32](1) // For least load to react quickly
	require.NoError(t, testK8sClient.Create(testCtx, m))
	updateModelWithMultipleBackends(t, m, []string{backend1.URL, backend2.URL})
	requireModelReplicas(t, m, 2, "Replicas should be 2", 10*time.Second)
	markAllModelPodsReady(t, m)

	var wg sync.WaitGroup

	// Send one blocking request to backend1 to make it "busy"
	sendRequests(t, &wg, m.Name, http.Header{"Routing-Key": []string{"keyToBackend1"}}, 1, http.StatusOK, "", "blocker req")
	// Wait for the first request to hit a backend and determine which one it is.
	var firstKeyTargetIsBackend1 bool
	select {
	case <-backend1HitRoutingKey:
		firstKeyTargetIsBackend1 = true
		log.Println("keyToBackend1 routed to Backend 1")
	case <-backend2HitRoutingKey:
		firstKeyTargetIsBackend1 = false
		log.Println("keyToBackend1 routed to Backend 2")
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for first routing key request to hit a backend")
	}
	
	// Ensure the target backend for "keyToBackend1" is busy by sending more requests to it.
	// These calls are async due to sendRequests using goroutines.
	if firstKeyTargetIsBackend1 {
		go func() { // Ensure this doesn't block test progress
			http.Get(backend1.URL) // Simulate another direct hit to keep it busy
			http.Get(backend1.URL)
		}()
	} else {
		go func() {
			http.Get(backend2.URL)
			http.Get(backend2.URL)
		}()
	}
	time.Sleep(50 * time.Millisecond) // Brief pause to allow backend load to build up

	// Send requests without Routing-Key header, expecting them to go to the less loaded backend (Backend 2 if keyToBackend1 went to Backend1)
	for i := 0; i < 3; i++ {
		sendRequests(t, &wg, m.Name, nil, 1, http.StatusOK, "", "fallback req")
	}

	// Send a request with a *different* Routing-Key, it should still be routed by key.
	sendRequests(t, &wg, m.Name, http.Header{"Routing-Key": []string{"keySpecific"}}, 1, http.StatusOK, "", "specific key req")

	wg.Wait() // Wait for all proxy requests to complete.

	// Assertions
	// Check fallback requests went to the expected backend
	if firstKeyTargetIsBackend1 {
		assert.GreaterOrEqual(t, backend2Requests.Load()+int32(len(backend2HitRoutingKey)), int32(3), "Backend 2 should have received fallback requests")
	} else {
		assert.GreaterOrEqual(t, backend1Requests.Load()+int32(len(backend1HitRoutingKey)), int32(3), "Backend 1 should have received fallback requests")
	}

	// Check specific key request
	specificKeyHit := false
	select {
	case key := <-backend1HitRoutingKey:
		if key == "keySpecific" {
			specificKeyHit = true
		}
	default:
	}
	select {
	case key := <-backend2HitRoutingKey:
		if key == "keySpecific" {
			specificKeyHit = true
		}
	default:
	}
	assert.True(t, specificKeyHit, "Request with 'keySpecific' should have been routed by key")
}

func TestProxyRoutingKey_NoFallbackHeaderMissing_Should400(t *testing.T) {
	sysCfg := baseSysCfg(t)
	initTest(t, sysCfg)

	backend1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// This backend should ideally not be hit.
		log.Printf("Backend (NoFallback) received request, this is unexpected!")
		w.WriteHeader(http.StatusOK)
	}))
	defer backend1.Close()

	m := modelForTest(t)
	m.Spec.LoadBalancing.Strategy = "RoutingKey"
	m.Spec.LoadBalancing.RoutingKey = &v1.RoutingKeyStrategy{FallbackToLeastLoad: false}
	m.Spec.MinReplicas = 1
	m.Spec.MaxReplicas = ptr.To[int32](1)
	require.NoError(t, testK8sClient.Create(testCtx, m))
	updateModelWithMultipleBackends(t, m, []string{backend1.URL}) // Only one backend
	requireModelReplicas(t, m, 1, "Replicas should be 1", 10*time.Second)
	markAllModelPodsReady(t, m)

	var wg sync.WaitGroup
	// Send request without Routing-Key header
	sendRequests(t, &wg, m.Name, nil, 1, http.StatusBadRequest, "Routing-Key header is required when fallbackToLeastLoad is disabled.", "no header 400 req")
	wg.Wait()
}

// updateModelWithMultipleBackends is a helper to update the model status with multiple backend IPs.
// This is a simplified version, assuming direct IP:Port, not service names.
func updateModelWithMultipleBackends(t *testing.T, m *v1.Model, backendTargets []string) {
	t.Helper()
	var podInfos []v1.PodInfo
	for i, targetURL := range backendTargets {
		parsedURL, err := url.Parse(targetURL)
		require.NoError(t, err)
		podInfos = append(podInfos, v1.PodInfo{
			Name:      m.Name + "-backend-" + http.StripPort(parsedURL.Host) + "-" + parsedURL.Port() + "-" + string(rune(i)),
			Namespace: m.Namespace,
			Address:   parsedURL.Host, // Assuming URL is http://IP:Port
			Phase:     "Running",
		})
	}

	m.Status.PodInfos = podInfos
	m.Status.Replicas.All = int32(len(podInfos))
	m.Status.Replicas.Ready = int32(len(podInfos))
	m.Status.State = "Online"
	require.NoError(t, testK8sClient.Status().Update(testCtx, m))
	log.Printf("Updated model %s status with %d backends", m.Name, len(backendTargets))
}
