// Copyright 2020 Matthew Holt
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package layer4

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type serverMetrics struct {
	udpPacketsDropped prometheus.Counter
}

func newServerMetrics(registry *prometheus.Registry) (*serverMetrics, error) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "caddy",
		Subsystem: "layer4",
		Name:      "udp_packets_dropped_total",
		Help:      "Number of UDP datagrams dropped because a virtual connection's bounded queue was full.",
	})
	if err := registry.Register(counter); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if !errors.As(err, &alreadyRegistered) {
			return nil, fmt.Errorf("registering UDP drop counter: %w", err)
		}
		existing, ok := alreadyRegistered.ExistingCollector.(prometheus.Counter)
		if !ok {
			return nil, fmt.Errorf("UDP drop metric is already registered as %T, want prometheus.Counter", alreadyRegistered.ExistingCollector)
		}
		metric := new(dto.Metric)
		if err := existing.Write(metric); err != nil {
			return nil, fmt.Errorf("reading existing UDP drop metric: %w", err)
		}
		if metric.Counter == nil {
			return nil, fmt.Errorf("UDP drop metric is already registered as %T, want counter metric type", alreadyRegistered.ExistingCollector)
		}
		counter = existing
	}
	return &serverMetrics{udpPacketsDropped: counter}, nil
}

func (m *serverMetrics) packetDropped() {
	if m != nil {
		m.udpPacketsDropped.Inc()
	}
}
