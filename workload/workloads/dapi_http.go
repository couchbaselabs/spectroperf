package workloads

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/couchbaselabs/spectroperf/configuration"
	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func newDapiHTTPClient(config *configuration.Config) *http.Client {
	tr := otelhttp.NewTransport(
		&http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   time.Duration(config.DialTimeout) * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxConnsPerHost:       500,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       time.Duration(config.IdleConnTimeout) * time.Second,
			ResponseHeaderTimeout: time.Duration(config.ResponseHeaderTimeout) * time.Second,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: config.TlsSkipVerify,
			},
		},
		otelhttp.WithClientTrace(func(ctx context.Context) *httptrace.ClientTrace {
			return otelhttptrace.NewClientTrace(ctx)
		}),
	)

	return &http.Client{
		Transport: tr,
		Timeout:   time.Duration(config.RequestTimeout) * time.Second,
	}
}
