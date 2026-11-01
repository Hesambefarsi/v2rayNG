package libv2ray

// Add this file to the root of 2dust/AndroidLibXrayLite (next to the file that
// defines MeasureOutboundDelay), then rebuild the AAR:
//
//   gomobile bind -v -androidapi 24 -trimpath -ldflags='-s -w -buildid= -checklinkname=0' ./
//
// It mirrors MeasureOutboundDelay: build a throw-away core instance from the
// given outbound config (no inbounds, nothing listening), download a file
// through it using core.Dial, then close the instance. No VPN connection and
// no local proxy port are needed.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	v2net "github.com/xtls/xray-core/common/net"
	v2core "github.com/xtls/xray-core/core"
	v2serial "github.com/xtls/xray-core/infra/conf/serial"
)

// MeasureOutboundBandwidth downloads up to maxBytes from url through the
// outbound described by ConfigureFileContent.
//
// gomobile only allows (value, error) returns, so the result is a compact
// string: "bytes,ttfbMs,durationMs"  e.g. "5000000,310,2840".
func MeasureOutboundBandwidth(ConfigureFileContent string, url string, maxBytes int64, timeoutSec int64) (string, error) {
	config, err := v2serial.LoadJSONConfig(strings.NewReader(ConfigureFileContent))
	if err != nil {
		return "", err
	}
	// Don't listen to anything for test purposes.
	config.Inbound = nil
	// NOTE: if your MeasureOutboundDelay also trims config.App / config.Transport
	// to keep only OutboundConfig, Dispatcher and Log, copy those lines here too.
	// It works without them, the instance is just a bit heavier.

	inst, err := v2core.New(config)
	if err != nil {
		return "", err
	}
	if err := inst.Start(); err != nil {
		return "", err
	}
	defer inst.Close()

	if timeoutSec <= 0 {
		timeoutSec = 30
	}
	if maxBytes <= 0 {
		maxBytes = 5_000_000
	}
	return measureInstBandwidth(context.Background(), inst, url, maxBytes, time.Duration(timeoutSec)*time.Second)
}

func measureInstBandwidth(ctx context.Context, inst *v2core.Instance, url string, maxBytes int64, timeout time.Duration) (string, error) {
	if inst == nil {
		return "", errors.New("core instance nil")
	}
	if url == "" {
		url = "https://speed.cloudflare.com/__down?bytes=5000000"
	}

	tr := &http.Transport{
		TLSHandshakeTimeout: 6 * time.Second,
		DisableKeepAlives:   true,
		DialContext: func(dctx context.Context, network, addr string) (net.Conn, error) {
			dest, err := v2net.ParseDestination(fmt.Sprintf("%s:%s", network, addr))
			if err != nil {
				return nil, err
			}
			return v2core.Dial(dctx, inst, dest)
		},
	}
	// Client.Timeout covers connect + headers + body, so a stalled download aborts.
	c := &http.Client{Transport: tr, Timeout: timeout}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Cache-Control", "no-cache")

	start := time.Now()
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	buf := make([]byte, 64*1024)
	var total int64
	var firstByte time.Time
	for total < maxBytes {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if firstByte.IsZero() {
				firstByte = time.Now()
			}
			total += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			if total == 0 {
				return "", rerr
			}
			break // partial download still gives a usable speed figure
		}
	}
	end := time.Now()

	if total == 0 || firstByte.IsZero() {
		return "", errors.New("no data received")
	}
	ttfb := firstByte.Sub(start).Milliseconds()
	dur := end.Sub(start).Milliseconds()
	return fmt.Sprintf("%d,%d,%d", total, ttfb, dur), nil
}
