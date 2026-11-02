package libv2ray

// Copied into AndroidLibXrayLite by CI (see .github/workflows/build-bandwidth.yml)
// before gomobile builds libv2ray.aar. It mirrors MeasureOutboundDelay: build a
// throw-away core instance from the given outbound config (no inbounds, nothing
// listening), download a file through it using core.Dial, then close the instance.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	v2net "github.com/xtls/xray-core/common/net"
	v2core "github.com/xtls/xray-core/core"
	v2serial "github.com/xtls/xray-core/infra/conf/serial"
)

// MeasureOutboundBandwidth downloads from url through the outbound described by
// ConfigureFileContent. The download stops when maxBytes have been received or
// maxDownloadMs have passed since the first byte, whichever comes first.
//
// gomobile only allows (value, error) returns, so the result is a string:
//
//	"bytes,ttfbMs,downloadMs"
//
// where bytes/downloadMs describe the transfer AFTER the first byte arrived
// (so connection setup does not count against the speed), e.g. "7950000,310,2840".
func MeasureOutboundBandwidth(ConfigureFileContent string, url string, maxBytes int64, maxDownloadMs int64) (string, error) {
	config, err := v2serial.LoadJSONConfig(strings.NewReader(ConfigureFileContent))
	if err != nil {
		return "", err
	}
	// Don't listen to anything for test purposes.
	config.Inbound = nil

	inst, err := v2core.New(config)
	if err != nil {
		return "", err
	}
	if err := inst.Start(); err != nil {
		return "", err
	}
	defer inst.Close()

	if maxBytes <= 0 {
		maxBytes = 8_000_000
	}
	if maxDownloadMs <= 0 {
		maxDownloadMs = 3000
	}
	return measureInstBandwidth(context.Background(), inst, url, maxBytes, time.Duration(maxDownloadMs)*time.Millisecond)
}

func measureInstBandwidth(ctx context.Context, inst *v2core.Instance, url string, maxBytes int64, maxDownload time.Duration) (string, error) {
	if inst == nil {
		return "", errors.New("core instance nil")
	}
	if url == "" {
		return "", errors.New("empty url")
	}
	const connectTimeout = 12 * time.Second // dial + TLS + headers + first byte

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
	// No Client.Timeout: timing is driven by the two timers below, which cancel the request.
	c := &http.Client{Transport: tr}

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Cache-Control", "no-cache")

	connectTimer := time.AfterFunc(connectTimeout, cancel)
	defer connectTimer.Stop()

	start := time.Now()
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	buf := make([]byte, 32*1024)
	var total, firstChunk int64
	var firstByte time.Time
	var dlTimer *time.Timer
	var rerr error
	for total < maxBytes {
		var n int
		n, rerr = resp.Body.Read(buf)
		if n > 0 {
			total += int64(n)
			if firstByte.IsZero() {
				firstByte = time.Now()
				firstChunk = int64(n)
				connectTimer.Stop()
				// Time limit: abort the transfer maxDownload after the first byte.
				dlTimer = time.AfterFunc(maxDownload, cancel)
			}
		}
		if rerr != nil {
			break // EOF, time limit reached (request cancelled), or network error
		}
	}
	end := time.Now()
	if dlTimer != nil {
		dlTimer.Stop()
	}

	if firstByte.IsZero() {
		if rerr != nil {
			return "", rerr
		}
		return "", errors.New("no data received")
	}

	ttfb := firstByte.Sub(start).Milliseconds()
	bytes := total - firstChunk
	downloadMs := end.Sub(firstByte).Milliseconds()
	if bytes <= 0 || downloadMs <= 0 {
		// Tiny transfer: fall back to the whole request so we still report something.
		bytes = total
		downloadMs = end.Sub(start).Milliseconds()
		if downloadMs <= 0 {
			downloadMs = 1
		}
	}
	return fmt.Sprintf("%d,%d,%d", bytes, ttfb, downloadMs), nil
}
