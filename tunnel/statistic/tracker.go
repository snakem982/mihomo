package statistic

import (
	"io"
	"net"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"

	"github.com/gofrs/uuid/v5"
)

type Tracker interface {
	ID() string
	Close() error
	Info() *TrackerInfo
	C.Connection
}

type rateSample struct {
	timestamp time.Time
	bytes     int64
}

type TrackerInfo struct {
	UUID            uuid.UUID    `json:"id"`
	Metadata        *C.Metadata  `json:"metadata"`
	UploadTotal     atomic.Int64 `json:"upload"`
	DownloadTotal   atomic.Int64 `json:"download"`
	Start           time.Time    `json:"start"`
	Chain           C.Chain      `json:"chains"`
	Rule            string       `json:"rule"`
	RulePayload     string       `json:"rulePayload"`
	MaxUploadRate   atomic.Int64 `json:"maxUploadRate"`
	MaxDownloadRate atomic.Int64 `json:"maxDownloadRate"`
}

type tcpTracker struct {
	C.Conn `json:"-"`
	*TrackerInfo
	manager *Manager

	pushToManager bool `json:"-"`

	uploadRateWindow   []rateSample
	downloadRateWindow []rateSample
}

func (tt *tcpTracker) ID() string {
	return tt.UUID.String()
}

func (tt *tcpTracker) Info() *TrackerInfo {
	return tt.TrackerInfo
}

func (tt *tcpTracker) Read(b []byte) (int, error) {
	n, err := tt.Conn.Read(b)
	download := int64(n)
	if tt.pushToManager {
		tt.manager.PushDownloaded(download)
	}
	tt.DownloadTotal.Add(download)
	updateMaxRate(&tt.downloadRateWindow, download, &tt.TrackerInfo.MaxDownloadRate, 5)
	return n, err
}

func (tt *tcpTracker) ReadBuffer(buffer *buf.Buffer) (err error) {
	err = tt.Conn.ReadBuffer(buffer)
	download := int64(buffer.Len())
	if tt.pushToManager {
		tt.manager.PushDownloaded(download)
	}
	tt.DownloadTotal.Add(download)
	updateMaxRate(&tt.downloadRateWindow, download, &tt.TrackerInfo.MaxDownloadRate, 5)
	return
}

func (tt *tcpTracker) UnwrapReader() (io.Reader, []N.CountFunc) {
	return tt.Conn, []N.CountFunc{func(download int64) {
		if tt.pushToManager {
			tt.manager.PushDownloaded(download)
		}
		tt.DownloadTotal.Add(download)
		updateMaxRate(&tt.downloadRateWindow, download, &tt.TrackerInfo.MaxDownloadRate, 5)
	}}
}

func (tt *tcpTracker) Write(b []byte) (int, error) {
	n, err := tt.Conn.Write(b)
	upload := int64(n)
	if tt.pushToManager {
		tt.manager.PushUploaded(upload)
	}
	tt.UploadTotal.Add(upload)
	updateMaxRate(&tt.uploadRateWindow, upload, &tt.TrackerInfo.MaxUploadRate, 5)
	return n, err
}

func (tt *tcpTracker) WriteBuffer(buffer *buf.Buffer) (err error) {
	upload := int64(buffer.Len())
	err = tt.Conn.WriteBuffer(buffer)
	if tt.pushToManager {
		tt.manager.PushUploaded(upload)
	}
	tt.UploadTotal.Add(upload)
	updateMaxRate(&tt.uploadRateWindow, upload, &tt.TrackerInfo.MaxUploadRate, 5)
	return
}

func (tt *tcpTracker) UnwrapWriter() (io.Writer, []N.CountFunc) {
	return tt.Conn, []N.CountFunc{func(upload int64) {
		if tt.pushToManager {
			tt.manager.PushUploaded(upload)
		}
		tt.UploadTotal.Add(upload)
		updateMaxRate(&tt.uploadRateWindow, upload, &tt.TrackerInfo.MaxUploadRate, 5)
	}}
}

func (tt *tcpTracker) Close() error {
	connErr := tt.Conn.Close()
	tt.manager.Leave(tt)
	return connErr
}

func (tt *tcpTracker) Upstream() any {
	return tt.Conn
}

func NewTCPTracker(conn C.Conn, manager *Manager, metadata *C.Metadata, rule C.Rule, uploadTotal int64, downloadTotal int64, pushToManager bool) *tcpTracker {
	metadata.RemoteDst = conn.RemoteDestination()

	trackerUUID := utils.NewUUIDV4()

	metadata.UUID = trackerUUID.String()

	t := &tcpTracker{
		Conn:    conn,
		manager: manager,
		TrackerInfo: &TrackerInfo{
			UUID:          trackerUUID,
			Start:         time.Now(),
			Metadata:      metadata,
			Chain:         conn.Chains(),
			Rule:          "",
			UploadTotal:   atomic.NewInt64(uploadTotal),
			DownloadTotal: atomic.NewInt64(downloadTotal),
		},
		pushToManager: pushToManager,
	}

	if pushToManager {
		if uploadTotal > 0 {
			manager.PushUploaded(uploadTotal)
		}
		if downloadTotal > 0 {
			manager.PushDownloaded(downloadTotal)
		}
	}

	if rule != nil {
		t.TrackerInfo.Rule = rule.RuleType().String()
		t.TrackerInfo.RulePayload = rule.Payload()
	}

	manager.Join(t)
	return t
}

type udpTracker struct {
	C.PacketConn `json:"-"`
	*TrackerInfo
	manager *Manager

	pushToManager bool `json:"-"`

	uploadRateWindow   []rateSample
	downloadRateWindow []rateSample
}

func (ut *udpTracker) ID() string {
	return ut.UUID.String()
}

func (ut *udpTracker) Info() *TrackerInfo {
	return ut.TrackerInfo
}

func (ut *udpTracker) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := ut.PacketConn.ReadFrom(b)
	download := int64(n)
	if ut.pushToManager {
		ut.manager.PushDownloaded(download)
	}
	ut.DownloadTotal.Add(download)
	updateMaxRate(&ut.downloadRateWindow, download, &ut.TrackerInfo.MaxDownloadRate, 5)
	return n, addr, err
}

func (ut *udpTracker) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	data, put, addr, err = ut.PacketConn.WaitReadFrom()
	download := int64(len(data))
	if ut.pushToManager {
		ut.manager.PushDownloaded(download)
	}
	ut.DownloadTotal.Add(download)
	updateMaxRate(&ut.downloadRateWindow, download, &ut.TrackerInfo.MaxDownloadRate, 5)
	return
}

func (ut *udpTracker) WriteTo(b []byte, addr net.Addr) (int, error) {
	n, err := ut.PacketConn.WriteTo(b, addr)
	upload := int64(n)
	if ut.pushToManager {
		ut.manager.PushUploaded(upload)
	}
	ut.UploadTotal.Add(upload)
	updateMaxRate(&ut.uploadRateWindow, upload, &ut.TrackerInfo.MaxUploadRate, 5)
	return n, err
}

func (ut *udpTracker) Close() error {
	connErr := ut.PacketConn.Close()
	ut.manager.Leave(ut)
	return connErr
}

func (ut *udpTracker) Upstream() any {
	return ut.PacketConn
}

func NewUDPTracker(conn C.PacketConn, manager *Manager, metadata *C.Metadata, rule C.Rule, uploadTotal int64, downloadTotal int64, pushToManager bool) *udpTracker {
	metadata.RemoteDst = conn.RemoteDestination()

	trackerUUID := utils.NewUUIDV4()

	metadata.UUID = trackerUUID.String()

	ut := &udpTracker{
		PacketConn: conn,
		manager:    manager,
		TrackerInfo: &TrackerInfo{
			UUID:          trackerUUID,
			Start:         time.Now(),
			Metadata:      metadata,
			Chain:         conn.Chains(),
			Rule:          "",
			UploadTotal:   atomic.NewInt64(uploadTotal),
			DownloadTotal: atomic.NewInt64(downloadTotal),
		},
		pushToManager: pushToManager,
	}

	if pushToManager {
		if uploadTotal > 0 {
			manager.PushUploaded(uploadTotal)
		}
		if downloadTotal > 0 {
			manager.PushDownloaded(downloadTotal)
		}
	}

	if rule != nil {
		ut.TrackerInfo.Rule = rule.RuleType().String()
		ut.TrackerInfo.RulePayload = rule.Payload()
	}

	manager.Join(ut)
	return ut
}

func updateMaxRate(rateWindow *[]rateSample, current int64, maxRate *atomic.Int64, windowSec int) {
	now := time.Now()
	*rateWindow = append(*rateWindow, rateSample{timestamp: now, bytes: current})

	windowStart := now.Add(-time.Duration(windowSec) * time.Second)
	i := 0
	for ; i < len(*rateWindow); i++ {
		if (*rateWindow)[i].timestamp.After(windowStart) {
			break
		}
	}
	*rateWindow = (*rateWindow)[i:]

	var totalBytes int64
	for _, sample := range *rateWindow {
		totalBytes += sample.bytes
	}
	avgRate := int64(float64(totalBytes) / float64(windowSec))
	if avgRate > maxRate.Load() {
		maxRate.Store(avgRate)
	}
}
