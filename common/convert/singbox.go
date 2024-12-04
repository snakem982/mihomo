package convert

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ConvertsSingBox convert SingBox subscribe proxies data to mihomo proxies config
func ConvertsSingBox(buf []byte) ([]map[string]any, error) {
	proxies := make([]map[string]any, 0)
	names := make(map[string]int, 200)

	config := SingConfig{}
	err := json.Unmarshal(buf, &config)
	if err != nil {
		return nil, err
	}

	for _, outbound := range config.Outbounds {
		scheme := outbound.Type
		switch scheme {
		case "hysteria":
			name := uniqueName(names, outbound.Tag)
			hysteria := make(map[string]any, 20)

			hysteria["name"] = name
			hysteria["type"] = scheme
			hysteria["server"] = outbound.Server
			hysteria["port"] = outbound.ServerPort

			if outbound.TLS != nil {
				hysteria["sni"] = outbound.TLS.ServerName
				hysteria["alpn"] = outbound.TLS.Alpn
			}

			if outbound.Obfs != nil {
				hysteria["obfs"] = outbound.Obfs.Value
			}

			hysteria["auth_str"] = outbound.AuthStr

			up := outbound.Up
			down := outbound.Down
			if up == "" {
				up = strconv.Itoa(outbound.UpMbps)
			}
			if down == "" {
				down = strconv.Itoa(outbound.DownMbps)
			}
			hysteria["down"] = down
			hysteria["up"] = up
			hysteria["skip-cert-verify"] = true

			proxies = append(proxies, hysteria)

		case "hysteria2":
			name := uniqueName(names, outbound.Tag)
			hysteria2 := make(map[string]any, 20)

			hysteria2["name"] = name
			hysteria2["type"] = scheme
			hysteria2["server"] = outbound.Server
			hysteria2["port"] = outbound.ServerPort

			if outbound.TLS != nil {
				hysteria2["sni"] = outbound.TLS.ServerName
				hysteria2["alpn"] = outbound.TLS.Alpn
			}

			if outbound.Obfs != nil {
				hysteria2["obfs"] = outbound.Obfs.Type
				hysteria2["obfs-password"] = outbound.Obfs.Value
			}

			hysteria2["skip-cert-verify"] = true
			hysteria2["password"] = outbound.Password

			hysteria2["down"] = outbound.DownMbps
			hysteria2["up"] = outbound.UpMbps

			proxies = append(proxies, hysteria2)

		case "tuic":
			name := uniqueName(names, outbound.Tag)
			tuic := make(map[string]any, 20)

			tuic["name"] = name
			tuic["type"] = scheme
			tuic["server"] = outbound.Server
			tuic["port"] = outbound.ServerPort

			tuic["uuid"] = outbound.UUID
			tuic["password"] = outbound.Password
			tuic["congestion-controller"] = outbound.CongestionController

			if outbound.TLS != nil {
				tuic["sni"] = outbound.TLS.ServerName
				tuic["alpn"] = outbound.TLS.Alpn
			}
			tuic["udp-relay-mode"] = outbound.UdpRelayMode
			tuic["udp"] = true
			tuic["skip-cert-verify"] = true

			proxies = append(proxies, tuic)
		case "trojan":
			name := uniqueName(names, outbound.Tag)
			trojan := make(map[string]any, 20)

			trojan["name"] = name
			trojan["type"] = scheme
			trojan["server"] = outbound.Server
			trojan["port"] = outbound.ServerPort

			trojan["password"] = outbound.Password
			trojan["udp"] = true
			trojan["skip-cert-verify"] = true

			if outbound.TLS != nil {
				trojan["sni"] = outbound.TLS.ServerName
				trojan["alpn"] = outbound.TLS.Alpn
			}

			trojan["network"] = outbound.Network

			switch outbound.Network {
			case "ws":
				wsOpts := make(map[string]any)

				if outbound.Transport != nil {
					wsOpts["path"] = outbound.Transport.Path
					wsOpts["headers"] = outbound.Transport.Headers
				}

				trojan["ws-opts"] = wsOpts
			case "grpc":
				grpcOpts := make(map[string]any)

				if outbound.Transport != nil {
					grpcOpts["grpc-service-name"] = outbound.Transport.ServiceName
				}

				trojan["grpc-opts"] = grpcOpts
			}

			trojan["client-fingerprint"] = "chrome"

			proxies = append(proxies, trojan)

		case "vless":
			name := uniqueName(names, outbound.Tag)
			vless := make(map[string]any, 20)

			vless["name"] = name
			vless["type"] = scheme
			vless["server"] = outbound.Server
			vless["port"] = outbound.ServerPort

			vless["uuid"] = outbound.UUID
			vless["flow"] = outbound.Flow

			vless["skip-cert-verify"] = true

			if outbound.TLS != nil {
				vless["servername"] = outbound.TLS.ServerName
				vless["alpn"] = outbound.TLS.Alpn

				if outbound.TLS.Reality != nil {
					vless["reality-opts"] = map[string]any{
						"public-key": outbound.TLS.Reality.PublicKey,
						"short-id":   outbound.TLS.Reality.ShortID,
					}
					vless["tls"] = true
					vless["client-fingerprint"] = "chrome"
				}
			}

			switch outbound.PacketEncoding {
			case "none":
			case "packetaddr":
				vless["packet-addr"] = true
			default:
				vless["xudp"] = true
			}

			vless["network"] = outbound.Network

			switch outbound.Network {
			case "h2":
				if outbound.Transport != nil {
					vless["h2-opts"] = outbound.Transport
				}
			case "tcp", "http":
				if outbound.Transport != nil {
					vless["http-opts"] = outbound.Transport
				}
			case "ws", "httpupgrade":
				if outbound.Transport != nil {
					vless["ws-opts"] = outbound.Transport
				}
			case "grpc":
				grpcOpts := make(map[string]any)
				if outbound.Transport != nil {
					grpcOpts["grpc-service-name"] = outbound.Transport.ServiceName
				}
				vless["grpc-opts"] = grpcOpts
			}

			proxies = append(proxies, vless)

		case "vmess":
			name := uniqueName(names, outbound.Tag)
			vmess := make(map[string]any, 20)

			vmess["name"] = name
			vmess["type"] = scheme
			vmess["server"] = outbound.Server
			vmess["port"] = outbound.ServerPort

			vmess["uuid"] = outbound.UUID
			vmess["alterId"] = outbound.AlterID
			vmess["cipher"] = outbound.Security

			vmess["udp"] = true
			vmess["xudp"] = true
			vmess["tls"] = false
			vmess["skip-cert-verify"] = true

			if outbound.TLS != nil {
				vmess["servername"] = outbound.TLS.ServerName
				vmess["alpn"] = outbound.TLS.Alpn
				vmess["tls"] = outbound.TLS.Enabled
			}

			vmess["network"] = outbound.Network

			switch outbound.Network {
			case "h2":
				if outbound.Transport != nil {
					vmess["h2-opts"] = outbound.Transport
				}
			case "tcp", "http":
				if outbound.Transport != nil {
					vmess["http-opts"] = outbound.Transport
				}
			case "ws", "httpupgrade":
				if outbound.Transport != nil {
					vmess["ws-opts"] = outbound.Transport
				}
			case "grpc":
				grpcOpts := make(map[string]any)
				if outbound.Transport != nil {
					grpcOpts["grpc-service-name"] = outbound.Transport.ServiceName
				}
				vmess["grpc-opts"] = grpcOpts
			}

			proxies = append(proxies, vmess)

		case "ss":

			name := uniqueName(names, outbound.Tag)
			ss := make(map[string]any, 20)

			ss["name"] = name
			ss["type"] = scheme
			ss["server"] = outbound.Server
			ss["port"] = outbound.ServerPort

			ss["cipher"] = outbound.Method
			ss["password"] = outbound.Password

			ss["udp"] = true

			if outbound.UdpOverTcp != nil {
				ss["udp-over-tcp"] = outbound.UdpOverTcp.Enabled
			}

			plugin := outbound.Plugin
			pluginInfo, _ := url.ParseQuery(strings.ReplaceAll(outbound.PluginOpts, ";", "&"))
			switch plugin {
			case "v2ray-plugin":
				ss["plugin"] = "v2ray-plugin"
				ss["plugin-opts"] = map[string]any{
					"mode": pluginInfo.Get("mode"),
					"host": pluginInfo.Get("host"),
					"path": pluginInfo.Get("path"),
					"tls":  strings.Contains(outbound.PluginOpts, "tls"),
				}
			case "obfs":
				ss["plugin"] = "obfs"
				ss["plugin-opts"] = map[string]any{
					"mode": pluginInfo.Get("obfs"),
					"host": pluginInfo.Get("obfs-host"),
				}
			}

			proxies = append(proxies, ss)
		}
	}

	if len(proxies) == 0 {
		return nil, fmt.Errorf("convert singbox subscribe error: format invalid")
	}

	return proxies, nil
}

type SingBoxOption struct {
	Username             string                    `json:"username,omitempty"`
	Password             string                    `json:"password,omitempty"`
	Server               string                    `json:"server,omitempty"`
	ServerPort           int                       `json:"server_port,omitempty"`
	Tag                  string                    `json:"tag,omitempty"`
	TLS                  *SingTLS                  `json:"tls,omitempty"`
	Transport            *SingTransport            `json:"transport,omitempty"`
	Type                 string                    `json:"type,omitempty"`
	Method               string                    `json:"method,omitempty"`
	AlterID              int                       `json:"alter_id,omitempty"`
	Security             string                    `json:"security,omitempty"`
	UUID                 string                    `json:"uuid,omitempty"`
	Default              string                    `json:"default,omitempty"`
	Outbounds            []string                  `json:"outbounds,omitempty"`
	Interval             string                    `json:"interval,omitempty"`
	Tolerance            int                       `json:"tolerance,omitempty"`
	URL                  string                    `json:"url,omitempty"`
	Network              string                    `json:"network,omitempty"`
	Plugin               string                    `json:"plugin,omitempty"`
	PluginOpts           string                    `json:"plugin_opts,omitempty"`
	ObfsParam            string                    `json:"obfs_param,omitempty"`
	Protocol             string                    `json:"protocol,omitempty"`
	ProtocolParam        string                    `json:"protocol_param,omitempty"`
	Flow                 string                    `json:"flow,omitempty"`
	PacketEncoding       string                    `json:"packet_encoding,omitempty"`
	AuthStr              string                    `json:"auth_str,omitempty"`
	DisableMtuDiscovery  bool                      `json:"disable_mtu_discovery,omitempty"`
	Down                 string                    `json:"down,omitempty"`
	DownMbps             int                       `json:"down_mbps,omitempty"`
	RecvWindow           int                       `json:"recv_window,omitempty"`
	RecvWindowConn       int                       `json:"recv_window_conn,omitempty"`
	Up                   string                    `json:"up,omitempty"`
	UpMbps               int                       `json:"up_mbps,omitempty"`
	Detour               string                    `json:"detour,omitempty"`
	Multiplex            *SingMultiplex            `json:"multiplex,omitempty"`
	Version              int                       `json:"version,omitempty"`
	UdpOverTcp           *SingUdpOverTcp           `json:"udp_over_tcp,omitempty"`
	SystemInterface      bool                      `json:"system_interface,omitempty"`
	InterfaceName        string                    `json:"interface_name,omitempty"`
	LocalAddress         []string                  `json:"local_address,omitempty"`
	PrivateKey           string                    `json:"private_key,omitempty"`
	Peers                []*SingWireguardMultiPeer `json:"peers,omitempty"`
	PeerPublicKey        string                    `json:"peer_public_key,omitempty"`
	PreSharedKey         string                    `json:"pre_shared_key,omitempty"`
	Reserved             []int64                   `json:"reserved,omitempty"`
	MTU                  uint                      `json:"mtu,omitempty"`
	CongestionController string                    `json:"congestion_control,omitempty"`
	UdpRelayMode         string                    `json:"udp_relay_mode,omitempty"`
	ZeroRttHandshake     bool                      `json:"zero_rtt_handshake,omitempty"`
	Heartbeat            string                    `json:"heartbeat,omitempty"`
	Obfs                 *SingObfs                 `json:"obfs,omitempty"`
	Ignored              bool                      `json:"-"`
	TcpFastOpen          bool                      `json:"tcp_fast_open,omitempty"`
	TcpMultiPath         bool                      `json:"tcp_multi_path,omitempty"`
	Visible              []string                  `json:"-"`
}

type SingUdpOverTcp struct {
	Enabled bool `json:"enabled,omitempty"`
}

type SingTLS struct {
	Enabled     bool         `json:"enabled,omitempty"`
	ServerName  string       `json:"server_name,omitempty"`
	Alpn        []string     `json:"alpn,omitempty"`
	Insecure    bool         `json:"insecure,omitempty"`
	Utls        *SingUtls    `json:"utls,omitempty"`
	Reality     *SingReality `json:"reality,omitempty"`
	Certificate string       `json:"certificate,omitempty"`
}

type SingUtls struct {
	Enabled     bool   `json:"enabled,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
}

type SingReality struct {
	Enabled   bool   `json:"enabled,omitempty"`
	PublicKey string `json:"public_key,omitempty"`
	ShortID   string `json:"short_id,omitempty"`
}

type SingTransport struct {
	Headers             map[string]any `json:"headers,omitempty"`
	Path                string         `json:"path,omitempty"`
	Type                string         `json:"type,omitempty"`
	EarlyDataHeaderName string         `json:"early_data_header_name,omitempty"`
	MaxEarlyData        int            `json:"max_early_data,omitempty"`
	Host                any            `json:"host,omitempty"`
	Method              string         `json:"method,omitempty"`
	ServiceName         string         `json:"service_name,omitempty"`
}

type SingMultiplex struct {
	Enabled        bool   `json:"enabled,omitempty"`
	MaxConnections int    `json:"max_connections,omitempty"`
	MinStreams     int    `json:"min_streams,omitempty"`
	MaxStreams     int    `json:"max_streams,omitempty"`
	Padding        bool   `json:"padding,omitempty"`
	Protocol       string `json:"protocol,omitempty"`
}

type SingWireguardMultiPeer struct {
	Server       string   `json:"server,omitempty"`
	ServerPort   int      `json:"server_port,omitempty"`
	PublicKey    string   `json:"public_key,omitempty"`
	PreSharedKey string   `json:"pre_shared_key,omitempty"`
	AllowedIps   []string `json:"allowed_ips,omitempty"`
	Reserved     []int64  `json:"reserved,omitempty"`
}

type SingObfs struct {
	Value string
	Type  string
}

type SingConfig struct {
	Outbounds []SingBoxOption `json:"outbounds,omitempty"`
}
