package convert

import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
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

			resolveBase(hysteria, name, scheme, outbound)
			resolveTls(hysteria, outbound.TLS)

			if outbound.Obfs != nil {
				hysteria["obfs"] = outbound.Obfs.Password
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

			proxies = append(proxies, hysteria)

		case "hysteria2":
			name := uniqueName(names, outbound.Tag)
			hysteria2 := make(map[string]any, 20)

			resolveBase(hysteria2, name, scheme, outbound)
			resolveTls(hysteria2, outbound.TLS)

			if outbound.Obfs != nil {
				hysteria2["obfs"] = outbound.Obfs.Type
				hysteria2["obfs-password"] = outbound.Obfs.Password
			}

			hysteria2["password"] = outbound.Password

			if outbound.DownMbps > 0 {
				hysteria2["down"] = outbound.DownMbps
			}
			if outbound.UpMbps > 0 {
				hysteria2["up"] = outbound.UpMbps
			}

			proxies = append(proxies, hysteria2)

		case "tuic":
			name := uniqueName(names, outbound.Tag)
			tuic := make(map[string]any, 20)

			resolveBase(tuic, name, scheme, outbound)

			tuic["uuid"] = outbound.UUID
			tuic["password"] = outbound.Password
			tuic["congestion-controller"] = outbound.CongestionController

			resolveTls(tuic, outbound.TLS)

			tuic["udp-relay-mode"] = outbound.UdpRelayMode
			tuic["udp"] = true

			proxies = append(proxies, tuic)
		case "trojan":
			name := uniqueName(names, outbound.Tag)
			trojan := make(map[string]any, 20)

			resolveBase(trojan, name, scheme, outbound)

			trojan["password"] = outbound.Password
			trojan["udp"] = true

			resolveTls(trojan, outbound.TLS)
			resolveNetwork(trojan, outbound)

			proxies = append(proxies, trojan)

		case "vless":
			name := uniqueName(names, outbound.Tag)
			vless := make(map[string]any, 20)

			resolveBase(vless, name, scheme, outbound)

			vless["uuid"] = outbound.UUID
			vless["flow"] = outbound.Flow

			resolveTls(vless, outbound.TLS)
			resolveNetwork(vless, outbound)

			switch outbound.PacketEncoding {
			case "none":
			case "packetaddr":
				vless["packet-addr"] = true
			default:
				vless["xudp"] = true
			}

			proxies = append(proxies, vless)

		case "vmess":
			name := uniqueName(names, outbound.Tag)
			vmess := make(map[string]any, 20)

			resolveBase(vmess, name, scheme, outbound)

			vmess["uuid"] = outbound.UUID
			vmess["alterId"] = outbound.AlterID
			vmess["cipher"] = outbound.Security

			vmess["udp"] = true
			vmess["xudp"] = true
			vmess["tls"] = false

			resolveTls(vmess, outbound.TLS)
			resolveNetwork(vmess, outbound)

			proxies = append(proxies, vmess)

		case "ss":

			name := uniqueName(names, outbound.Tag)
			ss := make(map[string]any, 20)

			resolveBase(ss, name, scheme, outbound)

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
	Headers             map[string]any `json:"headers,omitempty" yaml:"headers,omitempty"`
	Path                string         `json:"path,omitempty" yaml:"path,omitempty"`
	Type                string         `json:"type,omitempty" yaml:"type,omitempty"`
	EarlyDataHeaderName string         `json:"early_data_header_name,omitempty" yaml:"early-data-header-name,omitempty"`
	MaxEarlyData        int            `json:"max_early_data,omitempty" yaml:"max-early-data,omitempty"`
	Host                any            `json:"host,omitempty" yaml:"host,omitempty"`
	Method              string         `json:"method,omitempty" yaml:"method,omitempty"`
	ServiceName         string         `json:"service_name,omitempty" yaml:"grpc-service-name,omitempty"`
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
	Password string `json:"password,omitempty"`
	Type     string `json:"type,omitempty"`
}

type SingConfig struct {
	Outbounds []SingBoxOption `json:"outbounds,omitempty"`
}

func resolveBase(v map[string]any, name, scheme string, outbound SingBoxOption) {
	v["name"] = name
	v["type"] = scheme
	v["server"] = outbound.Server
	v["port"] = outbound.ServerPort
}

func resolveTls(v map[string]any, singTLS *SingTLS) {
	if singTLS != nil {
		v["servername"] = singTLS.ServerName
		v["tls"] = singTLS.Enabled

		if singTLS.Insecure {
			v["skip-cert-verify"] = true
		}

		if len(singTLS.Alpn) > 0 {
			v["alpn"] = singTLS.Alpn
		}

		if singTLS.Reality != nil {
			v["reality-opts"] = map[string]any{
				"public-key": singTLS.Reality.PublicKey,
				"short-id":   singTLS.Reality.ShortID,
			}
		}

		if singTLS.Utls != nil {
			v["client-fingerprint"] = singTLS.Utls.Fingerprint
		}
	}
}

func resolveNetwork(v map[string]any, outbound SingBoxOption) {
	if outbound.Network != "" {
		v["network"] = outbound.Network
	}

	if outbound.Transport != nil {
		network := "tcp"
		if outbound.Transport.Type != "" {
			network = outbound.Transport.Type
			outbound.Transport.Type = ""
		}
		v["network"] = network

		switch network {
		case "h2":
			v["h2-opts"] = SingTransportToMap(outbound.Transport)
		case "tcp", "http":
			v["http-opts"] = SingTransportToMap(outbound.Transport)
		case "ws":
			v["ws-opts"] = SingTransportToMap(outbound.Transport)
		case "httpupgrade":
			headers := make(map[string]any)
			wsOpts := make(map[string]any)
			headers["User-Agent"] = RandUserAgent()
			headers["Host"] = outbound.Transport.Host
			wsOpts["path"] = outbound.Transport.Path
			wsOpts["headers"] = headers
			wsOpts["v2ray-http-upgrade"] = true
			wsOpts["v2ray-http-upgrade-fast-open"] = true
			v["network"] = "ws"
			v["ws-opts"] = wsOpts
		case "grpc":
			v["grpc-opts"] = SingTransportToMap(outbound.Transport)
		}
	}
}

func SingTransportToMap(obj *SingTransport) map[string]interface{} {
	var result map[string]interface{}

	marshal, err := yaml.Marshal(obj)
	if err != nil {
		return nil
	}

	err = yaml.Unmarshal(marshal, &result)
	if err != nil {
		return nil
	}

	return result
}
