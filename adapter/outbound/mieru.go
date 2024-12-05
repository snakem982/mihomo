package outbound

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/netip"
	"runtime"
	"strconv"
	"sync"

	mieruclient "github.com/enfein/mieru/v3/apis/client"
	mierumodel "github.com/enfein/mieru/v3/apis/model"
	mierupb "github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/proxydialer"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
	"google.golang.org/protobuf/proto"
)

type Mieru struct {
	*Base
	option *MieruOption
	client mieruclient.Client
	mu     sync.Mutex
}

type MieruOption struct {
	BasicOption
	Name      string `proxy:"name"`
	Server    string `proxy:"server"`
	Port      int    `proxy:"port,omitempty"`
	PortRange string `proxy:"port-range,omitempty"`
	Transport string `proxy:"transport"`
	UserName  string `proxy:"username"`
	Password  string `proxy:"password"`
}

type MieruResolver struct {
	resolver.Resolver
}

// StreamConnContext implements C.ProxyAdapter
func (m *Mieru) StreamConnContext(ctx context.Context, c net.Conn, metadata *C.Metadata) (net.Conn, error) {
	if err := m.ensureClientIsRunning(); err != nil {
		return nil, err
	}

	addr := metadataToMieruNetAddrSpec(metadata)
	return m.client.DialContextWithConn(ctx, c, addr)
}

// DialContext implements C.ProxyAdapter
func (m *Mieru) DialContext(ctx context.Context, metadata *C.Metadata, opts ...dialer.Option) (C.Conn, error) {
	return m.DialContextWithDialer(ctx, dialer.NewDialer(m.Base.DialOptions(opts...)...), metadata)
}

// DialContextWithDialer implements C.ProxyAdapter
func (m *Mieru) DialContextWithDialer(ctx context.Context, dialer C.Dialer, metadata *C.Metadata) (C.Conn, error) {
	var err error
	if len(m.option.DialerProxy) > 0 {
		dialer, err = proxydialer.NewByName(m.option.DialerProxy, dialer)
		if err != nil {
			return nil, err
		}
	}
	network, address, err := m.pickOneServerEndpoint()
	if err != nil {
		return nil, err
	}
	c, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("%s connect error: %w", address, err)
	}
	defer func(c net.Conn) {
		safeConnClose(c, err)
	}(c)
	c, err = m.StreamConnContext(ctx, c, metadata)
	if err != nil {
		return nil, err
	}

	return NewConn(c, m), nil
}

// SupportWithDialer implements C.ProxyAdapter
func (m *Mieru) SupportWithDialer() C.NetWork {
	return C.TCP
}

// ProxyInfo implements C.ProxyAdapter
func (m *Mieru) ProxyInfo() C.ProxyInfo {
	info := m.Base.ProxyInfo()
	info.DialerProxy = m.option.DialerProxy
	return info
}

func (m *Mieru) ensureClientIsRunning() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client.IsRunning() {
		return nil
	}

	config, err := m.client.Load()
	if err != nil {
		return err
	}
	serverDomainName := config.Profile.GetServers()[0].GetDomainName()
	if serverDomainName != "" && config.Resolver == nil {
		if resolver.ProxyServerHostResolver == nil {
			return fmt.Errorf("mieru server is a domain name %q but DNS is not enabled", serverDomainName)
		}
		config.Resolver = MieruResolver{Resolver: resolver.ProxyServerHostResolver}
		if err := m.client.Store(config); err != nil {
			return err
		}
	}

	if err := m.client.Start(); err != nil {
		return fmt.Errorf("failed to start mieru client: %w", err)
	}
	return nil
}

func (m *Mieru) pickOneServerEndpoint() (network, address string, err error) {
	if m.option.Transport == "TCP" {
		network = "tcp"
	} else {
		err = fmt.Errorf("transport %s is invalid", m.option.Transport)
		return
	}
	if m.option.Port != 0 {
		address = net.JoinHostPort(m.option.Server, strconv.Itoa(m.option.Port))
	} else {
		var beginPort, endPort int
		beginPort, endPort, err = beginAndEndPortFromPortRange(m.option.PortRange)
		if err != nil {
			return
		}
		randomPort := beginPort + rand.Intn(endPort-beginPort+1)
		address = net.JoinHostPort(m.option.Server, strconv.Itoa(randomPort))
	}
	return
}

func (mr MieruResolver) LookupIP(ctx context.Context, network, host string) ([]net.IP, error) {
	var netIPs []netip.Addr
	var err error
	if network == "ip4" {
		netIPs, err = mr.Resolver.LookupIPv4(ctx, host)
	} else if network == "ip6" {
		netIPs, err = mr.Resolver.LookupIPv6(ctx, host)
	} else {
		netIPs, err = mr.Resolver.LookupIP(ctx, host)
	}
	if err != nil {
		return nil, err
	}

	ips := make([]net.IP, len(netIPs))
	for i := 0; i < len(netIPs); i++ {
		ips[i] = netIPs[i].AsSlice()
	}
	return ips, nil
}

func NewMieru(option MieruOption) (*Mieru, error) {
	config, err := buildMieruClientConfig(option)
	if err != nil {
		return nil, fmt.Errorf("failed to build mieru client config: %w", err)
	}
	c := mieruclient.NewClient()
	if err := c.Store(config); err != nil {
		return nil, fmt.Errorf("failed to store mieru client config: %w", err)
	}
	// Client is started lazily on the first use.

	var addr string
	if option.Port != 0 {
		addr = net.JoinHostPort(option.Server, strconv.Itoa(option.Port))
	} else {
		beginPort, _, _ := beginAndEndPortFromPortRange(option.PortRange)
		addr = net.JoinHostPort(option.Server, strconv.Itoa(beginPort))
	}
	outbound := &Mieru{
		Base: &Base{
			name:   option.Name,
			addr:   addr,
			iface:  option.Interface,
			tp:     C.Mieru,
			udp:    false,
			xudp:   false,
			rmark:  option.RoutingMark,
			prefer: C.NewDNSPrefer(option.IPVersion),
		},
		option: &option,
		client: c,
	}
	runtime.SetFinalizer(outbound, closeMieru)
	return outbound, nil
}

func closeMieru(m *Mieru) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client != nil && m.client.IsRunning() {
		m.client.Stop()
	}
}

func metadataToMieruNetAddrSpec(metadata *C.Metadata) mierumodel.NetAddrSpec {
	if metadata.Host != "" {
		return mierumodel.NetAddrSpec{
			AddrSpec: mierumodel.AddrSpec{
				FQDN: metadata.Host,
				Port: int(metadata.DstPort),
			},
			Net: "tcp",
		}
	} else {
		return mierumodel.NetAddrSpec{
			AddrSpec: mierumodel.AddrSpec{
				IP:   metadata.DstIP.AsSlice(),
				Port: int(metadata.DstPort),
			},
			Net: "tcp",
		}
	}
}

func buildMieruClientConfig(option MieruOption) (*mieruclient.ClientConfig, error) {
	if err := validateMieruOption(option); err != nil {
		return nil, fmt.Errorf("failed to validate mieru option: %w", err)
	}

	transportProtocol := mierupb.TransportProtocol_TCP.Enum()
	var server *mierupb.ServerEndpoint
	if net.ParseIP(option.Server) != nil {
		// server is an IP address
		if option.PortRange != "" {
			server = &mierupb.ServerEndpoint{
				IpAddress: proto.String(option.Server),
				PortBindings: []*mierupb.PortBinding{
					{
						PortRange: proto.String(option.PortRange),
						Protocol:  transportProtocol,
					},
				},
			}
		} else {
			server = &mierupb.ServerEndpoint{
				IpAddress: proto.String(option.Server),
				PortBindings: []*mierupb.PortBinding{
					{
						Port:     proto.Int32(int32(option.Port)),
						Protocol: transportProtocol,
					},
				},
			}
		}
	} else {
		// server is a domain name
		if option.PortRange != "" {
			server = &mierupb.ServerEndpoint{
				DomainName: proto.String(option.Server),
				PortBindings: []*mierupb.PortBinding{
					{
						PortRange: proto.String(option.PortRange),
						Protocol:  transportProtocol,
					},
				},
			}
		} else {
			server = &mierupb.ServerEndpoint{
				DomainName: proto.String(option.Server),
				PortBindings: []*mierupb.PortBinding{
					{
						Port:     proto.Int32(int32(option.Port)),
						Protocol: transportProtocol,
					},
				},
			}
		}
	}
	return &mieruclient.ClientConfig{
		Profile: &mierupb.ClientProfile{
			ProfileName: proto.String(option.Name),
			User: &mierupb.User{
				Name:     proto.String(option.UserName),
				Password: proto.String(option.Password),
			},
			Servers: []*mierupb.ServerEndpoint{server},
			Multiplexing: &mierupb.MultiplexingConfig{
				// Multiplexing doesn't work well with connection tracking.
				Level: mierupb.MultiplexingLevel_MULTIPLEXING_OFF.Enum(),
			},
		},
	}, nil
}

func validateMieruOption(option MieruOption) error {
	if option.Name == "" {
		return fmt.Errorf("name is empty")
	}
	if option.Server == "" {
		return fmt.Errorf("server is empty")
	}
	if option.Port == 0 && option.PortRange == "" {
		return fmt.Errorf("either port or port-range must be set")
	}
	if option.Port != 0 && option.PortRange != "" {
		return fmt.Errorf("port and port-range cannot be set at the same time")
	}
	if option.Port != 0 && (option.Port < 1 || option.Port > 65535) {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if option.PortRange != "" {
		begin, end, err := beginAndEndPortFromPortRange(option.PortRange)
		if err != nil {
			return fmt.Errorf("invalid port-range format")
		}
		if begin < 1 || begin > 65535 {
			return fmt.Errorf("begin port must be between 1 and 65535")
		}
		if end < 1 || end > 65535 {
			return fmt.Errorf("end port must be between 1 and 65535")
		}
		if begin > end {
			return fmt.Errorf("begin port must be less than or equal to end port")
		}
	}

	if option.Transport != "TCP" {
		return fmt.Errorf("transport must be TCP")
	}
	if option.UserName == "" {
		return fmt.Errorf("username is empty")
	}
	if option.Password == "" {
		return fmt.Errorf("password is empty")
	}
	return nil
}

func beginAndEndPortFromPortRange(portRange string) (int, int, error) {
	var begin, end int
	_, err := fmt.Sscanf(portRange, "%d-%d", &begin, &end)
	return begin, end, err
}
