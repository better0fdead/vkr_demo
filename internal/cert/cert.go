package cert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/rs/zerolog/log"
)

func generateSelfSignedCert() tls.Certificate {
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(100 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
	}
}

func LoadTLSCertificate() (tls.Certificate, error) {
	cert, certOk := os.LookupEnv("TLS_CERT")
	key, keyOk := os.LookupEnv("TLS_KEY")

	if certOk && keyOk {
		cert, err := tls.X509KeyPair([]byte(cert), []byte(key))
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("failed to load TLS cert: %v", err)
		}
		log.Info().Msg("using TLS certificate from env")
		return cert, nil
	}

	if certOk != keyOk {
		return tls.Certificate{}, fmt.Errorf("both TLS_CERT and TLS_KEY must be set")
	}

	log.Info().Msg("generating self-signed certificate")
	return generateSelfSignedCert(), nil
}

func LoadCACertPool() (*x509.CertPool, error) {
	caCert, ok := os.LookupEnv("CA_CERT")
	if !ok {
		log.Debug().Msg("CA_CERT not set")
		return nil, nil
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM([]byte(caCert)) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	log.Info().Msg("using CA certificate from env")
	return caCertPool, nil
}

func LoadClientTLSCertificate() (tls.Certificate, error) {
	clientCert, certOk := os.LookupEnv("CLIENT_TLS_CERT")
	clientKey, keyOk := os.LookupEnv("CLIENT_TLS_KEY")

	if certOk && keyOk {
		cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("failed to load client TLS cert: %v", err)
		}
		log.Info().Msg("using client TLS certificate from env")
		return cert, nil
	}

	if certOk != keyOk {
		return tls.Certificate{}, fmt.Errorf("both CLIENT_TLS_CERT and CLIENT_TLS_KEY must be set")
	}

	log.Debug().Msg("client certificates not configured")
	return tls.Certificate{}, nil
}

func GetTLSConfigForClient(insecureSkipVerify bool) (*tls.Config, error) {
	config := &tls.Config{
		NextProtos:         []string{"h3"},
		InsecureSkipVerify: insecureSkipVerify,
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
		CipherSuites: []uint16{
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
		},
		CurvePreferences: []tls.CurveID{
			tls.X25519,
			tls.CurveP256,
			tls.CurveP384,
		},
	}

	if !insecureSkipVerify {
		caPool, err := LoadCACertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to load CA cert pool: %v", err)
		}
		if caPool != nil {
			config.RootCAs = caPool
		}
	}
	clientCert, err := LoadClientTLSCertificate()
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %v", err)
	}

	if len(clientCert.Certificate) > 0 {
		config.Certificates = []tls.Certificate{clientCert}
		log.Info().Msg("client certificate configured for mutual TLS")
	}

	return config, nil
}
