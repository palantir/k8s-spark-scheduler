// Copyright (c) 2018 Palantir Technologies. All rights reserved.
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

package witchcraft

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"time"

	"github.com/palantir/pkg/tlsconfig"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-server/config"
)

func (s *Server) newServer(productName string, serverConfig config.Server, handler http.Handler) (rHTTPServer *http.Server, rStart func() error, rShutdown func(context.Context) error, rErr error) {
	return newServerStartShutdownFns(
		serverConfig,
		s.useSelfSignedServerCertificate,
		s.clientAuth,
		productName,
		s.svcLogger,
		handler,
	)
}

func (s *Server) newMgmtServer(productName string, serverConfig config.Server, handler http.Handler) (rStart func() error, rShutdown func(context.Context) error, rErr error) {
	serverConfig.Port = serverConfig.ManagementPort
	_, start, shutdown, err := newServerStartShutdownFns(
		serverConfig,
		s.useSelfSignedServerCertificate,
		tls.NoClientCert,
		productName+"-management",
		s.svcLogger,
		handler,
	)
	return start, shutdown, err
}

func newServerStartShutdownFns(
	serverConfig config.Server,
	useSelfSignedServerCertificate bool,
	clientAuthType tls.ClientAuthType,
	serverName string,
	svcLogger svc1log.Logger,
	handler http.Handler,
) (rHTTPServer *http.Server, start func() error, shutdown func(context.Context) error, rErr error) {
	tlsConfig, err := newTLSConfig(serverConfig, useSelfSignedServerCertificate, clientAuthType)
	if err != nil {
		return nil, nil, nil, err
	}

	addr := fmt.Sprintf("%v:%d", serverConfig.Address, serverConfig.Port)
	httpServer := &http.Server{
		Addr:      addr,
		TLSConfig: tlsConfig,
		Handler:   handler,
	}
	return httpServer, func() error {
		svcLogger.Info("Listening to https", svc1log.SafeParam("address", addr), svc1log.SafeParam("server", serverName))

		// cert and key specified in TLS config so no need to pass in here
		if err := httpServer.ListenAndServeTLS("", ""); err != nil {
			if err == http.ErrServerClosed {
				svcLogger.Info(fmt.Sprintf("%s was closed", serverName))
				return nil
			}
			return werror.Wrap(err, "server failed", werror.SafeParam("serverName", serverName))
		}
		return nil
	}, httpServer.Shutdown, nil
}

func newTLSConfig(serverConfig config.Server, useSelfSignedServerCertificate bool, clientAuthType tls.ClientAuthType) (*tls.Config, error) {
	if !useSelfSignedServerCertificate && (serverConfig.KeyFile == "" || serverConfig.CertFile == "") {
		var msg string
		if serverConfig.KeyFile == "" && serverConfig.CertFile == "" {
			msg = "key file and certificate file"
		} else if serverConfig.KeyFile == "" {
			msg = "key file"
		} else {
			msg = "certificate file"
		}
		return nil, werror.Error(msg + " for server not specified in configuration")
	}

	tlsConfig, err := tlsconfig.NewServerConfig(
		newTLSCertProvider(useSelfSignedServerCertificate, serverConfig.CertFile, serverConfig.KeyFile),
		tlsconfig.ServerClientCAFiles(serverConfig.ClientCAFiles...),
		tlsconfig.ServerClientAuthType(clientAuthType),
		tlsconfig.ServerNextProtos("h2"),
	)
	if err != nil {
		return nil, werror.Wrap(err, "failed to initialize TLS configuration for server")
	}
	return tlsConfig, nil
}

func newTLSCertProvider(useSelfSignedServerCertificate bool, certFile, keyFile string) tlsconfig.TLSCertProvider {
	if useSelfSignedServerCertificate {
		return func() (tls.Certificate, error) {
			return newSelfSignedCertificate()
		}
	}
	return tlsconfig.TLSCertFromFiles(certFile, keyFile)
}

// newSelfSignedCertificate creates a new self-signed certificate that can be used for TLS. The generated certificate is
// quite minimal: it has a hard-coded serial number, is valid for 1 year and does NOT have a common name or IP SANs
// set.
func newSelfSignedCertificate() (tls.Certificate, error) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	privPKCS1DERBytes := x509.MarshalPKCS1PrivateKey(privKey)
	privPKCS1PEMBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: privPKCS1DERBytes})

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-30 * time.Second),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
	}
	certDERBytes, err := x509.CreateCertificate(rand.Reader, template, template, &privKey.PublicKey, privKey)
	if err != nil {
		return tls.Certificate{}, err
	}

	cert, err := x509.ParseCertificate(certDERBytes)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEMBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	return tls.X509KeyPair(certPEMBytes, privPKCS1PEMBytes)
}
