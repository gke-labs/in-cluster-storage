/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"k8s.io/klog/v2"
)

// GeneratedCertificates holds in-memory PEM encoded TLS materials.
type GeneratedCertificates struct {
	CACert   []byte
	LeafCert []byte
	LeafKey  []byte
}

// GenerateTLS generates a self-signed CA (10-year validity) and a leaf certificate (1-year validity)
// for the given service name and namespace.
func GenerateTLS(serviceName, namespace string) (*GeneratedCertificates, error) {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate CA private key: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "objectfs-ca",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour), // 10 years
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	caDer, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create CA certificate: %w", err)
	}

	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate leaf private key: %w", err)
	}

	dnsNames := []string{
		serviceName,
		"localhost",
	}
	if namespace != "" {
		dnsNames = append(dnsNames,
			fmt.Sprintf("%s.%s", serviceName, namespace),
			fmt.Sprintf("%s.%s.svc", serviceName, namespace),
			fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, namespace),
		)
	}

	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: serviceName,
		},
		DNSNames:    dnsNames,
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour), // 1-year leaf
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	leafDer, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create leaf certificate: %w", err)
	}

	caPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDer})
	leafPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDer})
	leafKeyPem := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(leafKey)})

	return &GeneratedCertificates{
		CACert:   caPem,
		LeafCert: leafPem,
		LeafKey:  leafKeyPem,
	}, nil
}

type kubeSecret struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   kubeSecretMeta    `json:"metadata"`
	Type       string            `json:"type"`
	StringData map[string]string `json:"stringData"`
}

type kubeSecretMeta struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// EnsureTLSSecret checks whether secretName exists in namespace, and creates it with
// generated CA and leaf certificates if it does not.
func EnsureTLSSecret(ctx context.Context, apiServerURL, tokenPath, caCertPath, namespace, secretName, serviceName string) error {
	if apiServerURL == "" {
		host := os.Getenv("KUBERNETES_SERVICE_HOST")
		port := os.Getenv("KUBERNETES_SERVICE_PORT")
		if host != "" && port != "" {
			apiServerURL = fmt.Sprintf("https://%s:%s", host, port)
		} else {
			apiServerURL = "https://kubernetes.default.svc"
		}
	}
	if tokenPath == "" {
		tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}
	if caCertPath == "" {
		caCertPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	if namespace == "" {
		namespace = os.Getenv("POD_NAMESPACE")
	}
	if namespace == "" {
		namespace = "kube-objectfs-system"
	}
	if secretName == "" {
		secretName = "objectfs-tls"
	}
	if serviceName == "" {
		serviceName = "objectfs-controller"
	}

	var rootCAs *x509.CertPool
	b, err := os.ReadFile(caCertPath)
	if err != nil {
		if !os.IsNotExist(err) {
			klog.V(4).Infof("could not read kubernetes cluster CA cert from %s: %v", caCertPath, err)
		}
	} else {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(b) {
			rootCAs = pool
		}
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: rootCAs},
		},
		Timeout: 10 * time.Second,
	}

	controllerToken, err := os.ReadFile(tokenPath)
	if err != nil {
		return fmt.Errorf("failed to read service account token from %s: %w", tokenPath, err)
	}

	apiServerURL = strings.TrimRight(apiServerURL, "/")
	secretURL := fmt.Sprintf("%s/api/v1/namespaces/%s/secrets/%s", apiServerURL, namespace, secretName)

	// Check if Secret already exists
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, secretURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create GET request for secret: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(controllerToken)))

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to check secret %s/%s: %w", namespace, secretName, err)
	}
	defer errutil.CloseAndLog(resp.Body, "secret get response body")

	if resp.StatusCode == http.StatusOK {
		klog.Infof("TLS Secret %s/%s already exists; skipping generation", namespace, secretName)
		return nil
	} else if resp.StatusCode != http.StatusNotFound {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("unexpected status %d checking secret %s/%s (failed to read body: %w)", resp.StatusCode, namespace, secretName, err)
		}
		return fmt.Errorf("unexpected status %d checking secret %s/%s: %s", resp.StatusCode, namespace, secretName, strings.TrimSpace(string(body)))
	}

	// Generate certificates
	certs, err := GenerateTLS(serviceName, namespace)
	if err != nil {
		return fmt.Errorf("failed to generate TLS certificates: %w", err)
	}

	secretObj := kubeSecret{
		APIVersion: "v1",
		Kind:       "Secret",
		Metadata: kubeSecretMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		Type: "kubernetes.io/tls",
		StringData: map[string]string{
			"ca.crt":  string(certs.CACert),
			"tls.crt": string(certs.LeafCert),
			"tls.key": string(certs.LeafKey),
		},
	}

	payload, err := json.Marshal(secretObj)
	if err != nil {
		return fmt.Errorf("failed to marshal secret json: %w", err)
	}

	postURL := fmt.Sprintf("%s/api/v1/namespaces/%s/secrets", apiServerURL, namespace)
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create POST request for secret: %w", err)
	}
	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(controllerToken)))

	postResp, err := httpClient.Do(postReq)
	if err != nil {
		return fmt.Errorf("failed to create secret %s/%s: %w", namespace, secretName, err)
	}
	defer errutil.CloseAndLog(postResp.Body, "secret create response body")

	if postResp.StatusCode == http.StatusCreated || postResp.StatusCode == http.StatusOK || postResp.StatusCode == http.StatusConflict {
		klog.Infof("TLS Secret %s/%s successfully created or already exists", namespace, secretName)
		return nil
	}

	body, err := io.ReadAll(postResp.Body)
	if err != nil {
		return fmt.Errorf("failed to create secret %s/%s (HTTP %d, failed to read body: %w)", namespace, secretName, postResp.StatusCode, err)
	}
	return fmt.Errorf("failed to create secret %s/%s: HTTP %d: %s", namespace, secretName, postResp.StatusCode, strings.TrimSpace(string(body)))
}
