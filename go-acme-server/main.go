package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-acme/lego/v4/certificate"
	"github.com/go-acme/lego/v4/challenge/http01"
	"github.com/go-acme/lego/v4/lego"
	"github.com/go-acme/lego/v4/registration"
)

type MyUser struct {
	email string
	pk    crypto.PrivateKey
	reg   *registration.Resource
}

// GetEmail implements registration.User.
func (u *MyUser) GetEmail() string {
	return u.email
}

// GetRegistration implements registration.User.
func (u *MyUser) GetRegistration() *registration.Resource {
	return u.reg
}

// GetPrivateKey implements registration.User.
func (u *MyUser) GetPrivateKey() crypto.PrivateKey {
	return u.pk
}

func writeCertFile(name string, content []byte) error {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(content)
	return err
}

func run(addr string) error {
	pk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generating private key failed: %w", err)
	}
	user := MyUser{email: "me@example.com", pk: pk}
	cfg := lego.NewConfig(&user)
	cfg.CADirURL = "https://localhost:14000/dir"
	insecureTransport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	httpCl := &http.Client{Transport: insecureTransport}
	cfg.HTTPClient = httpCl

	cl, err := lego.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("lego client setup failed: %w", err)
	}
	err = cl.Challenge.SetHTTP01Provider(http01.NewProviderServer("", "5080"))
	if err != nil {
		return fmt.Errorf("HTTP-01 challenge setup failed: %w", err)
	}
	reg, err := cl.Registration.Register(registration.RegisterOptions{TermsOfServiceAgreed: true})
	if err != nil {
		return fmt.Errorf("client account registratin failed: %w", err)
	}
	user.reg = reg
	certs, err := cl.Certificate.Obtain(certificate.ObtainRequest{
		Domains: []string{"localhost"},
		Bundle:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to obtain certificates: %w", err)
	}

	certFile, keyFile := "cert-"+certs.Domain+".crt", "cert-"+certs.Domain+".key"
	err = writeCertFile(certFile, certs.Certificate)
	if err != nil {
		return fmt.Errorf("failed to open certificate file for writing: %w", err)
	}
	err = writeCertFile(keyFile, certs.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to open private key file for writing: %w", err)
	}
	// openssl x509 -text -noout -in cert-localhost.crt

	log.Println("listening on:", addr)
	return http.ListenAndServeTLS(addr, certFile, keyFile, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "Hello, World!") }))
}

func main() {
	addr := os.Args[1]

	err := run(addr)
	if err != nil {
		log.Fatal(err)
	}
}
