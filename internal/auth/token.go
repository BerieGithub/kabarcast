// Package auth verifies the two credentials kabarcast accepts:
//
//  1. Channel tokens - short-lived JWTs your application signs and hands to
//     its own logged-in users. They name exactly which channels that user may
//     subscribe to, so the hub enforces tenant isolation without ever reading
//     your database.
//  2. The service secret - a bearer token your backends use to publish.
package auth

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrNoSecret      = errors.New("kabarcast: no client token secret configured")
	ErrInvalidToken  = errors.New("kabarcast: invalid channel token")
	ErrTokenExpired  = errors.New("kabarcast: channel token expired")
	ErrNotAuthorized = errors.New("kabarcast: token does not grant this channel")
)

// ChannelClaims is the payload your app signs.
//
//	{
//	  "sub": "user-uuid",
//	  "channels": ["app:user:<id>", "app:org:<id>:*"],
//	  "exp": 1735689600
//	}
//
// A trailing "*" makes the entry a prefix grant, which lets you issue one
// token covering a whole tenant namespace.
type ChannelClaims struct {
	Channels []string `json:"channels"`
	jwt.RegisteredClaims
}

// VerifyChannelToken checks the HMAC signature and expiry, returning the
// claims. It performs no I/O.
func VerifyChannelToken(tokenStr, secret string) (*ChannelClaims, error) {
	if secret == "" {
		return nil, ErrNoSecret
	}
	claims := &ChannelClaims{}
	_, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
		}
		return []byte(secret), nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// CanSubscribe reports whether the token grants the given channel. Matching is
// exact, or by prefix when the grant ends in "*".
func (c *ChannelClaims) CanSubscribe(channel string) bool {
	for _, g := range c.Channels {
		if g == channel {
			return true
		}
		if strings.HasSuffix(g, "*") && strings.HasPrefix(channel, strings.TrimSuffix(g, "*")) {
			return true
		}
	}
	return false
}

// ValidServiceSecret compares the presented bearer secret in constant time.
func ValidServiceSecret(presented, expected string) bool {
	if expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(expected)) == 1
}
