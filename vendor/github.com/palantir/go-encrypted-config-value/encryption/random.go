// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryption

import (
	"crypto/rand"
	"fmt"
)

// RandomBytes returns a slice that contains the specified number of cryptographically strong pseudo-random bytes.
func RandomBytes(n int) ([]byte, error) {
	out := make([]byte, n)
	if _, err := rand.Read(out); err != nil {
		return nil, fmt.Errorf("failed to generate %d cryptographically strong pseudo-random bytes: %v", n, err)
	}
	return out, nil
}
