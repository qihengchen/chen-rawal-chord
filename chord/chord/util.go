/*
 *  Brown University, CS138, Spring 2017
 *
 *  Purpose: Utility functions to help with dealing with ID hashes in Chord.
 */

package chord

import (
	"bytes"
	"crypto/sha1"
	"math/big"
)

// Hash a string to its appropriate size.
func HashKey(key string) []byte {
	h := sha1.New()
	h.Write([]byte(key))
	v := h.Sum(nil)
	return v[:KEY_LENGTH/8]
}

// Convert a []byte to a big.Int string (useful for debugging/logging)
func HashStr(keyHash []byte) string {
	keyInt := big.Int{}
	keyInt.SetBytes(keyHash)
	return keyInt.String()
}

func EqualIds(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// Example of how to do math operations on []byte IDs.
// You may not need this function.
func AddIds(a, b []byte) []byte {
	aInt := big.Int{}
	aInt.SetBytes(a)

	bInt := big.Int{}
	bInt.SetBytes(b)

	sum := big.Int{}
	sum.Add(&aInt, &bInt)
	return sum.Bytes()
}

/*
 * On this crude ascii Chord ring, X is between (A : B)
 *
 *    ___
 *   /   \-A
 *  |     |
 * B-\   /-X
 *    ---
 */
func Between(nodeX, nodeA, nodeB []byte) bool {

	if bytes.Compare(nodeA, nodeB) > 0 {
		// < A, > B, no
		if bytes.Compare(nodeX, nodeA) < 0 && bytes.Compare(nodeX, nodeB) > 0 {
			return false
		} else { // >A, >B; <A, <B; yes
			return true
		}
	} else {
		if bytes.Compare(nodeX, nodeA) > 0 && bytes.Compare(nodeX, nodeB) < 0 {
			return true
		} else {
			return false
		}
	}

	// if (bytes.Compare(nodeX, nodeA) > 0) && (bytes.Compare(nodeX, nodeB) < 0) {
	// 	return true
	// }

	// If nodeX's id is between nodeA's id and nodeB's id
	// if xId > aId && xId < bId {
	// 	return true
	// }
}

// Returns true if X is between (A : B]
func BetweenRightIncl(nodeX, nodeA, nodeB []byte) bool {

	if bytes.Compare(nodeA, nodeB) > 0 {
		// < A, > B, no
		if bytes.Compare(nodeX, nodeA) < 0 && bytes.Compare(nodeX, nodeB) >= 0 {
			return false
		} else { // >A, >B; <A, <B; yes
			return true
		}
	} else {
		if bytes.Compare(nodeX, nodeA) > 0 && bytes.Compare(nodeX, nodeB) <= 0 {
			return true
		} else {
			return false
		}
	}
}
