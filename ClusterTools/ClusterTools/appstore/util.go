package appstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func hasPrefix(key, prefix []byte) bool {
	if len(prefix) == 0 {
		return false
	}
	p := append(prefix, byte(0))
	return bytes.HasPrefix(key, p)
}

func prefixKey(keys ...[]byte) []byte {
	size := len(keys) - 1
	for _, key := range keys {
		size += len(key)
	}
	buf := make([]byte, 0, size)

	for i, key := range keys {
		if i > 0 {
			buf = append(buf, 0)
		}
		buf = append(buf, key...)
	}
	return buf
}

func unprefixKey(key, prefix []byte) ([]byte, error) {
	if len(prefix)+1 > len(key) {
		return nil, fmt.Errorf("prefix %s longer than key %s", string(prefix), string(key))
	}
	return key[len(prefix)+1:], nil
}

func evmRootKey(blockHeight int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(blockHeight))
	return prefixKey([]byte(prefixStart), []byte(evmRootPrefix), b)
}

func uint64ToByteBigEndian(height uint64) []byte {
	heightB := make([]byte, 8)
	binary.BigEndian.PutUint64(heightB, height)
	return heightB
}

func byteToUint64LittleEndian(b []byte) uint64 {
	return uint64(binary.LittleEndian.Uint64(b))
}

func formatPrefixes(key, oldPrefix, newPrefix []byte) ([]byte, error) {
	heightByteL, err := unprefixKey(key, oldPrefix)
	if err != nil {
		return nil, err
	}
	height := byteToUint64LittleEndian(heightByteL)
	heightByteB := uint64ToByteBigEndian(height)
	return prefixKey([]byte(newPrefix), heightByteB), nil
}
