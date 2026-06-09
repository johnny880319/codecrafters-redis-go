package database

import (
	"strconv"
	"time"
)

const (
	longitudeBound = 180.0
	latitudeBound  = 85.05112878
	normalizeRange = 1 << 26
)

func (c *client) cmdGeoadd(args []string) []byte {
	if len(args) != 4 {
		return simpleError("wrong number of arguments for 'GEOADD' command")
	}
	key := args[0]
	member := args[3]
	longitude, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return simpleError("invalid longitude value")
	}
	latitude, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return simpleError("invalid latitude value")
	}

	if longitude < -longitudeBound || longitude > longitudeBound || latitude < -latitudeBound || latitude > latitudeBound {
		return simpleError(invalidLongitudeLatitude)
	}

	content, entry, exists, err := c.db.getSortedSetEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		entry = dbEntry{
			value:     make(map[string]float64),
			vType:     SortedSetType,
			expiresAt: time.Time{},
		}
	}

	returnVal := 1
	if _, memberExists := content[member]; memberExists {
		returnVal = 0
	}

	content[member] = encodeGeohash(longitude, latitude)
	entry.value = content
	c.db.data[key] = entry

	return respInteger(returnVal)
}

func encodeGeohash(longitude, latitude float64) float64 {
	// Normalization and Truncation
	normLon := int64(normalizeRange * (longitude + longitudeBound) / (2 * longitudeBound))
	normLat := int64(normalizeRange * (latitude + latitudeBound) / (2 * latitudeBound))

	// Interleaving
	return float64(spread_integer(normLon)<<1 | (spread_integer(normLat)))
}

func spread_integer(v int64) int64 {
	// Ensure only lower 32 bits are non-zero.
	v &= 0xFFFFFFFF

	// Bitwise operations to spread 32 bits into 64 bits with zeros in-between
	v = (v | (v << 16)) & 0x0000FFFF0000FFFF
	v = (v | (v << 8)) & 0x00FF00FF00FF00FF
	v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v << 2)) & 0x3333333333333333
	v = (v | (v << 1)) & 0x5555555555555555

	return v
}
