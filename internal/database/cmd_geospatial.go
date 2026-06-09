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

func (c *client) cmdGeopos(args []string) []byte {
	if len(args) < 2 {
		return simpleError("wrong number of arguments for 'GEOPOS' command")
	}
	key := args[0]
	members := args[1:]

	response := make([][]byte, 0, len(members))
	content, _, exists, err := c.db.getSortedSetEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}

	for _, member := range members {
		if !exists {
			response = append(response, respArray(nil))
			continue
		}
		geohash, memberExists := content[member]
		if !memberExists {
			response = append(response, respArray(nil))
			continue
		}

		longitude, latitude := decodeGeohash(geohash)
		response = append(response, respArray([][]byte{
			bulkString(strconv.FormatFloat(longitude, 'f', -1, 64), true),
			bulkString(strconv.FormatFloat(latitude, 'f', -1, 64), true),
		}))
	}
	return respArray(response)
}

func encodeGeohash(longitude, latitude float64) float64 {
	// Normalization and Truncation
	normLon := int64(normalizeRange * (longitude + longitudeBound) / (2 * longitudeBound))
	normLat := int64(normalizeRange * (latitude + latitudeBound) / (2 * latitudeBound))

	// Interleaving
	return float64(spreadInteger(normLon)<<1 | (spreadInteger(normLat)))
}

func decodeGeohash(geohash float64) (float64, float64) {
	normLon := compactInteger(int64(geohash) >> 1)
	normLat := compactInteger(int64(geohash))

	longitude := float64(2*normLon+1)*longitudeBound/normalizeRange - longitudeBound
	latitude := float64(2*normLat+1)*latitudeBound/normalizeRange - latitudeBound

	return longitude, latitude
}

func spreadInteger(v int64) int64 {
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

func compactInteger(v int64) int64 {
	v &= 0x5555555555555555

	v = (v | (v >> 1)) & 0x3333333333333333
	v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
	v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
	v = (v | (v >> 16)) & 0x00000000FFFFFFFF

	return v
}
