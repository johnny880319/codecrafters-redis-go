package database

import (
	"math"
	"strconv"
	"time"
)

const (
	longitudeBound      = 180.0
	latitudeBound       = 85.05112878
	normalizeRange      = 1 << 26
	earthRadiusInMeters = 6372797.560856
)

func (c *client) cmdGeoadd(args []string) []byte {
	c.db.rwMu.Lock()
	defer c.db.rwMu.Unlock()

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
	c.db.rwMu.Lock()
	defer c.db.rwMu.Unlock()

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

func (c *client) cmdGeodist(args []string) []byte {
	c.db.rwMu.Lock()
	defer c.db.rwMu.Unlock()

	if len(args) != 3 {
		return simpleError("wrong number of arguments for 'GEODIST' command")
	}
	key := args[0]
	member1 := args[1]
	member2 := args[2]

	content, _, exists, err := c.db.getSortedSetEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		return bulkString("", false)
	}

	hash1, member1Exists := content[member1]
	hash2, member2Exists := content[member2]
	if !member1Exists || !member2Exists {
		return bulkString("", false)
	}

	distance := computeDistance(hash1, hash2)
	return bulkString(strconv.FormatFloat(distance, 'f', -1, 64), true)
}

func (c *client) cmdGeosearch(args []string) []byte {
	c.db.rwMu.Lock()
	defer c.db.rwMu.Unlock()

	if len(args) != 7 {
		return simpleError("wrong number of arguments for 'GEOSEARCH' command")
	}

	if args[1] != "FROMLONLAT" || args[4] != "BYRADIUS" || args[6] != "m" {
		return simpleError("GEOSEARCH currently only supports the syntax: " +
			"GEOSEARCH <key> FROMLONLAT <longitude> <latitude> BYRADIUS <radius> m")
	}

	key := args[0]
	longitude, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return simpleError("invalid longitude value")
	}
	latitude, err := strconv.ParseFloat(args[3], 64)
	if err != nil {
		return simpleError("invalid latitude value")
	}
	radius, err := strconv.ParseFloat(args[5], 64)
	if err != nil {
		return simpleError("invalid radius value")
	}

	if longitude < -longitudeBound || longitude > longitudeBound || latitude < -latitudeBound || latitude > latitudeBound {
		return simpleError(invalidLongitudeLatitude)
	}

	content, _, _, err := c.db.getSortedSetEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}

	centerHash := encodeGeohash(longitude, latitude)
	response := make([][]byte, 0)
	for member, geohash := range content {
		distance := computeDistance(centerHash, geohash)
		if distance <= radius {
			response = append(response, bulkString(member, true))
		}
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

func computeDistance(hash1, hash2 float64) float64 {
	lon1, lat1 := decodeGeohash(hash1)
	lon2, lat2 := decodeGeohash(hash2)

	lon1r, lat1r := lon1*math.Pi/180, lat1*math.Pi/180
	lon2r, lat2r := lon2*math.Pi/180, lat2*math.Pi/180

	v := math.Sin((lon2r - lon1r) / 2)
	u := math.Sin((lat2r - lat1r) / 2)

	return 2 * earthRadiusInMeters * math.Asin(math.Sqrt(u*u+math.Cos(lat1r)*math.Cos(lat2r)*v*v))
}
