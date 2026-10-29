// Package encoded mirrors go.uber.org/cadence/encoded: payload encoding.
package encoded

import "github.com/larsla/takt/internal/sdk"

type (
	// Value is an encoded value that can be decoded later.
	Value = sdk.Value
	// Values is a list of encoded values.
	Values = sdk.Values
	// DataConverter turns values into payloads and back.
	DataConverter = sdk.DataConverter
)

// GetDefaultDataConverter returns the JSON converter used by default.
func GetDefaultDataConverter() DataConverter { return sdk.GetDefaultDataConverter() }
