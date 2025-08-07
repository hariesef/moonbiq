package helper

import (
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
)

func NormalizeTableName(tablePath string) string {
	// Extract filename without extension
	base := filepath.Base(tablePath)
	name := strings.TrimSuffix(base, filepath.Ext(base))

	// Replace all invalid characters with underscore
	reg := regexp.MustCompile(`[^a-zA-Z0-9_]+`)
	normalized := reg.ReplaceAllString(name, "_")

	// Convert to lowercase
	normalized = strings.ToLower(normalized)

	// Ensure name starts with letter/underscore and isn't empty
	if normalized == "" {
		return "" //indicates invalid table name
	}
	if !unicode.IsLetter(rune(normalized[0])) && normalized[0] != '_' {
		normalized = "t_" + normalized
	}

	return normalized
}

func IsNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "notFound") ||
		strings.Contains(err.Error(), "Not found") ||
		strings.Contains(err.Error(), "does not exist")
}
