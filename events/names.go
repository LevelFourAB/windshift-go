package events

// IsValidSubject checks if the subject is valid, either allowing or disallowing
// wildcards.
//
// NATS recommends ASCII characters for subjects, but does not enforce it. In
// Windshift subjects are limited to the characters `a`-`z`, `A`-`Z`, `0`-`9`,
// `_`, `-`, and `.`.
//
// If wildcards are allowed, then `*` and `>` are also allowed. `*` can be used
// to match a single token, and `>` can be used to match one or more tokens but
// only at the end of the subject.
//
// Period is a special character that indicates a subject hierarchy. We
// validate that the subject does not start or end with a period, and that
// periods are not adjacent.
//
// Empty subjects are not allowed.
func IsValidSubject(subject string, allowWildcards bool) bool {
	if len(subject) == 0 {
		return false
	}

	if subject[0] == '.' || subject[len(subject)-1] == '.' {
		return false
	}

	for i, c := range subject {
		if c >= 'a' && c <= 'z' {
			continue
		}

		if c >= 'A' && c <= 'Z' {
			continue
		}

		if c >= '0' && c <= '9' {
			continue
		}

		if c == '_' || c == '-' {
			continue
		}

		if c == '.' && subject[i-1] != '.' {
			continue
		}

		if allowWildcards && (c == '*' || c == '>') {
			if i == 0 || subject[i-1] != '.' {
				// Check that the previous character is a period
				return false
			} else if c == '>' && i != len(subject)-1 {
				// Check that the > is at the end of the subject
				return false
			}

			continue
		}

		return false
	}

	return true
}

// IsValidStreamName checks if the stream name is valid.
//
// NATS provides the guidance that stream names cannot contain whitespace, `.`,
// `*`, `>“, path separators (forward or backwards slash), and non-printable
// characters.
//
// In Windshift stream names are aligned with subject names and allow only
// the characters `a`-`z`, `A`-`Z`, `0`-`9`, `_`, and `-`.
//
// Empty stream names are not allowed.
func IsValidStreamName(name string) bool {
	return IsValidConsumerName(name)
}

// IsValidConsumerName checks if the consumer name is valid.
//
// NATS provides the guidance that durable names of consumers cannot contain
// whitespace, `.`, `*`, `>`, path separators (forward or backwards slash), and
// non-printable characters.
//
// In Windshift consumer names are aligned with subject names and allow only
// the characters `a`-`z`, `A`-`Z`, `0`-`9`, `_`, and `-`.
//
// Empty consumer names are not allowed.
func IsValidConsumerName(name string) bool {
	if len(name) == 0 {
		return false
	}

	for _, c := range name {
		if c >= 'a' && c <= 'z' {
			continue
		}

		if c >= 'A' && c <= 'Z' {
			continue
		}

		if c >= '0' && c <= '9' {
			continue
		}

		if c == '_' || c == '-' {
			continue
		}

		return false
	}

	return true
}
