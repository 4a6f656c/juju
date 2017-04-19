// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package status

import (
	"fmt"
	"reflect"

	"github.com/juju/utils"
)

// stringKeysFromMap takes a map with keys which are strings and returns
// only the keys.
func stringKeysFromMap(m interface{}) (keys []string) {
	for _, k := range reflect.ValueOf(m).MapKeys() {
		keys = append(keys, k.String())
	}
	return
}

// indent prepends a format string with the given number of spaces.
func indent(prepend string, level int, append string) string {
	return fmt.Sprintf("%s%*s%s", prepend, level, "", append)
}

// limitWidth limits a string to the specified width.
func limitWidth(s string, width int) string {
	if r := []rune(s); len(r) > width {
		const ellipsis = "..."
		return string(r[:width-len(ellipsis)]) + ellipsis
	}
	return s
}

// recurseUnits calls the given recurseMap function on the given unit
// and its subordinates (recursively defined on the given unit).
func recurseUnits(u unitStatus, il int, recurseMap func(string, unitStatus, int)) {
	if len(u.Subordinates) == 0 {
		return
	}
	for _, uName := range utils.SortStringsNaturally(stringKeysFromMap(u.Subordinates)) {
		unit := u.Subordinates[uName]
		recurseMap(uName, unit, il)
		recurseUnits(unit, il+1, recurseMap)
	}
}
