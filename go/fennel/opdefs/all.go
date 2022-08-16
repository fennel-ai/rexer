package opdefs

/*
	Sole purpose of this package is to import all the operators such that if some binary
	imports the package opdefs, all operators get imported too.
*/

import (
	_ "fennel/opdefs/aggregate"
	_ "fennel/opdefs/feature"

	_ "fennel/opdefs/embedding"

	_ "fennel/opdefs/model"

	_ "fennel/opdefs/std"
	_ "fennel/opdefs/std/bool"
	_ "fennel/opdefs/std/dedup"
	_ "fennel/opdefs/std/group"
	_ "fennel/opdefs/std/map"
	_ "fennel/opdefs/std/profile"
	_ "fennel/opdefs/std/rename"
	_ "fennel/opdefs/std/repeat"
	_ "fennel/opdefs/std/set"
	_ "fennel/opdefs/std/zip"

	_ "fennel/opdefs/remote"

	_ "fennel/opdefs/math"
)
