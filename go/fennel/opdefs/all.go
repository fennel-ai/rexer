package opdefs

/*
	Sole purpose of this package is to import all the operators such that if some binary
	imports the package opdefs, all operators get imported too.
*/

import (
	_ "fennel/opdefs/aggregate"
	_ "fennel/opdefs/feature"

	_ "fennel/opdefs/std"
	_ "fennel/opdefs/std/bool"
	_ "fennel/opdefs/std/dedup"
	_ "fennel/opdefs/std/group"
	_ "fennel/opdefs/std/map"
	_ "fennel/opdefs/std/number"
	_ "fennel/opdefs/std/predict"
	_ "fennel/opdefs/std/profile"
	_ "fennel/opdefs/std/set"
)
