package opdefs

/*
	Sole purpose of this package is to import all the operators such that if some binary
	imports the package opdefs, all operators get imported too.
*/

import (
	_ "fennel/opdefs/profile"
	_ "fennel/opdefs/std"
	_ "fennel/opdefs/time"
)
