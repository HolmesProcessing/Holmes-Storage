package Status

import "log"

var (
	warning *log.Logger
	info    *log.Logger
	debug   *log.Logger
)

func InitLogging(_w, _i, _d *log.Logger) {
	warning = _w
	info = _i
	debug = _d
}
