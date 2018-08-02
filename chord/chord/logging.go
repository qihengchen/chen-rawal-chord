/*
 *  Brown University, CS138, Spring 2017
 *
 *  Purpose: Setup loggers that print various kinds of output (general output,
 *  errors, or debugging info.)
 */

package chord

import (
	"io/ioutil"
	"log"
	"os"
)

var Debug *log.Logger
var Out *log.Logger
var Error *log.Logger

// Initialize the loggers
func init() {
	Debug = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	Out = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	Error = log.New(os.Stdout, "ERROR: ", log.Ltime|log.Lshortfile)
}

// Turn debug on or off
func SetDebug(enabled bool) {
	if enabled {
		Debug = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		Debug = log.New(ioutil.Discard, "", log.Ldate|log.Ltime|log.Lshortfile)
	}
}
