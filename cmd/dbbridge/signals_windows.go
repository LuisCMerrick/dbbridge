package main

import "os"

// forceExitSignals are the signals that abort a drain. SIGTERM is not
// registered on Windows: the runtime delivers CTRL_CLOSE, CTRL_LOGOFF and
// CTRL_SHUTDOWN events as SIGTERM, and a service receives CTRL_LOGOFF_EVENT
// whenever any interactive user logs off.
var forceExitSignals = []os.Signal{os.Interrupt}
