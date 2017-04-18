package Status

import ()

// Data structures containing information about all planners and their services
// on a single machine, identified by its uuid.
// Planners are identified by the process ID that they inhabit. This way we can
// also keep track of planners that died and potentially restarted using a
// different PID shortly after (logs could potentially indicate this too).
// Similarily the services that accompany a planner are reported as children of
// that very planner, mapped by their respective port numbers.

// type SystemStatus struct {
//   Uptime int64
//
//   CpuIOWait uint64
//   CpuIdle   uint64
//   CpuBusy   uint64
//   CpuTotal  uint64
//
//   MemoryUsage uint64
//   MemoryMax   uint64
//   SwapUsage   uint64
//   SwapMax     uint64
//
//   Harddrives []*Harddrive
//
//   Loads1  float64 // System load as reported by sysinfo syscall
//   Loads5  float64
//   Loads15 float64
// }
//
// type Harddrive struct {
//   FsType     string
//   MountPoint string
//   Used       uint64
//   Total      uint64
//   Free       uint64
// }

// type NetworkStatus struct {
//   Interfaces []*NetworkInterface
// }
//
// type NetworkInterface struct {
//   ID        int
//   Name      string
//   IP        net.IP
//   Netmask   net.IPMask
//   Broadcast net.IP
//   Scope     string
// }
