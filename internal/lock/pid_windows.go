//go:build windows

package lock

import "syscall"

const processQueryLimitedInformation = 0x1000

var (
	modKernel32                  = syscall.NewLazyDLL("kernel32.dll")
	procOpenProcess              = modKernel32.NewProc("OpenProcess")
	procCloseHandle              = modKernel32.NewProc("CloseHandle")
)

func pidAlive(pid int) (bool, error) {
	r1, _, _ := procOpenProcess.Call(uintptr(processQueryLimitedInformation), 0, uintptr(pid))
	if r1 == 0 {
		return false, nil
	}
	_, _, _ = procCloseHandle.Call(r1)
	return true, nil
}
