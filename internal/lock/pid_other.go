//go:build !unix && !windows

package lock

func pidAlive(pid int) (bool, error) {
	return false, nil
}
