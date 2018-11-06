package conf

import (
	"github.com/golang/glog"
)

// Create two feature flags to work together with tee handler.

const (
	// Flag to enable/disable write to minor.
	flagKeyWriteToMinor = "tee_write_to_minor"

	// Flag to enable/disable read from minor.
	flagKeyReadFromMinor = "tee_read_from_minor"
)

func initTeeFlag() {

	PutFlag(&FeatureFlag{
		Key: flagKeyWriteToMinor,
		// TODO(hanyh)
		Enabled:    false,
		Domains:    []uint32{},
		Groups:     []string{},
		Percentage: uint32(0),
	})

	PutFlag(&FeatureFlag{
		Key: flagKeyReadFromMinor,
		// TODO(hanyh)
		Enabled:    false,
		Domains:    []uint32{},
		Groups:     []string{},
		Percentage: uint32(0),
	})
}

func IsMinorWriteOk(domain int64) bool {
	ff, err := GetFlag(flagKeyWriteToMinor)
	if err != nil {
		glog.Warningf("feature %s error %v", flagKeyWriteToMinor, err)
		return false
	}

	return ff.DomainHasAccess(uint32(domain))
}

func IsMinorReadOk(domain int64) bool {
	ff, err := GetFlag(flagKeyReadFromMinor)
	if err != nil {
		glog.Warningf("feature %s error %v", flagKeyReadFromMinor, err)
		return false
	}

	return ff.DomainHasAccess(uint32(domain))
}
