package conf

import (
	"fmt"
	"sync"

	"jingoal.com/dfs/instrument"
)

var (
	features map[string]*FeatureFlag
	flock    sync.RWMutex
)

// PutFlag puts a feature flag into configuration context.
func PutFlag(f *FeatureFlag) error {
	flock.Lock()
	defer flock.Unlock()

	if _, ok := features[f.Key]; ok {
		return fmt.Errorf("feature already existed")
	}
	features[f.Key] = f

	instrument.FlagGauge <- flagToMeasure(f)

	return nil
}

// GetFlag gets a feature flag from configuration context.
func GetFlag(key string) (*FeatureFlag, error) {
	flock.RLock()
	defer flock.RUnlock()

	f, ok := features[key]
	if !ok {
		return nil, fmt.Errorf("feature not exist")
	}

	return f, nil
}

// UpdateFlag updates an existed feature flag.
// for example:
// set/create /shard/conf/dfs.svr.featureflag.foo
//  {"key":"foo","enabled":false,"percentage":100,"domains":[1,2,3],
//  "groups":["a","b","c"]}
func UpdateFlag(f *FeatureFlag) error {
	flock.Lock()
	defer flock.Unlock()

	if _, ok := features[f.Key]; !ok {
		return fmt.Errorf("feature not exist")
	}
	features[f.Key] = f

	instrument.FlagGauge <- flagToMeasure(f)

	return nil
}

func flagToMeasure(f *FeatureFlag) *instrument.Measurements {
	v := uint32(1e9)
	v += f.Percentage * 1e7

	if len(f.Domains) > 0 {
		v += f.Domains[0]
	}

	vf := float64(v)
	if !f.Enabled {
		vf = vf * -1
	}

	return &instrument.Measurements{
		Name:  f.Key,
		Value: vf,
	}
}
