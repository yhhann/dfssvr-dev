package conf

// Create two feature flags to work together with back store files.

const (
	// Flag to enable/disable back store.
	FlagKeyBackStore = "backstore"

	// Flag to enable/disable read from back store.
	FlagKeyReadFromBackStore = "read_from_backstore"

	// Flag to enable/disable cache file when write error.
	FlagKeyCacheFile = "cachefile"
)

func init() {
	features = make(map[string]*FeatureFlag)

	initBackStoreFlag()
	initCacheFileFlag()
	initTeeFlag()
}

func initBackStoreFlag() {
	PutFlag(&FeatureFlag{
		Key:        FlagKeyBackStore,
		Enabled:    false,
		Domains:    []uint32{},
		Groups:     []string{},
		Percentage: uint32(0),
	})

	PutFlag(&FeatureFlag{
		Key:        FlagKeyReadFromBackStore,
		Enabled:    false,
		Domains:    []uint32{},
		Groups:     []string{},
		Percentage: uint32(0),
	})
}

func initCacheFileFlag() {
	PutFlag(&FeatureFlag{
		Key:        FlagKeyCacheFile,
		Enabled:    false,
		Domains:    []uint32{},
		Groups:     []string{},
		Percentage: uint32(0),
	})
}
