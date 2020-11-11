package version

var (
	Build  = ""
	GitSHA = ""
)

func FullVersion() string {
	version := Build
	if Build == "" {
		version = "dev"
	}
	return version + "@" + GitSHA
}
