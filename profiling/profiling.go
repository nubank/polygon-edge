package profiling

import (
	"os"
	"runtime"

	"github.com/grafana/pyroscope-go"
)

type Profiler interface {
	Start() error
	Stop() error
}

type profiler struct {
	prof  *pyroscope.Profiler
	pConf pyroscope.Config
}

func NewProfiler(profilerServer, appName string, opts ...func(config *pyroscope.Config)) Profiler {
	// These 2 lines are only required if you're using mutex or block profiling
	// Read the explanation below for how to set these rates:
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)

	pConf := pyroscope.Config{
		ApplicationName: appName,

		// replace this with the address of pyroscope server
		ServerAddress: profilerServer,

		// you can disable logging by setting this to nil
		Logger: pyroscope.StandardLogger,

		// you can provide static tags via a map:
		Tags: map[string]string{"hostname": os.Getenv("HOSTNAME")},

		ProfileTypes: []pyroscope.ProfileType{
			// these profile types are enabled by default:
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,

			// these profile types are optional:
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount,
			pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount,
			pyroscope.ProfileBlockDuration,
		},
	}

	for _, f := range opts {
		f(&pConf)
	}

	return &profiler{
		pConf: pConf,
	}
}

// Start starts the profiler
func (p *profiler) Start() error {
	var err error
	p.prof, err = pyroscope.Start(p.pConf)

	return err
}

// Stop stops the profiler
func (p *profiler) Stop() error {
	return p.prof.Stop()
}

// WithTags merges user defined tags with default tags
func WithTags(tags map[string]string) func(config *pyroscope.Config) {
	return func(c *pyroscope.Config) {
		for k, v := range tags {
			c.Tags[k] = v
		}
	}
}
