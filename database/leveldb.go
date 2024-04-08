package database

import (
	"fmt"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"os"
	"strconv"
	"syscall"
	"time"
)

const (
	cacheSizeMb                = "LEVELDB_CACHE_SIZE_MB"
	forceFileDescriptorDivisor = "LEVELDB_FORCE_FILE_DESCRIPTOR_DIVISOR"
)

func getEnvInt(name string) (int, error) {
	return strconv.Atoi(os.Getenv(name))
}

func raiseAndReturnFDLimit() (uint64, error) {
	var limit syscall.Rlimit

	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return 0, err
	}

	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return 0, err
	}

	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return 0, err
	}
	return limit.Cur, nil
}

func NewLevelDB(path string, name string, logger hclog.Logger) (db *leveldb.DB, err error) {
	opts := &opt.Options{}

	if value, err := getEnvInt(forceFileDescriptorDivisor); err == nil {
		limit, err := raiseAndReturnFDLimit()

		if err != nil {
			return nil, err
		}

		openFilesCacheCapacity := int(limit) / value
		logger.Info("forcing FD limit",
			"limit", strconv.FormatUint(limit, 10),
			"leveldbCache", strconv.Itoa(openFilesCacheCapacity),
		)
		opts.OpenFilesCacheCapacity = openFilesCacheCapacity
	}

	// Instantiate the bloom filter to 10 bits
	opts.Filter = filter.NewBloomFilter(10)

	// Disabling seeks compaction
	opts.DisableSeeksCompaction = true

	if value, err := getEnvInt(cacheSizeMb); err == nil {
		blockCacheCapacity := value / 2
		opts.BlockCacheCapacity = blockCacheCapacity * opt.MiB

		writeBuffer := value / 4
		opts.WriteBuffer = writeBuffer * opt.MiB

		logger.Info("setting leveldb cache size", "writeBuffer", strconv.Itoa(writeBuffer), "blockCacheCapacity", strconv.Itoa(blockCacheCapacity))
	}

	db, err = leveldb.OpenFile(path, opts)
	if err != nil {
		return
	}

	go meter(db, time.Second*5, name, logger)

	return
}

func meter(db *leveldb.DB, refresh time.Duration, name string, logger hclog.Logger) {
	setGauge := func(metricName string, value int64) {
		metrics.SetGauge([]string{"leveldb", name, metricName}, float32(value))
	}

	logger.Info("started metering")

	// Create the counters to store current and previous compaction values
	compactions := make([][]int64, 2)
	for i := 0; i < 2; i++ {
		compactions[i] = make([]int64, 4)
	}
	// Create storages for states and warning log tracer.
	var (
		stats           leveldb.DBStats
		iostats         [2]int64
		delaystats      [2]int64
		lastWritePaused time.Time
	)
	timer := time.NewTimer(refresh)
	defer timer.Stop()

	// Iterate ad infinitum and collect the stats
	i := 1
	for {
		// Retrieve the database stats
		// Stats method resets buffers inside therefore it's okay to just pass the struct.
		err := db.Stats(&stats)
		if err != nil {
			logger.Error("failed to read database stats", "err", err.Error())
			break
		}
		// Iterate over all the leveldbTable rows, and accumulate the entries
		for j := 0; j < len(compactions[i%2]); j++ {
			compactions[i%2][j] = 0
		}
		compactions[i%2][0] = stats.LevelSizes.Sum()
		for _, t := range stats.LevelDurations {
			compactions[i%2][1] += t.Nanoseconds()
		}
		compactions[i%2][2] = stats.LevelRead.Sum()
		compactions[i%2][3] = stats.LevelWrite.Sum()

		// Update all the requested meters
		if i > 1 {
			setGauge("diskSize", compactions[i%2][0])
			setGauge("compactTime", compactions[i%2][1]-compactions[(i-1)%2][1])
			setGauge("compactRead", compactions[i%2][2]-compactions[(i-1)%2][2])
			setGauge("compactWrite", compactions[i%2][3]-compactions[(i-1)%2][3])
		}

		var (
			delayN   = int64(stats.WriteDelayCount)
			duration = stats.WriteDelayDuration
			paused   = stats.WritePaused
		)

		setGauge("writeDelayNMeter", delayN-delaystats[0])
		setGauge("writeDelayMeter", duration.Nanoseconds()-delaystats[1])

		// If a warning that db is performing compaction has been displayed, any subsequent
		// warnings will be withheld for one minute not to overwhelm the user.
		if paused && delayN-delaystats[0] == 0 && duration.Nanoseconds()-delaystats[1] == 0 &&
			time.Now().After(lastWritePaused.Add(time.Minute)) {
			logger.Warn("database compacting, degraded performance")
			lastWritePaused = time.Now()
		}
		delaystats[0], delaystats[1] = delayN, duration.Nanoseconds()

		var (
			nRead  = int64(stats.IORead)
			nWrite = int64(stats.IOWrite)
		)
		setGauge("diskRead", nRead-iostats[0])
		setGauge("diskWrite", nWrite-iostats[1])
		iostats[0], iostats[1] = nRead, nWrite

		setGauge("memCompaction", int64(stats.MemComp))
		setGauge("level0Compaction", int64(stats.Level0Comp))
		setGauge("nonlevel0Compaction", int64(stats.NonLevel0Comp))
		setGauge("seekCompaction", int64(stats.SeekComp))

		for i, tables := range stats.LevelTablesCounts {
			setGauge(fmt.Sprintf("level_%d_tableCount", i), int64(tables))
		}

		i += 1

		// Sleep a bit, then repeat the stats collection
		select {
		case <-timer.C:
			timer.Reset(refresh)
		}
	}
}
