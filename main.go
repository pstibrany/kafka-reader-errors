package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaOffsetStart is a special offset value that means the beginning of the partition.
const kafkaOffsetStart = int64(-2)

type KafkaConfig struct {
	Address       string
	Topic         string
	ClientID      string
	DialTimeout   time.Duration
	WriteTimeout  time.Duration
	ConsumerGroup string
	Partition     int
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+".address", "", "The Kafka backend address.")
	f.StringVar(&cfg.Topic, prefix+".topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+".client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+".dial-timeout", 2*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
	f.DurationVar(&cfg.WriteTimeout, prefix+".write-timeout", 10*time.Second, "How long to wait for an incoming write request to be successfully committed to the Kafka backend.")
	f.StringVar(&cfg.ConsumerGroup, prefix+".consumer-group", "", "The consumer group used by the consumer to track the last consumed offset. The consumer group must be different for each ingester. If the configured consumer group contains the '<partition>' placeholder, it will be replaced with the actual partition ID owned by the ingester. When empty (recommended), Mimir will use the ingester instance ID to guarantee uniqueness.")
	f.IntVar(&cfg.Partition, prefix+".partition", 0, "partition ID to read")
}

func main() {
	cfg := KafkaConfig{}
	cfg.RegisterFlagsWithPrefix("kafka", flag.CommandLine)
	flag.Parse()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	//logger = level.NewFilter(logger, level.AllowInfo())

	if cfg.Address == "" || cfg.Topic == "" {
		level.Error(logger).Log("msg", "please provide kafka address and topic")
		return
	}

	client, err := newKafkaReader(cfg, kafkaOffsetStart, logger)
	if err != nil {
		level.Error(logger).Log("msg", "failed to initialize Kafka client", "err", err)
		return
	}

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)

		numRecords := 0
		oldestTs := time.Now()

		fetches.EachRecord(func(record *kgo.Record) {
			numRecords++
			if record.Timestamp.Before(oldestTs) {
				oldestTs = record.Timestamp
			}
		})

		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			level.Error(logger).Log("msg", "fetch error", "err", err)
		})

		level.Info(logger).Log("msg", "fetched records", "numRecords", numRecords, "oldestTimestamp", oldestTs.UTC().Format(time.RFC3339))
	}
}

func newKafkaReader(cfg KafkaConfig, offset int64, logger log.Logger) (*kgo.Client, error) {
	const fetchMaxBytes = 100_000_000

	opts := []kgo.Opt{
		kgo.ClientID(cfg.ClientID),
		kgo.SeedBrokers(cfg.Address),
		kgo.DialTimeout(cfg.DialTimeout),

		kgo.MetadataMinAge(10 * time.Second),
		kgo.MetadataMaxAge(10 * time.Second),

		kgo.WithLogger(newKafkaLogger(logger)),

		kgo.RetryTimeoutFn(func(key int16) time.Duration {
			// 30s is the default timeout in the Kafka client.
			return 30 * time.Second
		}),

		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			cfg.Topic: {int32(cfg.Partition): kgo.NewOffset().At(offset)},
		}),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxWait(5 * time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),

		// BrokerMaxReadBytes sets the maximum response size that can be read from
		// Kafka. This is a safety measure to avoid OOMing on invalid responses.
		// franz-go recommendation is to set it 2x FetchMaxBytes.
		kgo.BrokerMaxReadBytes(2 * fetchMaxBytes),

		kgo.WithHooks(&errorLogger{logger: logger}),
	}

	return kgo.NewClient(opts...)
}

type errorLogger struct {
	logger log.Logger
}

func (el *errorLogger) OnBrokerE2E(meta kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {
	if e2e.ReadErr != nil {
		level.Error(el.logger).Log("msg", "read error", "node", meta.NodeID, "host", meta.Host, "port", meta.Port, "key", key, "err", e2e.ReadErr)
	}
}

type kafkaLogger struct {
	logger log.Logger
}

func newKafkaLogger(logger log.Logger) *kafkaLogger {
	return &kafkaLogger{
		logger: log.With(logger, "component", "kafka_client"),
	}
}

func (l *kafkaLogger) Level() kgo.LogLevel {
	return kgo.LogLevelDebug
}

func (l *kafkaLogger) Log(lev kgo.LogLevel, msg string, keyvals ...any) {
	keyvals = append([]any{"msg", msg}, keyvals...)
	switch lev {
	case kgo.LogLevelDebug:
		level.Debug(l.logger).Log(keyvals...)
	case kgo.LogLevelInfo:
		level.Info(l.logger).Log(keyvals...)
	case kgo.LogLevelWarn:
		level.Warn(l.logger).Log(keyvals...)
	case kgo.LogLevelError:
		level.Error(l.logger).Log(keyvals...)
	}
}
