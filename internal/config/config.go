package config

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime configuration for the API service.
type Config struct {
	ListenAddr            string
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	ShutdownTimeout       time.Duration
	DefaultRunningLimit   int
	DefaultCustomerReport string

	DBEnabled         bool
	DBHost            string
	DBPort            int
	DBUser            string
	DBPassword        string
	DBName            string
	DBConnTimeout     time.Duration
	DBQueryTimeout    time.Duration
	RunningStuckAfter time.Duration

	CustomerMapSQLitePath string

	SSDBEnabled      bool
	SSDBHost         string
	SSDBPort         int
	SSDBUser         string
	SSDBPassword     string
	SSDBName         string
	SSDBConnTimeout  time.Duration
	SSDBQueryTimeout time.Duration

	PromEnabled          bool
	PromTargets          []string
	PromMatchPrefix      string
	PromScrapeTimeout    time.Duration
	PromScrapeInterval   time.Duration
	PromHistoryMaxPoints int

	ESEnabled     bool
	ESEndpoint    string
	ESTimeout     time.Duration
	ESLookupLimit int
	ESAIPIndex    string
	ESAIPPageSize int

	RiskUnknownHotRate         float64
	RiskUnknownHotAbs          int
	RiskMissingIDsHotRate      float64
	RiskMissingIDsHotAbs       int
	RiskMissingCreatedHotRate  float64
	RiskMissingCreatedHotAbs   int
	RiskExtMismatchHotRate     float64
	RiskExtMismatchHotAbs      int
	RiskDupFilesHotRate        float64
	RiskDupFilesHotAbs         int
	RiskDupGroupsHotAbs        int
	RiskIndexLagP95HotSec      int
	RiskMinDiversityRatio      float64
	RiskTinyFilesMax           int
	RiskMinUniqueFormats       int
	RiskMinUniqueFormatsForTiny int
}

// FromEnv loads configuration from environment variables with sensible defaults.
func FromEnv() Config {
	loadConfigDefaultsFromFile()
	loadSecretsDefaultsFromFile()

	return Config{
		ListenAddr:            getEnv("APP_LISTEN_ADDR", ":8080"),
		ReadTimeout:           time.Duration(getEnvInt("APP_READ_TIMEOUT_SEC", 10)) * time.Second,
		WriteTimeout:          time.Duration(getEnvInt("APP_WRITE_TIMEOUT_SEC", 20)) * time.Second,
		ShutdownTimeout:       time.Duration(getEnvInt("APP_SHUTDOWN_TIMEOUT_SEC", 10)) * time.Second,
		DefaultRunningLimit:   getEnvInt("APP_DEFAULT_RUNNING_LIMIT", 50),
		DefaultCustomerReport: getEnv("APP_DEFAULT_CUSTOMER_ID", "default"),
		DBEnabled:             getEnvBool("APP_DB_ENABLED", false),
		DBHost:                getEnv("APP_DB_HOST", "127.0.0.1"),
		DBPort:                getEnvInt("APP_DB_PORT", 62001),
		DBUser:                getEnv("APP_DB_USER", "archivematica"),
		DBPassword:            getEnv("APP_DB_PASSWORD", "demo"),
		DBName:                getEnv("APP_DB_NAME", "MCP"),
		DBConnTimeout:         time.Duration(getEnvInt("APP_DB_CONN_TIMEOUT_SEC", 5)) * time.Second,
		DBQueryTimeout:        time.Duration(getEnvInt("APP_DB_QUERY_TIMEOUT_SEC", 10)) * time.Second,
		RunningStuckAfter:     time.Duration(getEnvInt("APP_RUNNING_STUCK_MINUTES", 30)) * time.Minute,
		CustomerMapSQLitePath: getEnv("APP_CUSTOMER_MAP_SQLITE_PATH", ""),
		SSDBEnabled:           getEnvBool("APP_SS_DB_ENABLED", false),
		SSDBHost:              getEnv("APP_SS_DB_HOST", "127.0.0.1"),
		SSDBPort:              getEnvInt("APP_SS_DB_PORT", 62001),
		SSDBUser:              getEnv("APP_SS_DB_USER", "archivematica"),
		SSDBPassword:          getEnv("APP_SS_DB_PASSWORD", "demo"),
		SSDBName:              getEnv("APP_SS_DB_NAME", "SS"),
		SSDBConnTimeout:       time.Duration(getEnvInt("APP_SS_DB_CONN_TIMEOUT_SEC", 5)) * time.Second,
		SSDBQueryTimeout:      time.Duration(getEnvInt("APP_SS_DB_QUERY_TIMEOUT_SEC", 10)) * time.Second,
		PromEnabled:           getEnvBool("APP_PROM_ENABLED", false),
		PromTargets:           getEnvList("APP_PROM_TARGETS", []string{"http://127.0.0.1:7999/metrics"}),
		PromMatchPrefix:       getEnv("APP_PROM_MATCH_PREFIX", "archivematica_"),
		PromScrapeTimeout:     time.Duration(getEnvInt("APP_PROM_SCRAPE_TIMEOUT_SEC", 5)) * time.Second,
		PromScrapeInterval:    time.Duration(getEnvInt("APP_PROM_SCRAPE_INTERVAL_SEC", 15)) * time.Second,
		PromHistoryMaxPoints:  getEnvInt("APP_PROM_HISTORY_MAX_POINTS", 720),
		ESEnabled:             getEnvBool("APP_ES_ENABLED", false),
		ESEndpoint:            getEnv("APP_ES_ENDPOINT", "http://127.0.0.1:62002"),
		ESTimeout:             time.Duration(getEnvInt("APP_ES_TIMEOUT_SEC", 5)) * time.Second,
		ESLookupLimit:         getEnvInt("APP_ES_LOOKUP_LIMIT", 5),
		ESAIPIndex:            getEnv("APP_ES_AIP_INDEX", "aipfiles"),
		ESAIPPageSize:         getEnvInt("APP_ES_AIP_PAGE_SIZE", 500),
		RiskUnknownHotRate:         getEnvFloat("APP_RISK_UNKNOWN_HOT_RATE", 0.01),
		RiskUnknownHotAbs:          getEnvInt("APP_RISK_UNKNOWN_HOT_ABS", 5),
		RiskMissingIDsHotRate:      getEnvFloat("APP_RISK_MISSING_IDS_HOT_RATE", 0.10),
		RiskMissingIDsHotAbs:       getEnvInt("APP_RISK_MISSING_IDS_HOT_ABS", 20),
		RiskMissingCreatedHotRate:  getEnvFloat("APP_RISK_MISSING_CREATED_HOT_RATE", 0.10),
		RiskMissingCreatedHotAbs:   getEnvInt("APP_RISK_MISSING_CREATED_HOT_ABS", 20),
		RiskExtMismatchHotRate:     getEnvFloat("APP_RISK_EXT_MISMATCH_HOT_RATE", 0.02),
		RiskExtMismatchHotAbs:      getEnvInt("APP_RISK_EXT_MISMATCH_HOT_ABS", 10),
		RiskDupFilesHotRate:        getEnvFloat("APP_RISK_DUP_FILES_HOT_RATE", 0.20),
		RiskDupFilesHotAbs:         getEnvInt("APP_RISK_DUP_FILES_HOT_ABS", 50),
		RiskDupGroupsHotAbs:        getEnvInt("APP_RISK_DUP_GROUPS_HOT_ABS", 20),
		RiskIndexLagP95HotSec:      getEnvInt("APP_RISK_INDEX_LAG_P95_HOT_SEC", 1800),
		RiskMinDiversityRatio:      getEnvFloat("APP_RISK_MIN_DIVERSITY_RATIO", 0.02),
		RiskTinyFilesMax:           getEnvInt("APP_RISK_TINY_FILES_MAX", 20),
		RiskMinUniqueFormats:       getEnvInt("APP_RISK_MIN_UNIQUE_FORMATS", 2),
		RiskMinUniqueFormatsForTiny: getEnvInt("APP_RISK_MIN_UNIQUE_FORMATS_TINY", 1),
	}
}

func loadConfigDefaultsFromFile() {
	bootstrapCandidates := []string{
		"./am-ops-observer.env",
		"./am-realtime-report-ui.env",
		"/etc/default/am-ops-observer",
		"/etc/default/am-realtime-report-ui",
	}

	for _, candidate := range bootstrapCandidates {
		abs := candidate
		if !filepath.IsAbs(candidate) {
			if wd, err := os.Getwd(); err == nil {
				abs = filepath.Join(wd, candidate)
			}
		}
		_ = applyEnvDefaultsFromFile(abs)
	}

	candidates := make([]string, 0, 2)
	if explicit := strings.TrimSpace(os.Getenv("APP_CONFIG_FILE")); explicit != "" {
		candidates = append(candidates, explicit)
	}
	candidates = append(candidates, "/etc/am-ops-observer/config.env")

	for _, candidate := range candidates {
		abs := candidate
		if !filepath.IsAbs(candidate) {
			if wd, err := os.Getwd(); err == nil {
				abs = filepath.Join(wd, candidate)
			}
		}

		if err := applyEnvDefaultsFromFile(abs); err == nil {
			return
		}
	}
}

func loadSecretsDefaultsFromFile() {
	candidates := make([]string, 0, 3)
	if explicit := strings.TrimSpace(os.Getenv("APP_SECRETS_FILE")); explicit != "" {
		candidates = append(candidates, explicit)
	}
	if credDir := strings.TrimSpace(os.Getenv("CREDENTIALS_DIRECTORY")); credDir != "" {
		credName := strings.TrimSpace(os.Getenv("APP_SECRETS_CREDENTIAL_NAME"))
		if credName == "" {
			credName = "app-secrets"
		}
		candidates = append(candidates, filepath.Join(credDir, credName))
	}
	candidates = append(candidates, "/etc/am-ops-observer/secrets.env")
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if err := applyEnvDefaultsFromFile(candidate); err == nil {
			return
		}
	}
}

func applyEnvDefaultsFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			continue
		}

		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		if key == "" {
			continue
		}

		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}

		if os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}

	return scanner.Err()
}

// MySQLDSN returns a mysql driver DSN with safe defaults for TCP access.
func (c Config) MySQLDSN() string {
	params := url.Values{}
	params.Set("parseTime", "true")
	params.Set("timeout", c.DBConnTimeout.String())
	params.Set("readTimeout", c.DBQueryTimeout.String())
	params.Set("writeTimeout", c.DBQueryTimeout.String())
	params.Set("charset", "utf8mb4")
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName, params.Encode())
}

// SSMySQLDSN returns a mysql DSN for Storage Service DB access.
func (c Config) SSMySQLDSN() string {
	params := url.Values{}
	params.Set("parseTime", "true")
	params.Set("timeout", c.SSDBConnTimeout.String())
	params.Set("readTimeout", c.SSDBQueryTimeout.String())
	params.Set("writeTimeout", c.SSDBQueryTimeout.String())
	params.Set("charset", "utf8mb4")
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", c.SSDBUser, c.SSDBPassword, c.SSDBHost, c.SSDBPort, c.SSDBName, params.Encode())
}

func getEnv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func getEnvInt(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return parsed
}

func getEnvFloat(key string, def float64) float64 {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	parsed, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return def
	}
	return parsed
}

func getEnvBool(key string, def bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return def
	}
	return parsed
}

func getEnvList(key string, def []string) []string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		out := make([]string, 0, len(def))
		for _, d := range def {
			d = strings.TrimSpace(d)
			if d != "" {
				out = append(out, d)
			}
		}
		return out
	}

	parts := strings.Split(val, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
