package harmonyquery

import (
	"context"
	"embed"
	"fmt"
	"maps"
	"math/rand"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/yugabyte/pgx/v5"
	"github.com/yugabyte/pgx/v5/pgconn"
	"github.com/yugabyte/pgx/v5/pgxpool"
	"golang.org/x/xerrors"
)

type ITestID string

// ITestNewID see ITestWithID doc
func ITestNewID() ITestID {
	return ITestID(strconv.Itoa(rand.Intn(99999)))
}

type DB struct {
	pgx              *pgxpool.Pool
	cfg              *pgxpool.Config
	schema           string
	itestBaseDB      string     // when set, ITestDeleteAll uses DROP DATABASE
	itestConn        connParams // when itestBaseDB set, used to connect back for DROP
	hostnames        []string
	BTFPOnce         sync.Once
	BTFP             uintptr // A PC only in your stack when you call BeginTransaction()
	sqlEmbedFS       embed.FS
	downgradeEmbedFS embed.FS
}

var logger = logging.Logger("harmonyquery")

type Config struct {
	// HOSTS is a list of hostnames to nodes running YugabyteDB
	// in a cluster. Only 1 is required
	Hosts []string

	// The Yugabyte server's username with full credentials to operate on Lotus' Database. Blank for default.
	Username string

	// The password for the related username. Blank for default.
	Password string

	// The database (logical partition) within Yugabyte. Blank for default.
	Database string

	// The port to find Yugabyte. Blank for default.
	Port string

	// Load Balance the connection over multiple nodes
	LoadBalance bool

	// SSL Mode for the connection
	SSLMode string

	// Schema to use for the connection
	Schema string

	// ApplicationName is sent to PostgreSQL as the application_name connection parameter,
	// visible in pg_stat_activity. Defaults to the binary name (os.Args[0]).
	ApplicationName string

	SqlEmbedFS *embed.FS

	DowngradeEmbedFS *embed.FS

	ITestID ITestID

	*PoolConfig // Set all or nothing. We use every value.

	UseTemplate      bool
	RecreateTemplate bool
}

type PoolConfig struct {
	MaxConnections        int
	MinConnections        int
	MaxConnectionLifetime time.Duration
	MaxIdleTime           time.Duration
}

func envElse(env, els string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return els
}

var DefaultHostEnv = "HARMONYQUERY_HOSTS"

func NewFromConfigWithITestID(t *testing.T, id ITestID) (*DB, error) {
	logger.Infof("%s: %s", DefaultHostEnv, os.Getenv(DefaultHostEnv))
	db, err := New(
		[]string{envElse(DefaultHostEnv, "127.0.0.1")},
		"yugabyte",
		"yugabyte",
		"yugabyte",
		"5433",
		false,
		id,
	)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		db.ITestDeleteAll()
	})
	return db, nil
}

var DefaultSchema = "curio"

const itestTemplateDatabase = "itest_template"

var (
	itestMutex        sync.Mutex
	itestTemplateOnce sync.Once
	itestTemplateErr  error
)

// connParams holds connection parameters for building conn strings and one-off connections.
type connParams struct {
	username string
	password string
	host     string // host:port or host1:port,host2:port
	connArgs string
}

func buildConnParams(options Config, hosts []string, port string) connParams {
	var host string
	if options.LoadBalance {
		pairs := make([]string, len(hosts))
		for i, h := range hosts {
			pairs[i] = fmt.Sprintf("%s:%s", h, port)
		}
		host = strings.Join(pairs, ",")
	} else {
		host = fmt.Sprintf("%s:%s", hosts[0], port)
	}
	args := []string{"application_name=" + url.QueryEscape(options.ApplicationName)}
	if options.SSLMode == "" {
		args = append(args, "sslmode=disable")
	} else {
		args = append(args, "sslmode="+options.SSLMode)
	}
	if options.LoadBalance {
		args = append(args, "load_balance=true")
	} else {
		args = append(args, "load_balance=false", "fallback_to_topology_keys_only=true")
	}
	return connParams{username: options.Username, password: options.Password, host: host, connArgs: strings.Join(args, "&")}
}

// connString returns a connection string with the real password, suitable for pgx parsing.
func (c connParams) connString(database string) string {
	return fmt.Sprintf("postgresql://%s:%s@%s/%s?%s", url.QueryEscape(c.username), url.QueryEscape(c.password), c.host, database, c.connArgs)
}

// safeString returns a connection string with the password masked, suitable for logging.
func (c connParams) safeString(database string) string {
	return fmt.Sprintf("postgresql://%s:%s@%s/%s?%s", url.QueryEscape(c.username), "********", c.host, database, c.connArgs)
}

func (c connParams) runWithConn(database string, timeout time.Duration, fn func(*pgx.Conn) error) error {
	ctx, cncl := context.WithDeadline(context.Background(), time.Now().Add(timeout))
	defer cncl()
	p, err := pgx.Connect(ctx, c.connString(database))
	if err != nil {
		return err
	}
	defer func() { _ = p.Close(context.Background()) }()
	return fn(p)
}

// New is to be called once per binary to establish the pool.
// log() is for errors. It returns an upgraded database's connection.
// This entry point serves both production and integration tests, so it's more DI.
func New(hosts []string, username, password, database, port string, loadBalance bool, itestID ITestID) (*DB, error) {
	return NewFromConfig(Config{
		Hosts:       hosts,
		Username:    username,
		Password:    password,
		Database:    database,
		Port:        port,
		LoadBalance: loadBalance,
		ITestID:     itestID,
	})
}

func NewFromConfig(options Config) (*DB, error) {
	// Upgrade from New() limitations.
	if options.Schema == "" {
		options.Schema = DefaultSchema
	}

	if options.ApplicationName == "" {
		options.ApplicationName = filepath.Base(os.Args[0])
	}

	hosts, database, port, loadBalance := options.Hosts, options.Database, options.Port, options.LoadBalance
	itest := string(options.ITestID)

	if len(hosts) == 0 {
		return nil, xerrors.Errorf("no hosts provided")
	}

	// Debug: Log which path we're taking
	logger.Infof("Yugabyte connection config: loadBalance=%v, hosts=%v, port=%s", loadBalance, hosts, port)

	conn := buildConnParams(options, hosts, port)
	connString := conn.connString(database)

	itestBaseDB := ""
	if itest != "" {
		itestMutex.Lock()
		defer itestMutex.Unlock()

		itestDB := "itest_" + itest

		if options.UseTemplate {
			if err := ensureTemplateDatabase(conn, database, options); err != nil {
				return nil, xerrors.Errorf("ensure itest template database: %w", err)
			}
			if !schemaRE.MatchString(itestDB) {
				return nil, xerrors.Errorf("invalid itest database name: %q", itestDB)
			}
			if err := conn.runWithConn(database, 30*time.Second, func(p *pgx.Conn) error {
				_, err := p.Exec(context.Background(), "CREATE DATABASE "+itestDB+" TEMPLATE "+itestTemplateDatabase)
				return err
			}); err != nil {
				return nil, xerrors.Errorf("create itest database: %w", err)
			}

			options.Database = itestDB
			options.Schema = "public"
			connString = conn.connString(itestDB)
			itestBaseDB = database
		} else {
			options.Schema = "itest_" + itest
			if err := ensureSchemaExists(conn, options.Schema, database); err != nil {
				return nil, err
			}
			connString = conn.connString(database)
		}
	} else if err := ensureSchemaExists(conn, options.Schema, database); err != nil {
		return nil, err
	}

	cfg, err := buildPoolConfig(connString, options, hosts, port)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.OnNotice = func(conn *pgconn.PgConn, n *pgconn.Notice) {
		logger.Debug("database notice: " + n.Message + ": " + n.Detail)
		DBMeasures.Errors.M(1)
	}

	db := DB{
		cfg:              cfg,
		schema:           options.Schema,
		itestBaseDB:      itestBaseDB,
		itestConn:        conn,
		hostnames:        hosts,
		sqlEmbedFS:       *options.SqlEmbedFS,
		downgradeEmbedFS: *options.DowngradeEmbedFS} // pgx populated in AddStatsAndConnect
	if err := db.addStatsAndConnect(); err != nil {
		return nil, err
	}

	if err = db.upgrade(); err != nil {
		return nil, err
	}

	return &db, db.setBTFP()
}

// called by New(). Not thread-safe. Exposed for testing.
func (db *DB) setBTFP() error {
	_, err := db.BeginTransaction(context.Background(), func(tx *Tx) (bool, error) {
		fp := make([]uintptr, 20)
		runtime.Callers(2, fp)
		db.BTFP = fp[0]
		return false, nil
	})
	return err
}

type tracer struct {
}

type ctxkey string

const SQL_START = ctxkey("sqlStart")
const SQL_STRING = ctxkey("sqlString")

func (t tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(context.WithValue(ctx, SQL_START, time.Now()), SQL_STRING, data.SQL)
}

var slowQueryThreshold = 5000 * time.Millisecond

func (t tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	DBMeasures.Hits.M(1)
	ms := time.Since(ctx.Value(SQL_START).(time.Time)).Milliseconds()
	DBMeasures.TotalWait.M(ms)
	DBMeasures.Waits.Observe(float64(ms))
	if data.Err != nil {
		DBMeasures.Errors.M(1)
	}
	if ms > slowQueryThreshold.Milliseconds() {
		logger.Warnw("Slow SQL run",
			"query", ctx.Value(SQL_STRING).(string),
			"err", data.Err,
			"rowCt", data.CommandTag.RowsAffected(),
			"milliseconds", ms)
		return
	}
	logger.Debugw("SQL run",
		"query", ctx.Value(SQL_STRING).(string),
		"err", data.Err,
		"rowCt", data.CommandTag.RowsAffected(),
		"milliseconds", ms)
}

func (db *DB) GetRoutableIP() (string, error) {
	tx, err := db.pgx.Begin(context.Background())
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	local := tx.Conn().PgConn().Conn().LocalAddr()
	addr, ok := local.(*net.TCPAddr)
	if !ok {
		return "", fmt.Errorf("could not get local addr from %v", addr)
	}
	return addr.IP.String(), nil
}

// addStatsAndConnect connects a prometheus logger. Be sure to run this before using the DB.
func (db *DB) addStatsAndConnect() error {

	db.cfg.ConnConfig.Tracer = tracer{}

	hostnameToIndex := map[string]float64{}
	for i, h := range db.hostnames {
		hostnameToIndex[h] = float64(i)
	}
	db.cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		s := db.pgx.Stat()
		DBMeasures.OpenConnections.M(int64(s.TotalConns()))
		DBMeasures.WhichHost.Observe(hostnameToIndex[c.Config().Host])

		//FUTURE place for any connection seasoning
		return nil
	}

	// Timeout the first connection so we know if the DB is down.
	ctx, ctxClose := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer ctxClose()
	var err error
	db.pgx, err = pgxpool.NewWithConfig(ctx, db.cfg)
	if err != nil {
		logger.Error(fmt.Sprintf("Unable to connect to database: %v\n", err))
		return err
	}
	return nil
}

// ITestDeleteAll will delete everything created for "this" integration test.
// This must be called at the end of each integration test.
func (db *DB) ITestDeleteAll() {
	// Template mode uses a dedicated itest database per test.
	if db.itestBaseDB != "" {
		itestDB := db.cfg.ConnConfig.Database
		db.pgx.Close()
		if !schemaRE.MatchString(itestDB) {
			logger.Warn("warning: unclean itest shutdown: invalid database name")
			return
		}
		err := db.itestConn.runWithConn(db.itestBaseDB, 10*time.Second, func(p *pgx.Conn) error {
			_, err := p.Exec(context.Background(), "DROP DATABASE "+itestDB)
			return err
		})
		if err != nil {
			logger.Warn("warning: unclean itest shutdown: cannot drop database: " + err.Error())
		}
		return
	}

	// Non-template mode keeps tests isolated by schema within the base DB.
	if !strings.HasPrefix(db.schema, "itest_") {
		logger.Warn("Warning: this should never be called on anything but an itest schema/database.")
		return
	}
	defer db.pgx.Close()
	if !schemaRE.MatchString(db.schema) {
		logger.Warn("warning: unclean itest shutdown: invalid schema name")
		return
	}
	_, err := db.pgx.Exec(context.Background(), "DROP SCHEMA "+db.schema+" CASCADE")
	if err != nil {
		logger.Warn("warning: unclean itest shutdown: cannot delete schema: " + err.Error())
	}
}

var schemaREString = "^[A-Za-z0-9_]+$"
var schemaRE = regexp.MustCompile(schemaREString)

func ensureSchemaExists(conn connParams, schema, database string) error {
	if len(schema) < 5 || !schemaRE.MatchString(schema) {
		return xerrors.New("schema must be of the form " + schemaREString + "\n Got: " + schema)
	}
	return conn.runWithConn(database, 3*time.Second, func(p *pgx.Conn) error {
		_, err := backoffForSerializationError(func() (pgconn.CommandTag, error) {
			return p.Exec(context.Background(), "CREATE SCHEMA IF NOT EXISTS "+schema)
		})
		return err
	})
}

func ensureTemplateDatabase(conn connParams, baseDB string, options Config) error {
	itestTemplateOnce.Do(func() {
		var exists bool
		err := conn.runWithConn(baseDB, 60*time.Second, func(p *pgx.Conn) error {
			return p.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = "+"'"+itestTemplateDatabase+"')").Scan(&exists)
		})
		if err != nil {
			itestTemplateErr = xerrors.Errorf("cannot check if template database exists: %w", err)
			return
		}
		if !exists || options.RecreateTemplate {
			itestTemplateErr = ensureTemplateDatabaseOnce(conn, baseDB, options)
		}
	})
	return itestTemplateErr
}

func ensureTemplateDatabaseOnce(conn connParams, baseDB string, options Config) error {
	if err := conn.runWithConn(baseDB, 60*time.Second, func(p *pgx.Conn) error {
		if _, err := p.Exec(context.Background(), "DROP DATABASE IF EXISTS "+itestTemplateDatabase); err != nil {
			return err
		}
		_, err := p.Exec(context.Background(), "CREATE DATABASE "+itestTemplateDatabase+" TEMPLATE template0")
		return err
	}); err != nil {
		return xerrors.Errorf("create template database: %w", err)
	}

	templateOpts := options
	templateOpts.Database = itestTemplateDatabase
	templateOpts.Schema = "public"
	templateOpts.ITestID = ""
	templateDB, err := connectAndUpgrade(conn.connString(itestTemplateDatabase), templateOpts)
	if err != nil {
		return xerrors.Errorf("upgrade template database: %w", err)
	}
	templateDB.pgx.Close()
	return nil
}

// buildPoolConfig creates a pgxpool.Config from connString and options.
func buildPoolConfig(connString string, options Config, hosts []string, port string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(connString + "&search_path=" + options.Schema)
	if err != nil {
		return nil, err
	}
	if options.PoolConfig != nil {
		cfg.MaxConns = int32(options.PoolConfig.MaxConnections)
		cfg.MinConns = int32(options.PoolConfig.MinConnections)
		cfg.MaxConnLifetime = options.PoolConfig.MaxConnectionLifetime
		cfg.MaxConnIdleTime = options.PoolConfig.MaxIdleTime
	}
	if !options.LoadBalance && len(hosts) > 0 {
		portInt, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return nil, xerrors.Errorf("invalid port: %w", err)
		}
		cfg.ConnConfig.Host = hosts[0]
		cfg.ConnConfig.Port = uint16(portInt)
	}
	return cfg, nil
}

// connectAndUpgrade creates a pool, connects, runs upgrade, and returns the DB. Caller owns the pool.
func connectAndUpgrade(connString string, options Config) (*DB, error) {
	cfg, err := buildPoolConfig(connString, options, options.Hosts, options.Port)
	if err != nil {
		return nil, err
	}
	ctx, cncl := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cncl()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	db := DB{
		pgx: pool, cfg: cfg, schema: options.Schema,
		hostnames: options.Hosts, sqlEmbedFS: *options.SqlEmbedFS, downgradeEmbedFS: *options.DowngradeEmbedFS,
	}
	if err := db.upgrade(); err != nil {
		db.pgx.Close()
		return nil, err
	}
	return &db, nil
}

var ITestUpgradeFunc func(*pgxpool.Pool, string, string)

// DowngradeTo downgrades the database schema to a previous date (when an upgrade was applied).
// Note: these dates (YYYYMMDD) are not the SQL date but the date the user did an upgrade.
func (db *DB) DowngradeTo(ctx context.Context, dateNum int) error {
	// Is the date good?
	if dateNum < 2000_01_01 || dateNum > 2099_12_31 {
		return xerrors.Errorf("invalid date: %d", dateNum)
	}
	// Ensure all SQL files after that date have a corresponding downgrade file
	var toDowngrade []string
	err := db.Select(ctx, &toDowngrade, "SELECT entry FROM base WHERE applied >= TO_DATE($1, 'YYYYMMDD') ORDER by entry DESC", strconv.Itoa(dateNum))
	if err != nil {
		return xerrors.Errorf("cannot select to downgrade: %w", err)
	}
	// Ensure all SQL files after that date have a corresponding downgrade file
	m := map[string]string{}
	downgrades, err := db.downgradeEmbedFS.ReadDir("downgrade")
	if err != nil {
		return xerrors.Errorf("cannot read downgrade directory: %w", err)
	}

	for _, downgrade := range downgrades {
		m[downgrade.Name()[:8]] = "downgrade/" + downgrade.Name()
	}
	logger.Infof("All available downgrades:", strings.Join(slices.Collect(maps.Values(m)), ", "))

	allGood := true
	for _, file := range toDowngrade {
		file = strings.TrimSpace(file)
		downgradeFile, ok := m[file[:8]]
		if !ok {
			allGood = false
			logger.Errorf("cannot find downgrade file for %s", file)
			f, err := findFileStartingWith(db.sqlEmbedFS, file[:8])
			if err != nil {
				logger.Errorf("cannot find file starting with %s that relates to downgrade-needed value: %w", file[:8], err)
				continue
			}
			logger.Errorf("Original file needing downgrade: %s", file[:8], f)
			continue
		}
		if _, err := db.downgradeEmbedFS.ReadFile(downgradeFile); err != nil {
			allGood = false
			logger.Errorf("cannot find/read downgrade file for %s. Err: %w", file, err)
		}
	}
	if !allGood {
		return xerrors.New("cannot downgrade to date: some downgrade files are missing")
	}
	for _, file := range toDowngrade {
		if err := applySqlFile(db, db.downgradeEmbedFS, m[file[:8]]); err != nil {
			return xerrors.Errorf("cannot apply downgrade file for %s. Err: %w", file, err)
		}
		_, err := db.Exec(context.Background(), "DELETE FROM base WHERE entry = $1", file[:8])
		if err != nil {
			return xerrors.Errorf("cannot delete from base for downgrade: %w", err)
		}
	}
	return nil
}
func (db *DB) upgrade() error {
	// Does the version table exist? if not, make it.
	// NOTE: This cannot change except via the next sql file.
	_, err := db.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS base (
		id SERIAL PRIMARY KEY,
		entry CHAR(12),
		applied TIMESTAMP DEFAULT current_timestamp
	)`)
	if err != nil {
		logger.Error("Upgrade failed.")
		return xerrors.Errorf("Cannot create base table %w", err)
	}

	// __Run scripts in order.__

	landed := map[string]bool{}
	{
		var landedEntries []struct{ Entry string }
		err = db.Select(context.Background(), &landedEntries, "SELECT entry FROM base")
		if err != nil {
			logger.Error("Cannot read entries: " + err.Error())
			return xerrors.Errorf("cannot read entries: %w", err)
		}
		for _, l := range landedEntries {
			landed[l.Entry[:8]] = true
		}
	}
	dir, err := db.sqlEmbedFS.ReadDir("sql")
	if err != nil {
		logger.Error("Cannot read fs entries: " + err.Error())
		return xerrors.Errorf("cannot read fs entries: %w", err)
	}
	sort.Slice(dir, func(i, j int) bool { return dir[i].Name() < dir[j].Name() })

	if len(dir) == 0 {
		logger.Error("No sql files found.")
	}
	for _, e := range dir {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			logger.Debug("Must have only SQL files here, found: " + name)
			continue
		}
		if landed[name[:8]] {
			logger.Debug("DB Schema " + name + " already applied.")
			continue
		}
		file, err := db.sqlEmbedFS.ReadFile("sql/" + name)
		if err != nil {
			logger.Error("weird embed file read err")
			return xerrors.Errorf("cannot read sql file %s: %w", name, err)
		}

		logger.Infow("Upgrading", "file", name, "size", len(file))
		if err := applySqlFile(db, db.sqlEmbedFS, "sql/"+name); err != nil {
			logger.Error("Cannot apply sql file: " + err.Error())
			return xerrors.Errorf("cannot apply sql file %s: %w", name, err)
		}

		// Mark Completed.
		_, err = db.Exec(context.Background(), "INSERT INTO base (entry) VALUES ($1)", name[:8])
		if err != nil {
			logger.Error("Cannot update base: " + err.Error())
			return xerrors.Errorf("cannot insert into base: %w", err)
		}
	}
	return nil
}

func parseSQLStatements(sqlContent string) []string {
	var statements []string
	var currentStatement strings.Builder

	lines := strings.Split(sqlContent, "\n")
	var inFunction bool

	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "--") || strings.HasPrefix(trimmedLine, "#") {
			// Skip empty lines and comments.
			continue
		}

		// Detect function blocks starting or ending.
		if strings.Contains(trimmedLine, "$$") {
			inFunction = !inFunction
		}

		// Add the line to the current statement.
		currentStatement.WriteString(line + "\n")

		// If we're not in a function and the line ends with a semicolon, or we just closed a function block.
		if (!inFunction && strings.HasSuffix(trimmedLine, ";")) || (strings.Contains(trimmedLine, "$$") && !inFunction) {
			statements = append(statements, currentStatement.String())
			currentStatement.Reset()
		}
	}

	// Add any remaining statement not followed by a semicolon (should not happen in well-formed SQL but just in case).
	if currentStatement.Len() > 0 {
		statements = append(statements, currentStatement.String())
	}

	return statements
}

func applySqlFile(db *DB, fs embed.FS, path string) error {
	file, err := fs.ReadFile(path)
	if err != nil {
		return xerrors.Errorf("cannot read sql file %s: %w", path, err)
	}

	var megaSQL strings.Builder
	for _, statement := range parseSQLStatements(string(file)) {
		trimmed := strings.TrimSpace(statement)
		if trimmed == "" {
			continue
		}
		if !strings.HasSuffix(trimmed, ";") {
			megaSQL.WriteString(statement)
			megaSQL.WriteString(";")
		} else {
			megaSQL.WriteString(statement)
		}
	}

	if megaSQL.Len() == 0 {
		return nil
	}

	_, err = db.Exec(context.Background(), rawStringOnly(megaSQL.String()))
	if err != nil {
		return xerrors.Errorf("cannot apply sql file: %w", err)
	}

	if ITestUpgradeFunc != nil {
		ITestUpgradeFunc(db.pgx, path[:8], megaSQL.String())
	}

	return err
}

func findFileStartingWith(fs embed.FS, prefix string) (string, error) {
	entries, err := fs.ReadDir("sql")
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			return entry.Name(), nil
		}
	}
	return "", xerrors.New("file not found")
}

func errFilter(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "host") || strings.Contains(err.Error(), "port") || strings.Contains(err.Error(), "://") {
		logger.Error("redacted db error: " + err.Error())
		return xerrors.New("redacted db error")
	}
	return err
}
