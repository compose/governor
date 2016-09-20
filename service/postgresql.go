package service

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/compose/governor/fsm"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type PostgresqlConfig struct {
	Name                 string                    `yaml:"name"`
	Listen               string                    `yaml:"listen"`
	DataDirectory        string                    `yaml:"data_dir"`
	MaximumLagOnFailover uint64                    `yaml:"maximum_lag_on_failover"`
	Replication          postgresqlReplicationInfo `yaml:"replication"`
	Parameters           map[string]interface{}    `yaml:"parameters"`
}

type postgresqlReplicationInfo struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Network  string `yaml:"network"`
}

func NewPostgresql(config *PostgresqlConfig) (*postgresql, error) {
	pg := &postgresql{
		name:                 config.Name,
		host:                 strings.Split(config.Listen, ":")[0],
		dataDir:              filepath.Join(config.DataDirectory, "data"),
		authDir:              filepath.Join(config.DataDirectory, "auth"),
		maximumLagOnFailover: config.MaximumLagOnFailover,
		replication:          config.Replication,
		parameters:           config.Parameters,
		opLock:               &sync.Mutex{},
		atomicLock:           &sync.Mutex{},
		members:              make(map[string]fsm.Member),
	}

	port, err := strconv.Atoi(strings.Split(config.Listen, ":")[1])
	if err != nil {
		return nil, err
	}
	pg.port = port

	if err := os.MkdirAll(pg.dataDir, os.FileMode(0700)); err != nil {
		return nil, errors.Wrap(err, "Error ensuring data dir is created")
	}
	if err := os.MkdirAll(pg.authDir, os.FileMode(0700)); err != nil {
		return nil, errors.Wrap(err, "Error ensuring auth dir is created")
	}

	db, err := sql.Open("postgres", pg.localControlConnectionString())
	if err != nil {
		return nil, err
	}

	pg.conn = db

	if err := pg.initConfigs(); err != nil {
		return nil, errors.Wrap(err, "Error setting initial config values")
	}

	return pg, nil
}

func (p *postgresql) initConfigs() error {
	if val, ok := p.parameters["config_file"]; ok {
		p.configFile = val.(string)
	} else {
		p.configFile = filepath.Join(p.dataDir, "postgresql.conf")
	}

	if val, ok := p.parameters["hba_file"]; ok {
		p.hbaFile = val.(string)
	} else {
		p.hbaFile = filepath.Join(p.dataDir, "pg_hba.conf")
	}

	if val, ok := p.parameters["pid_file"]; ok {
		p.pidFile = val.(string)
	}

	return nil
}

type postgresql struct {
	name string
	host string
	port int

	dataDir    string
	authDir    string
	configFile string
	hbaFile    string
	pidFile    string

	maximumLagOnFailover uint64
	replication          postgresqlReplicationInfo
	parameters           map[string]interface{}

	// opLock is to be used specifically bounded to low level operations
	// Should be bounded only to queries or sys execs, NEVER lock while calling
	// Other functions from postgresql struct
	opLock *sync.Mutex

	members map[string]fsm.Member

	// atomicLock allows us to bind a set of operations to a lock
	// As such we avoid tricky situations where we don't know if calling a long
	// chain of operations will cause deadlock
	//
	// atomicLock also governs access to leader flag
	atomicLock *sync.Mutex

	conn *sql.DB
}

type clusterMember struct {
	Name             string `json:"name"`
	WalPosition      uint64 `json:"wal_position"`
	ConnectionString string `json:"connection_string"`
}

func (c *clusterMember) ID() string {
	return c.Name
}

func (c *clusterMember) MarshalFSM() ([]byte, error) {
	return json.Marshal(c)
}

func (c *clusterMember) UnmarshalFSM(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *clusterMember) connect() (*sql.DB, error) {
	return sql.Open("postgres", c.ConnectionString)
}

func (p *postgresql) connectionString() string {
	// TODO: look into verfying ssl
	// TODO: ENABLE SSL!!!
	return fmt.Sprintf("postgres://%s:%s@%s:%d/postgres?sslmode=disable",
		p.replication.Username,
		p.replication.Password,
		p.host,
		p.port,
	)
}

func (p *postgresql) localControlConnectionString() string {
	//TODO ENABLE SSL!!!
	return fmt.Sprintf("postgres://%s:%d/postgres?sslmode=disable", p.host, p.port)
}

func (p *postgresql) AsFSMLeader() (fsm.Leader, error) {
	op, err := p.lastOperation()
	if err != nil {
		return &clusterMember{}, err
	}

	return &clusterMember{
		Name:             p.name,
		WalPosition:      op,
		ConnectionString: p.connectionString(),
	}, nil
}

func (p *postgresql) AsFSMMember() (fsm.Member, error) {
	xlogPos, err := p.lastOperation()
	if err != nil {
		return nil, err
	}

	return &clusterMember{
		Name:             p.name,
		WalPosition:      xlogPos,
		ConnectionString: p.connectionString(),
	}, nil
}

func (p *postgresql) FSMLeaderFromBytes(data []byte) (fsm.Leader, error) {
	leader := &clusterMember{}
	if err := leader.UnmarshalFSM(data); err != nil {
		return nil, err
	}
	return leader, nil
}

func (p *postgresql) FSMMemberFromBytes(data []byte) (fsm.Member, error) {
	member := &clusterMember{}
	if err := member.UnmarshalFSM(data); err != nil {
		return nil, err
	}

	return member, nil
}

// TODO: Change interface to (bool, error)???
// TODO: Have HA ask FSM to give update of WAL and pass those in
//	 That way we have consensus in the FSM of which node should be elected
func (p *postgresql) IsHealthiestOf(leader fsm.Leader, members []fsm.Member) bool {
	selfLocation, err := p.lastOperation()
	if err != nil {
		return false
	}

	// If we're behind the leader's position we know we aren't healthy
	if leader != nil && leader.(*clusterMember).WalPosition-selfLocation > p.maximumLagOnFailover {
		return false
	}

	for _, member := range members {
		if (leader != nil && leader.ID() == member.ID()) || member.ID() == p.name {
			continue
		}
		member := member.(*clusterMember)
		if !p.healthierThan(member) {
			return false
		}
	}
	return true
}

// TODO: Check some of these errs
func (p *postgresql) healthierThan(c2 *clusterMember) bool {
	conn, err := c2.connect()
	if err != nil {
		return false
	}

	// Supposedly the sql package handles pooling really well
	// Perhaps bencmark closing vs not closing
	defer conn.Close()

	location, err := p.lastOperation()
	if err != nil {
		return false
	}

	result := p.conn.QueryRow(
		"SELECT $1 - (pg_last_xlog_replay_location() - '0/000000'::pg_lsn) AS bytes;",
		location)

	var diff uint64
	if err := result.Scan(&diff); err != nil {
		return false
	}

	log.Infof("Difference between logs in health check %d", diff)
	log.Infof("My last op: %d", location)

	if diff < 0 {
		return false
	}
	return true
}

func (p *postgresql) xlogReplayLocation() (uint64, error) {
	row := p.conn.QueryRow("SELECT pg_last_xlog_replay_location() - '0/0000000'::pg_lsn;")

	var location uint64
	if err := row.Scan(&location); err != nil {
		switch {
		case err == sql.ErrNoRows:
			log.Warnf("No rows for query")
		}
		return 0, errors.Wrap(err, "Error scanning query for pg_last_xlog_replay_location")
	}

	return location, nil
}

func (p *postgresql) xlogLocation() (uint64, error) {
	result := p.conn.QueryRow("SELECT pg_current_xlog_location() - '0/0000000'::pg_lsn;")

	var location uint64
	if err := result.Scan(&location); err != nil {
		return 0, err
	}

	return location, nil
}

func (p *postgresql) lastOperation() (uint64, error) {
	// TODO: have leader check be atomic query
	if p.runningAsLeader() {
		return p.xlogLocation()
	} else {
		return p.xlogReplayLocation()
	}
}

func (p *postgresql) AddMembers(members []fsm.Member) error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Infof("Adding members")
	meAsFSMMember, err := p.AsFSMMember()
	if err != nil {
		return errors.Wrap(err, "Error getting self as FSM Member")
	}

	for _, member := range members {
		if member.ID() != meAsFSMMember.ID() && p.runningAsLeader() {
			if err := p.addReplSlot(member); err != nil {
				return errors.Wrap(err, "Issue adding members to PG")
			}
		}
		p.members[member.ID()] = member
	}
	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Infof("Successfully added members")
	return nil
}

// this should be safe async
func (p *postgresql) addReplSlot(member fsm.Member) error {
	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Infof("Adding Repl slot with ID: %s", member.ID())
	query := fmt.Sprintf("DO LANGUAGE plpgsql $$DECLARE somevar VARCHAR; "+
		"BEGIN SELECT slot_name INTO somevar FROM pg_replication_slots WHERE slot_name = '%s' LIMIT 1; "+
		"IF NOT FOUND THEN PERFORM pg_create_physical_replication_slot('%s'); END IF; END$$;", member.ID(), member.ID())
	_, err := p.conn.Exec(query)
	if err != nil {
		return errors.Wrap(err, "Error querying pg for replication slot addition")
	}

	return nil
}

func (p *postgresql) addAllMembersReplSlot() error {
	meAsFSMMember, err := p.AsFSMMember()
	if err != nil {
		return errors.Wrap(err, "Error getting self as FSM Member")
	}
	for _, member := range p.members {
		if member.ID() != meAsFSMMember.ID() {
			if err := p.addReplSlot(member); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *postgresql) DeleteMembers(members []fsm.Member) error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Infof("Deleting members")
	meAsFSMMember, err := p.AsFSMMember()
	if err != nil {
		return errors.Wrap(err, "Error getting self as FSM Member")
	}
	for _, member := range members {
		if member.ID() != meAsFSMMember.ID() && p.runningAsLeader() {
			if err := p.deleteReplSlot(member); err != nil {
				return errors.Wrap(err, "Issue deleting members from PG")
			}
		}
		delete(p.members, member.ID())
	}
	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Infof("Successfully deleted members")

	return nil
}

// this should be safe async
func (p *postgresql) deleteReplSlot(member fsm.Member) error {
	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Infof("Deleting Repl slot with ID: %s", member.ID())
	query := fmt.Sprintf("DO LANGUAGE plpgsql $$DECLARE somevar VARCHAR; "+
		"BEGIN SELECT slot_name INTO somevar FROM pg_replication_slots WHERE slot_name = '%s' LIMIT 1; "+
		"IF FOUND THEN PERFORM pg_drop_replication_slot('%s'); END IF; END$$;", member.ID(), member.ID())
	_, err := p.conn.Exec(query)
	if err != nil {
		return errors.Wrap(err, "Error querying pg for replication slot deletion")
	}

	return nil
}

func (p *postgresql) deleteAllMembersReplSlot() error {
	meAsFSMMember, err := p.AsFSMMember()
	if err != nil {
		return errors.Wrap(err, "Error getting self as FSM Member")
	}
	for _, member := range p.members {
		if member.ID() != meAsFSMMember.ID() {
			if err := p.deleteReplSlot(member); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *postgresql) Initialize() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if err := p.initialize(); err != nil {
		return errors.Wrap(err, "Error initializing PG")
	}
	if err := p.start(); err != nil {
		return errors.Wrap(err, "Error starting PG in initialization")
	}
	if err := p.createReplicationUser(); err != nil {
		return errors.Wrap(err, "Error creating replication user for PG")
	}
	if err := p.stop(); err != nil {
		return errors.Wrap(err, "Error stopping PG in initialization")
	}
	if err := p.writePGHBA(); err != nil {
		return errors.Wrap(err, "Error writing pg_hba")
	}

	return nil
}

func (p *postgresql) initialize() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("initdb", "-D", p.dataDir)
	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Initializing Postgres database.")

	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "Issue initializing database")
	}

	return errors.Wrap(p.ensureConfig(), "Error ensuring basic configs exist")
}

func (p *postgresql) ensureConfig() error {
	// TODO: change to only specify archive_mode
	// if archive level args are specified somewhere. Then force archive_mode to "always"
	requiredConfigArgs := map[string]string{
		"wal_level":   "hot_standby",
		"hot_standby": "on",
	}

	config, err := os.OpenFile(p.configFile,
		os.O_RDWR|os.O_CREATE,
		os.FileMode(0666),
	)
	defer config.Close()
	if err != nil {
		return errors.Wrapf(err, "Error opening config file: %s", p.configFile)

	}

	configData, err := ioutil.ReadAll(config)
	if err != nil {
		return errors.Wrap(err, "Error reading config file")
	}
	// add note to end of config about auto-generated info
	// using bytes due to better performance documented here
	// http://crypticjags.com/golang/can-golang-beat-perl-on-regex-performance.html
	configData = append(configData, []byte("\n# Generated by governor")...)

	// Should find config items over an entire line
	// Note: we could have multiple commented items of the same arg
	// that we need to take into account
	regexTemplate := `(?m)^\s*(%s[= ].*)$`
	for arg, val := range requiredConfigArgs {
		// Ensure we aren't passing contrary configs
		delete(p.parameters, arg)

		re := regexp.MustCompile(fmt.Sprintf(regexTemplate, arg))

		// if this arg doesn't exist
		switch len(re.FindAll(configData, 2)) {
		case 0:
			configData = append(configData, []byte(fmt.Sprintf("\n%s = '%s'", arg, val))...)
		case 1:
			overrideArg := fmt.Sprintf("#$1\n# This value generated by governor\n%s = '%s'", arg, val)
			configData = re.ReplaceAll(configData, []byte(overrideArg))
		default:
			return errors.New(fmt.Sprintf("Given config is faulty. Multiple instances for argument %s", arg))
		}

	}

	// TODO: Replace at proper seek points
	if err := config.Truncate(0); err != nil {
		return errors.Wrap(err, "Issue truncating old config")
	}
	if _, err := config.Seek(0, 0); err != nil {
		return errors.Wrap(err, "Issue seeking to begin of config")
	}
	if _, err := config.Write(configData); err != nil {
		return errors.Wrap(err, "Issue writing new config")
	}

	return nil
}

func (p *postgresql) writePGHBA() error {
	hbaConf, err := os.OpenFile(p.hbaFile,
		os.O_RDWR|os.O_APPEND,
		os.FileMode(0666),
	)
	defer hbaConf.Close()

	if err != nil {
		return errors.Wrap(err, "Error opening pg_hba.conf")
	}

	_, err = hbaConf.WriteString(
		fmt.Sprintf("host replication %s %s md5",
			p.replication.Username,
			p.replication.Network,
		),
	)
	return errors.Wrap(err, "Error writing to pg_hba.conf")
}

func (p *postgresql) createReplicationUser() error {
	query := fmt.Sprintf("CREATE USER %s WITH REPLICATION ENCRYPTED PASSWORD '%s'",
		p.replication.Username,
		p.replication.Password,
	)
	_, err := p.conn.Exec(query)
	return errors.Wrapf(err, "Creating the replication priv user %s", p.replication.Username)
}

func (p *postgresql) Ping() error {
	return errors.Wrap(p.conn.Ping(), "Error pinging PG")
}

func (p *postgresql) Start() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if p.isRunning() {
		return ErrorAlreadyRunning{
			Service: "postgresql",
		}
	}
	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Starting Postgresql")

	if err := p.removePIDFile(); err != nil {
		return errors.Wrap(err, "Error removing PID file")
	}

	if err := p.start(); err != nil {
		return errors.Wrap(err, "Error starting PG")
	}

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Successfully started Postgresql")
	return nil
}

func (p *postgresql) start() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	startArg := "start"
	waitFlag := "-w"
	logArg := fmt.Sprintf("--log=%s", filepath.Join(p.dataDir, "pg_log"))
	dataArg := fmt.Sprintf("-D %s", p.dataDir)

	combinedArgs := strings.Join([]string{startArg, logArg, waitFlag, dataArg}, " ")

	argFields := strings.Fields(combinedArgs)

	// make sure we pass the -o string as it's own member of the slice
	argFields = append(argFields, "-o")
	argFields = append(argFields, p.serverOptions())

	cmd := exec.Command("pg_ctl",
		argFields...,
	)

	var runtimeErrs bytes.Buffer
	cmd.Stderr = &runtimeErrs

	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "Error running pg_ctl command: %s", string(runtimeErrs.Bytes()))
	}

	if err := p.updateFiles(); err != nil {
		return errors.Wrap(err, "Error updating PG files")
	}
	return nil
}

func (p *postgresql) updateFiles() error {
	var configFile string
	var hbaFile string
	var pidFile string

	confRow := p.conn.QueryRow("SHOW config_file;")
	hbaRow := p.conn.QueryRow("SHOW hba_file;")
	pidRow := p.conn.QueryRow("SHOW external_pid_file;")

	log.Info("Finding Files to update")

	if err := confRow.Scan(&configFile); err != nil {
		log.Errorf("Couldn't find config file: %+v", err)
		return errors.Wrap(err, "Error querying pg for config_file")
	}

	if err := hbaRow.Scan(&hbaFile); err != nil {
		log.Error("Couldn't find hba file")
		return errors.Wrap(err, "Error querying pg for hba_file")
	}
	if err := pidRow.Scan(&pidFile); err != nil {
		log.Error("Couldn't find pid file")
		return errors.Wrap(err, "Error querying pg for hba_file")
	}

	p.configFile = configFile
	p.hbaFile = hbaFile
	p.pidFile = pidFile

	return nil
}

func (p *postgresql) removePIDFile() error {
	if _, err := os.Stat(p.pidFile); err == nil {
		if err := os.Remove(p.pidFile); err != nil {
			return errors.Wrap(err, "error in syscall to remove PID")
		}
		log.WithFields(log.Fields{
			"package": "postgresql",
		}).Infof("Removed %s", p.pidFile)

	}

	return nil
}

func (p *postgresql) Stop() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if !p.isRunning() {
		return ErrorNotRunning{
			Service: "postgresql",
		}
	}

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Stopping Postgresql")

	if err := p.stop(); err != nil {
		return errors.Wrap(err, "Error stopping PG")
	}

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Successfully stopped Postgresql")
	return nil
}

func (p *postgresql) stop() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	stopArg := "stop"
	waitFlag := "-w"
	logArg := fmt.Sprintf("--log=%s", filepath.Join(p.dataDir, "pg_log"))
	dataArg := fmt.Sprintf("-D %s", p.dataDir)

	combinedArgs := strings.Join([]string{stopArg, logArg, waitFlag, dataArg}, " ")

	argFields := strings.Fields(combinedArgs)

	cmd := exec.Command("pg_ctl",
		argFields...,
	)

	return errors.Wrap(cmd.Run(), "Error running pg_ctl stop command")
}

func (p *postgresql) Restart() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()
	return p.restart()
}

// Restart restarts the PG instance.
func (p *postgresql) restart() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	if err := p.updateFiles(); err != nil {
		return errors.Wrap(err, "Error updating file locations")
	}

	restartArg := "restart"
	waitFlag := "-w"
	logArg := fmt.Sprintf("--log=%s", filepath.Join(p.dataDir, "pg_log"))
	dataArg := fmt.Sprintf("-D %s", p.dataDir)
	mArg := fmt.Sprintf("-m %s", "fast")

	combinedArgs := strings.Join([]string{restartArg, waitFlag, logArg, dataArg, mArg}, " ")

	argFields := strings.Fields(combinedArgs)

	argFields = append(argFields, "-o")
	argFields = append(argFields, p.serverOptions())

	cmd := exec.Command("pg_ctl",
		argFields...,
	)

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Restarting Postgresql")

	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "Error running pg_ctl restart command")
	}

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Successfully restarted Postgresql")

	return nil
}

func (p *postgresql) IsHealthy() bool {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()
	if !p.isRunning() {
		return false
	}

	return true
}

// Consider adding repl slots here
func (p *postgresql) Promote() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Promoting Postgresql")

	if err := p.promote(); err != nil {
		return errors.Wrap(err, "Error promiting node")
	}
	if err := p.addAllMembersReplSlot(); err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Successfully promoted Postgresql")

	return nil
}

func (p *postgresql) promote() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	promoteArg := "promote"
	logArg := fmt.Sprintf("--log=%s", filepath.Join(p.dataDir, "pg_log"))
	waitFlag := "-w"
	dataArg := fmt.Sprintf("-D %s", p.dataDir)

	combinedArgs := strings.Join([]string{promoteArg, logArg, waitFlag, dataArg}, " ")

	argFields := strings.Fields(combinedArgs)

	cmd := exec.Command("pg_ctl",
		argFields...,
	)
	return errors.Wrap(cmd.Run(), "Error running pg_ctl promote command")
}

// Consider removing repl slots
func (p *postgresql) Demote(leader fsm.Leader) error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Demoting Postgresql")

	if err := p.writeRecoveryConf(leader); err != nil {
		return errors.Wrap(err, "Error writing recovery conf")
	}

	if err := p.deleteAllMembersReplSlot(); err != nil {
		return err
	}

	if p.isRunning() {
		if err := p.restart(); err != nil {
			return errors.Wrap(err, "Error restarting PG")
		}
	}

	log.WithFields(log.Fields{
		"package": "postgresql",
	}).Info("Successfully demoted Postgresql")

	return nil
}

func (p *postgresql) syncFromLeader(leader fsm.Leader) error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	if err := p.writePGPass(leader.(*clusterMember)); err != nil {
		return errors.Wrap(err, "Error writing pgpass file")
	}

	pgURL, err := url.Parse(leader.(*clusterMember).ConnectionString)
	if err != nil {
		return errors.Wrap(err, "Error parsing leader connection string")
	}

	host, _, err := net.SplitHostPort(pgURL.Host)
	if err != nil {
		return errors.Wrap(err, "Error splitting host and port")
	}

	user := pgURL.User.Username()

	dataArg := fmt.Sprintf("-D %s", p.dataDir)
	userArg := fmt.Sprintf("-U %s", user)
	hostArg := fmt.Sprintf("--host=%s", host)
	streamXlogArg := fmt.Sprintf("--xlog-method=%s", "stream")

	combinedArgs := strings.Join([]string{dataArg, userArg, hostArg, streamXlogArg}, " ")
	fmt.Println(combinedArgs)

	argFields := strings.Fields(combinedArgs)

	cmd := exec.Command("pg_basebackup",
		argFields...,
	)

	path, ok := os.LookupEnv("PATH")
	if !ok {
		return errors.New("PATH variable isn't set")
	}

	cmd.Env = []string{fmt.Sprintf("PGPASSFILE=%s", filepath.Join(p.authDir, ".pgpass")),
		fmt.Sprintf("PATH=%s", path),
	}

	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "Error running pg_basebackup command")
	}
	// pg_basebackup resets the config. BAD!
	return p.ensureConfig()
}

func (p *postgresql) writePGPass(member *clusterMember) error {
	pgURL, err := url.Parse(member.ConnectionString)
	if err != nil {
		return errors.Wrap(err, "Error parsing leader connection string")
	}

	host, port, err := net.SplitHostPort(pgURL.Host)
	if err != nil {
		return errors.Wrap(err, "Error splitting host and port")
	}

	user := pgURL.User.Username()
	pass, _ := pgURL.User.Password()

	pgPass, err := os.OpenFile(filepath.Join(p.authDir, ".pgpass"),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		os.FileMode(0600),
	)
	defer pgPass.Close()
	if err != nil {
		return errors.Wrap(err, "Error opening pgpass file")
	}

	_, err = pgPass.WriteString(
		fmt.Sprintf("%s:%s:*:%s:%s", host, port, user, pass),
	)

	return errors.Wrap(err, "Error writing pgpass file")
}

var ErrorAlreadyLeader = errors.New("The node is already a leader")

func (p *postgresql) RunningAsLeader() bool {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()
	return p.runningAsLeader()
}

func (p *postgresql) runningAsLeader() bool {
	if !p.isRunning() {
		return false
	}

	p.opLock.Lock()
	defer p.opLock.Unlock()
	row := p.conn.QueryRow("SELECT pg_is_in_recovery()")

	var inRecovery bool
	if err := row.Scan(&inRecovery); err != nil {
		panic(err)
	}

	return !inRecovery
}

func (p *postgresql) FollowTheLeader(leader fsm.Leader) error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if p.NeedsInitialization() {
		log.WithFields(log.Fields{
			"package": "governor",
		})
		log.Info("Syncing from Leader")
		if err := p.syncFromLeader(leader); err != nil {
			return errors.Wrap(err, "Error syncing from leader")
		}
	}

	if err := p.writeRecoveryConf(leader); err != nil {
		return errors.Wrap(err, "Error writing recovery conf")
	}

	if p.isRunning() {
		if err := p.restart(); err != nil {
			return errors.Wrap(err, "Error restarting PG")
		}
	} else {
		log.WithFields(log.Fields{
			"package": "governor",
		})
		log.Info("Starting Postgresql")
		if err := p.start(); err != nil {
			return errors.Wrap(err, "Error starting PG")
		}
		log.WithFields(log.Fields{
			"package": "governor",
		})
		log.Info("Successfully started Postgresql")
	}

	return nil
}

func (p *postgresql) FollowNoLeader() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if err := p.writeRecoveryConf(nil); err != nil {
		return errors.Wrap(err, "Error writing recovery conf")
	}
	if p.isRunning() {
		if err := p.restart(); err != nil {
			return errors.Wrap(err, "Error restarting PG")
		}
	}

	return nil
}

func (p *postgresql) NeedsInitialization() bool {
	files, err := ioutil.ReadDir(p.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return true
		}
		log.WithFields(log.Fields{
			"package": "governor",
		})
		log.Fatal(err)
	}
	return len(files) == 0
}

func (p *postgresql) IsRunning() bool {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()
	return p.isRunning()
}

func (p *postgresql) isRunning() bool {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	statusArg := "status"
	logArg := fmt.Sprintf("--log=%s", filepath.Join(p.dataDir, "pg_log"))
	dataArg := fmt.Sprintf("-D %s", p.dataDir)

	combinedArgs := strings.Join([]string{statusArg, logArg, dataArg}, " ")

	argFields := strings.Fields(combinedArgs)

	cmd := exec.Command("pg_ctl",
		argFields...,
	)

	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

func (p *postgresql) serverOptions() string {
	var buffer bytes.Buffer

	buffer.WriteString(fmt.Sprintf("-clisten_addresses=%s -cport=%d", p.host, p.port))
	for setting, value := range p.parameters {
		buffer.WriteString(fmt.Sprintf(" -c%s=\"%v\"", setting, value))
	}
	return buffer.String()
}

// writeRecovery is NOT concurrency safe. Manage with a lock before call
func (p *postgresql) writeRecoveryConf(leader fsm.Leader) error {
	conf, err := os.OpenFile(filepath.Join(p.dataDir, "recovery.conf"),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		os.FileMode(0666),
	)
	defer conf.Close()
	if err != nil {
		return errors.Wrap(err, "Error opening recovery.conf")
	}

	_, err = conf.WriteString(fmt.Sprintf(
		"standby_mode = 'on'\n"+
			"primary_slot_name = '%s'\n"+
			"recovery_target_timeline = 'latest'\n",
		p.name,
	))

	if err != nil {
		return errors.Wrap(err, "Error writing string to recovery conf")
	}

	if leader != nil {
		parsedLead, err := url.Parse(leader.(*clusterMember).ConnectionString)
		if err != nil {
			return errors.Wrap(err, "Error parsing PG connection string")
		}
		pass, _ := parsedLead.User.Password()
		_, err = conf.WriteString(fmt.Sprintf(
			"primary_conninfo = 'user=%s password=%s host=%s port=%s sslmode=prefer sslcompression=1'",
			parsedLead.User.Username(),
			pass,
			strings.Split(parsedLead.Host, ":")[0],
			strings.Split(parsedLead.Host, ":")[1],
		))
		if err != nil {
			return errors.Wrap(err, "Error writing string to recovery conf")
		}
		//TODO: Parse recovery conf

	}
	return nil
}
