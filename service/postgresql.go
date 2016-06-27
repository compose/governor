package service

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/compose/governor/fsm"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

type PostgresqlConfig struct {
	Name                 string                    `yaml:"name"`
	Listen               string                    `yaml:"listen"`
	DataDirectory        string                    `yaml:"data_dir"`
	MaximumLagOnFailover int                       `yaml:"maximum_lag_on_failover"`
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
		dataDir:              config.DataDirectory,
		maximumLagOnFailover: config.MaximumLagOnFailover,
		replication:          config.Replication,
		parameters:           config.Parameters,
		opLock:               &sync.Mutex{},
		atomicLock:           &sync.Mutex{},
	}

	port, err := strconv.Atoi(strings.Split(config.Listen, ":")[1])
	if err != nil {
		return nil, err
	}
	pg.port = port

	db, err := sql.Open("postgres", pg.connectionString())
	if err != nil {
		return nil, err
	}

	pg.conn = db
	return pg, nil
}

type postgresql struct {
	name                 string
	host                 string
	port                 int
	dataDir              string
	maximumLagOnFailover int
	replication          postgresqlReplicationInfo
	parameters           map[string]interface{}

	// opLock is to be used specifically bounded to low level operations
	// Should be bounded only to queries or sys execs, NEVER lock while calling
	// Other functions from postgresql struct
	opLock *sync.Mutex

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
	WalPosition      int    `json:"wal_position"`
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
	return fmt.Sprintf("postgres://%s:%s@%s:%d/postgres?sslmode=require",
		p.replication.Username,
		p.replication.Password,
		p.host,
		p.port,
	)
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
	xlogPos, err := p.xlogLocation()
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

func (p *postgresql) FSMLeaderTemplate() fsm.Leader {
	return &clusterMember{}
}

func (p *postgresql) FSMMemberTemplate() fsm.Member {
	return &clusterMember{}
}

// TODO: Change interface to (bool, error)???
func (p *postgresql) IsHealthiestOf(leader fsm.Leader, members []fsm.Member) bool {
	selfLocation, err := p.xlogLocation()
	if err != nil {
		return false
	}

	if leader.(*clusterMember).WalPosition-selfLocation > p.maximumLagOnFailover {
		return false
	}

	for _, member := range members {
		if leader.ID() == member.ID() {
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

	location, err := p.xlogLocation()
	if err != nil {
		return false
	}

	result := p.conn.QueryRow(
		"SELECT %s - (pg_last_xlog_replay_location() - '0/000000'::pg_lsn) AS bytes;",
		location)

	var diff int
	if err := result.Scan(&diff); err != nil {
		return false
	}

	if diff < 0 {
		return false
	}
	return true
}

func (p *postgresql) xlogLocation() (int, error) {
	result := p.conn.QueryRow("SELECT pg_last_xlog_replay_location() - '0/0000000'::pg_lsn;")

	var location int
	if err := result.Scan(&location); err != nil {
		return 0, err
	}

	return location, nil
}

func (p *postgresql) lastOperation() (int, error) {
	result := p.conn.QueryRow("SELECT pg_current_xlog_location() - '0/0000000'::pg_lsn;")

	var location int
	if err := result.Scan(&location); err != nil {
		return 0, err
	}

	return location, nil
}

func (p *postgresql) AddMembers(members []fsm.Member) error {
	for _, member := range members {
		if err := p.addReplSlot(member); err != nil {
			return err
		}
	}
	return nil
}

// this should be safe async
func (p *postgresql) addReplSlot(member fsm.Member) error {
	_, err := p.conn.Exec("DO LANGUAGE plpgsql $$DECLARE somevar VARCHAR; "+
		"BEGIN SELECT slot_name INTO somevar FROM pg_replication_slots WHERE slot_name = $1 LIMIT 1; "+
		"IF NOT FOUND THEN PERFORM pg_create_physical_replication_slot($1); END IF; END$$;", member.ID())
	if err != nil {
		return err
	}

	return nil
}

func (p *postgresql) DeleteMembers(members []fsm.Member) error {
	for _, member := range members {
		if err := p.deleteReplSlot(member); err != nil {
			return err
		}
	}
	return nil
}

// this should be safe async
func (p *postgresql) deleteReplSlot(member fsm.Member) error {
	_, err := p.conn.Exec("DO LANGUAGE plpgsql $$DECLARE somevar VARCHAR; "+
		"BEGIN SELECT slot_name INTO somevar FROM pg_replication_slots WHERE slot_name = $1 LIMIT 1; "+
		"IF FOUND THEN PERFORM pg_drop_replication_slot($1); END IF; END$$;", member.ID())
	if err != nil {
		return err
	}

	return nil
}

func (p *postgresql) Initialize() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("initdb", "-D", p.dataDir)
	log.Printf("Initializing Postgres database.")
	return cmd.Run()
}

func (p *postgresql) Ping() error {
	return p.conn.Ping()
}

func (p *postgresql) Start() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if p.IsRunning() {
		return ErrorAlreadyRunning{
			Service: "postgresql",
		}
	}

	if err := p.removePIDFile(); err != nil {
		return err
	}

	return p.start()
}

func (p *postgresql) start() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("pg_ctl",
		"start",
		"-w",
		fmt.Sprintf("-D %s", p.dataDir),
		fmt.Sprintf("-s '%s'", p.serverOptions()),
	)

	return cmd.Run()
}

func (p *postgresql) removePIDFile() error {
	pidPath := fmt.Sprintf("%s/postmaster.pid", p.dataDir)

	if _, err := os.Stat(pidPath); err == nil {
		if err := os.Remove(pidPath); err != nil {
			return err
		}
		log.Printf("Removed %s", pidPath)
	}

	return nil
}

func (p *postgresql) Stop() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if !p.IsRunning() {
		return ErrorNotRunning{
			Service: "postgresql",
		}
	}

	return p.stop()
}

func (p *postgresql) stop() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("pg_ctl",
		"stop",
		"-w",
		fmt.Sprintf("-D %s", p.dataDir),
		"-m fast",
	)

	return cmd.Run()
}

// Restart restarts the PG instance.
func (p *postgresql) Restart() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("pg_ctl",
		"restart",
		"-w",
		fmt.Sprintf("-D %s", p.dataDir),
		"-m fast",
	)

	return cmd.Run()
}

// Already implements ping from *sql.DB
/*
func (p *postgresql) Ping() error {
	return p.nnection.Ping()
}
*/

func (p *postgresql) IsHealthy() bool {
	if !p.IsRunning() {
		return false
	}

	return true
}

// Consider adding repl slots here
func (p *postgresql) Promote() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if err := p.promote(); err != nil {
		return err
	}

	return nil
}

func (p *postgresql) promote() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("pg_ctl",
		"promote",
		"-w",
		fmt.Sprintf("-D %s", p.dataDir),
	)
	return cmd.Run()
}

// Consider removing repl slots
func (p *postgresql) Demote(leader fsm.Leader) error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if err := p.writeRecoveryConf(leader); err != nil {
		return err
	}
	if p.IsRunning() {
		if err := p.Restart(); err != nil {
			return err
		}
	}

	return nil
}

func (p *postgresql) syncFromLeader(leader fsm.Leader) error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("pg_basebackup", leader.(*clusterMember).ConnectionString)
	return cmd.Run()
}

var ErrorAlreadyLeader = errors.New("The node is already a leader")

func (p *postgresql) RunningAsLeader() bool {
	row := p.conn.QueryRow("SELECT pg_is_in_recovery()")

	var inRecovery bool
	if err := row.Scan(&inRecovery); err != nil {
		panic(err)
	}

	return inRecovery
}

func (p *postgresql) FollowTheLeader(leader fsm.Leader) error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if p.NeedsInitialization() {
		if err := p.syncFromLeader(leader); err != nil {
			return err
		}
	}

	// Is this nescessary since we'll just be writing over it?
	// Me thinks premature optimization
	/*
		parsedLead, err := url.Parse(leader.(*clusterMember).ConnectionString)
		if err != nil {
			return err
		}
			cmd := exec.Command("grep",
				fmt.Sprintf("'host=%s port=%d'", parsedLead.Host, parsedLead.Port),
				fmt.Sprintf("%s/recovery.conf", p.dataDir),
			)
			// Wait call will return runtime errors
			if err := cmd.Start(); err != nil {
				return err
			}

			err := cmd.Wait()

			switch err {
			case exec.ExitError:
			default:
				return err
			}
	*/

	if err := p.writeRecoveryConf(leader); err != nil {
		return err
	}
	if p.IsRunning() {
		if err := p.Restart(); err != nil {
			return err
		}
	}

	return nil
}

func (p *postgresql) FollowNoLeader() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if err := p.writeRecoveryConf(nil); err != nil {
		return err
	}
	if p.IsRunning() {
		if err := p.Restart(); err != nil {
			return err
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
		log.Fatal(err)
	}
	return len(files) == 0
}

func (p *postgresql) IsRunning() bool {
	cmd := exec.Command("pg_ctl",
		"status",
		fmt.Sprintf("-D %s", p.dataDir))

	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

func (p *postgresql) serverOptions() string {
	var buffer bytes.Buffer

	buffer.WriteString(fmt.Sprintf("-c listen_addresses=%s -c port=%s", p.host, p.port))
	for setting, value := range p.parameters {
		buffer.WriteString(fmt.Sprintf(" -c \"%s=%s\"", setting, value))
	}
	return buffer.String()
}

// writeRecovery is NOT concurrency safe. Manage with a lock before call
func (p *postgresql) writeRecoveryConf(leader fsm.Leader) error {
	conf, err := os.OpenFile(fmt.Sprintf("%s/recovery.conf", p.dataDir),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		os.FileMode(0666),
	)
	if err != nil {
		return err
	}
	defer conf.Close()

	conf.WriteString(fmt.Sprintf(
		"standby_mode = 'on'\n"+
			"primary_slot_name = '%s'\n"+
			"recovery_target_timeline = 'latest'\n",
		p.name,
	))
	if leader != nil {
		parsedLead, err := url.Parse(leader.(*clusterMember).ConnectionString)
		if err != nil {
			return err
		}
		pass, _ := parsedLead.User.Password()
		conf.WriteString(fmt.Sprintf(
			"primary_conninfo = 'user=%s password=%s host=%s port=%s sslmode=prefer sslcompression=1'",
			parsedLead.User.Username(),
			pass,
			strings.Split(parsedLead.Host, ":")[0],
			strings.Split(parsedLead.Host, ":")[1],
		))
		//TODO: Parse recovery conf

	}
	return nil
}
