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
	"strings"
	"sync"
)

type Postgresql struct {
	Name                 string                    `yaml:"name"`
	Listen               string                    `yaml:"listen"`
	DataDirectory        string                    `yaml:"data_dir"`
	MaximumLagOnFailover int                       `yaml:"maximum_lag_on_failover"`
	Replication          PostgresqlReplicationInfo `yaml:"replication"`
	Parameters           map[string]interface{}    `yaml:"parameters"`

	// opLock is to be used specifically bounded to low level operations
	// Should be bounded only to queries or sys execs, NEVER lock while calling
	// Other functions from Postgresql struct
	opLock *sync.Mutex

	// atomicLock allows us to bind a set of operations to a lock
	// As such we avoid tricky situations where we don't know if calling a long
	// chain of operations will cause deadlock
	//
	// atomicLock also governs access to leader flag
	atomicLock *sync.Mutex

	*sql.DB
}

type PostgresqlReplicationInfo struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Network  string `yaml:"network"`
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

func (p *Postgresql) connectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s/postgres",
		p.Replication.Username,
		p.Replication.Password,
		p.Listen)
}

func (p *Postgresql) AsFSMLeader() (fsm.Leader, error) {
	op, err := p.lastOperation()
	if err != nil {
		return &clusterMember{}, err
	}

	return &clusterMember{
		Name:             p.Name,
		WalPosition:      op,
		ConnectionString: p.connectionString(),
	}, nil
}

func (p *Postgresql) AsFSMMember() (fsm.Member, error) {
	xlogPos, err := p.xlogLocation()
	if err != nil {
		return nil, err
	}

	return &clusterMember{
		Name:             p.Name,
		WalPosition:      xlogPos,
		ConnectionString: p.connectionString(),
	}, nil
}

func (p *Postgresql) FSMLeaderTemplate() fsm.Leader {
	return &clusterMember{}
}

func (p *Postgresql) FSMMemberTemplate() fsm.Member {
	return &clusterMember{}
}

// TODO: Change interface to (bool, error)???
func (p *Postgresql) IsHealthiestOf(leader fsm.Leader, members []fsm.Member) bool {
	selfLocation, err := p.xlogLocation()
	if err != nil {
		return false
	}

	if leader.(*clusterMember).WalPosition-selfLocation > p.MaximumLagOnFailover {
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
func (p *Postgresql) healthierThan(c2 *clusterMember) bool {
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

	result := conn.QueryRow(
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

func (p *Postgresql) xlogLocation() (int, error) {
	result := p.QueryRow("SELECT pg_last_xlog_replay_location() - '0/0000000'::pg_lsn;")

	var location int
	if err := result.Scan(&location); err != nil {
		return 0, err
	}

	return location, nil
}

func (p *Postgresql) lastOperation() (int, error) {
	result := p.QueryRow("SELECT pg_current_xlog_location() - '0/0000000'::pg_lsn;")

	var location int
	if err := result.Scan(&location); err != nil {
		return 0, err
	}

	return location, nil
}

func (p *Postgresql) AddMembers(members []fsm.Member) error {
	for _, member := range members {
		if err := p.addReplSlot(member); err != nil {
			return err
		}
	}
	return nil
}

// this should be safe async
func (p *Postgresql) addReplSlot(member fsm.Member) error {
	_, err := p.Exec("DO LANGUAGE plpgsql $$DECLARE somevar VARCHAR; "+
		"BEGIN SELECT slot_name INTO somevar FROM pg_replication_slots WHERE slot_name = $1 LIMIT 1; "+
		"IF NOT FOUND THEN PERFORM pg_create_physical_replication_slot($1); END IF; END$$;", member.ID())
	if err != nil {
		return err
	}

	return nil
}

func (p *Postgresql) DeleteMembers(members []fsm.Member) error {
	for _, member := range members {
		if err := p.deleteReplSlot(member); err != nil {
			return err
		}
	}
	return nil
}

// this should be safe async
func (p *Postgresql) deleteReplSlot(member fsm.Member) error {
	_, err := p.Exec("DO LANGUAGE plpgsql $$DECLARE somevar VARCHAR; "+
		"BEGIN SELECT slot_name INTO somevar FROM pg_replication_slots WHERE slot_name = $1 LIMIT 1; "+
		"IF FOUND THEN PERFORM pg_drop_replication_slot($1); END IF; END$$;", member.ID())
	if err != nil {
		return err
	}

	return nil
}

func (p *Postgresql) Initialize() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("initdb", "-D", p.DataDirectory)
	log.Printf("Initializing Postgres database.")
	return cmd.Run()
}

func (p *Postgresql) Start() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if p.isRunning() {
		return ErrorAlreadyRunning{
			Service: "Postgresql",
		}
	}

	if err := p.removePIDFile(); err != nil {
		return err
	}

	return p.start()
}

func (p *Postgresql) start() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("pg_ctl",
		"start",
		"-w",
		fmt.Sprintf("-D %s", p.DataDirectory),
		fmt.Sprintf("-s '%s'", p.serverOptions()),
	)

	return cmd.Run()
}

func (p *Postgresql) removePIDFile() error {
	pidPath := fmt.Sprintf("%s/postmaster.pid", p.DataDirectory)

	if _, err := os.Stat(pidPath); err == nil {
		if err := os.Remove(pidPath); err != nil {
			return err
		}
		log.Printf("Removed %s", pidPath)
	}

	return nil
}

func (p *Postgresql) Stop() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if !p.isRunning() {
		return ErrorNotRunning{
			Service: "Postgresql",
		}
	}

	return p.stop()
}

func (p *Postgresql) stop() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("pg_ctl",
		"stop",
		"-w",
		fmt.Sprintf("-D %s", p.DataDirectory),
		"-m fast",
	)

	return cmd.Run()
}

// Restart restarts the PG instance.
func (p *Postgresql) Restart() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("pg_ctl",
		"restart",
		"-w",
		fmt.Sprintf("-D %s", p.DataDirectory),
		"-m fast",
	)

	return cmd.Run()
}

// Already implements ping from *sql.DB
/*
func (p *Postgresql) Ping() error {
	return p.nnection.Ping()
}
*/

func (p *Postgresql) IsHealthy() bool {
	if !p.isRunning() {
		return false
	}

	return true
}

// Consider adding repl slots here
func (p *Postgresql) Promote() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if err := p.promote(); err != nil {
		return err
	}

	return nil
}

func (p *Postgresql) promote() error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("pg_ctl",
		"promote",
		"-w",
		fmt.Sprintf("-D %s", p.DataDirectory),
	)
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

// Consider removing repl slots
func (p *Postgresql) Demote(leader fsm.Leader) error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if err := p.writeRecoveryConf(leader); err != nil {
		return err
	}
	if err := p.Restart(); err != nil {
		return err
	}

	return nil
}

func (p *Postgresql) SyncFromLeader(leader fsm.Leader) error {
	p.opLock.Lock()
	defer p.opLock.Unlock()

	cmd := exec.Command("pg_basebackup", leader.(*clusterMember).ConnectionString)
	return cmd.Run()
}

var ErrorAlreadyLeader = errors.New("The node is already a leader")

func (p *Postgresql) RunningAsLeader() bool {
	row := p.QueryRow("SELECT pg_is_in_recovery()")

	var inRecovery bool
	if err := row.Scan(&inRecovery); err != nil {
		panic(err)
	}

	return inRecovery
}

func (p *Postgresql) FollowTheLeader(leader fsm.Leader) error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	// Is this nescessary since we'll just be writing over it?
	// Me thinks premature optimization
	/*
		parsedLead, err := url.Parse(leader.(*clusterMember).ConnectionString)
		if err != nil {
			return err
		}
			cmd := exec.Command("grep",
				fmt.Sprintf("'host=%s port=%d'", parsedLead.Host, parsedLead.Port),
				fmt.Sprintf("%s/recovery.conf", p.DataDirectory),
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
	if err := p.Restart(); err != nil {
		return err
	}

	return nil
}

func (p *Postgresql) FollowNoLeader() error {
	p.atomicLock.Lock()
	defer p.atomicLock.Unlock()

	if err := p.writeRecoveryConf(nil); err != nil {
		return err
	}
	if err := p.Restart(); err != nil {
		return err
	}

	return nil
}

func (p *Postgresql) NeedsInitialization() bool {
	files, err := ioutil.ReadDir(p.DataDirectory)
	if err != nil {
		if os.IsNotExist(err) {
			return true
		}
		log.Fatal(err)
	}
	return len(files) == 0
}

func (p *Postgresql) isRunning() bool {
	cmd := exec.Command("pg_ctl",
		"status",
		fmt.Sprintf("-D %s", p.DataDirectory))

	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

func (p *Postgresql) serverOptions() string {
	var buffer bytes.Buffer
	host := strings.Split(p.Listen, ":")[0]
	port := strings.Split(p.Listen, ":")[1]

	buffer.WriteString(fmt.Sprintf("-c listen_addresses=%s -c port=%s", host, port))
	for setting, value := range p.Parameters {
		buffer.WriteString(fmt.Sprintf(" -c \"%s=%s\"", setting, value))
	}
	return buffer.String()
}

// writeRecovery is NOT concurrency safe. Manage with a lock before call
func (p *Postgresql) writeRecoveryConf(leader fsm.Leader) error {
	conf, err := os.OpenFile(fmt.Sprintf("%s/recovery.conf", p.DataDirectory),
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
		p.Name,
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
