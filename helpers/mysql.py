import sys, os, re, time
import logging
import _mysql as msycosql
import _mysql_exceptions
import subprocess

from urlparse import urlparse


logger = logging.getLogger(__name__)

class MySQL:

    def __init__(self, config):
        logger.debug("######## __init__")
        self.name = config["name"]
        self.host, self.port = config["listen"].split(":")
        self.socket = config["socket"]
        self.data_dir = config["data_dir"]
        self.replication = config["replication"]

        self.config = config

        self.conn_holder = None
        self.mysql_process = None
        self.connection_string = "postgres://%s:%s@%s:%s/postgres" % (self.replication["username"], self.replication["password"], self.host, self.port)

    def conn(self):
        logger.debug("######## conn")
        if not self.conn_holder:
            max_attempts = 0
            while True:
                try:
                    self.conn_holder = msycosql.connect(unix_socket=self.socket, user='root', db='information_schema')
                    break
                except _mysql_exceptions.OperationalError as e:
                    self.conn_holder = None
                    if e[0] == 1049: # catch Unknown database 'mysql' and allow it to proceed
                        time.sleep(5)
                    else:
                        if max_attempts > 4:
                            raise e
                        max_attempts += 1
                        time.sleep(5)
        return self.conn_holder

    def disconnect(self):
        logger.debug("######## disconnect")
        try:
            self.conn().close()
        except Exception as e:
            logger.error("Error disconnecting: %s" % e)

    def query(self, sql):
        logger.debug("######## query")
        max_attempts = 0
        result = None
        while True:
            try:
                logger.debug("###### Trying query: %s" % sql)
                self.conn().query(sql)
                result = self.conn().store_result()
                break
            except _mysql_exceptions.OperationalError as e:
                logger.debug('###### rescued and will reconnect')
                if self.conn():
                    self.disconnect()
                self.conn_holder = None
                if max_attempts > 4:
                    raise e
                max_attempts += 1
                time.sleep(5)
        return result

    def data_directory_empty(self):
        logger.debug("######## data_directory_empty")
        return not os.path.exists(self.data_dir) or os.listdir(self.data_dir) == []

    def initialize(self):
        logger.debug("######## initializing: mysqld %s" % self.initdb_options())
        if subprocess.call("mysqld %s" % self.initdb_options(), shell=True) == 0:
            # start MySQL without options to setup replication user indepedent of other system settings
            logger.debug("######## starting: mysqld %s" % self.initdb_options())
            self.clear_replication_conf()
            self.start()
            self.create_replication_user()
            self.run_post_initialization_commands()
            self.stop()

            return True

        return False

    def sync_from_leader(self, leader):
        logger.debug("######## sync_from_leader")
        leader = urlparse(leader["address"])

        if subprocess.call("mysqldump --host %(hostname)s --port %(port)s -u %(username)s -p%(password)s --all-databases --master-data=2 --single-transaction > /tmp/sync-from-leader.db" %
                {"hostname": leader.hostname, "port": leader.port, "username": leader.username, "password": leader.password}, shell=True) != 0:
            logger.fatal("Error running mysqldump.")
            sys.exit(1)

        if subprocess.call("mysqld %s" % self.initdb_options(), shell=True) == 0:
            self.clear_replication_conf()
            self.start()
        else:
            logger.fatal("Error starting mysql as follower")
            sys.exit(1)

        master_settings = subprocess.check_output("head -n 30 /tmp/sync-from-leader.db | grep -o \"CHANGE MASTER TO MASTER_LOG_FILE='[^\\']\\+', MASTER_LOG_POS=[0-9]\+\"", shell=True)
        master_settings += """
        , MASTER_HOST='%(hostname)s', MASTER_PORT=%(port)s, MASTER_USER='%(username)s',MASTER_PASSWORD='%(password)s'
        """ % {"hostname": leader.hostname, "port": leader.port, "username": leader.username, "password": leader.password}

        logger.debug("Loading data with: mysql -u root --socket %s < /tmp/sync-from-leader.db" % self.socket)
        if subprocess.call("mysql -u root --socket %s < /tmp/sync-from-leader.db" % self.socket, shell=True) == 0:
            logger.info("Data load successful.")
            self.stop()
            self.write_replication_conf()
            self.start()
            self.query(master_settings);
            self.query("START SLAVE;")
            return True
        else:
            logger.fatal("Error loading data from mysqldump.")
            sys.exit(1)

    def is_leader(self):
        logger.debug("######## is_leader")
        return self.query("SHOW SESSION VARIABLES WHERE variable_name = 'read_only';").fetch_row()[0][1] == "OFF"

    def is_running(self):
        if self.mysql_process:
            logger.debug("######## is_running: %s", self.mysql_process.poll())
            return self.mysql_process.poll() == None # if no status, then the process is running
        else:
            logger.debug("######## is_running: never started")
            return False

    def start(self):
        logger.debug("######## start")
        if self.is_running():
            logger.error("Cannot start MySQL because one is already running.")
            return False

        logger.debug("######## starting mysql")
        self.mysql_process = subprocess.Popen("mysqld --defaults-extra-file=%s/replication.cnf  %s" % (self.data_dir, self.server_options()), shell=True)

        while not self.is_ready():
            if not self.is_running():
                return False
            time.sleep(3)
        return self.mysql_process.poll() == None

    def is_ready(self):
        return self.is_running() and subprocess.call("mysqladmin status -u root --socket %s" % self.socket, shell=True) == 0

    def stop(self):
        logger.debug("######## stop")
        if subprocess.call("mysqladmin shutdown -u root --socket %s" % self.socket, shell=True) == 0:
            return self.mysql_process.wait() == 0

    def reload(self):
        logger.debug("######## reload")
        return subprocess.call("mysqladmin reload -u root --socket %s" % self.socket, shell=True) == 0

    def restart(self):
        logger.debug("######## restart")
        return self.stop() and self.start()

    def server_options(self):
        logger.debug("######## server_options")
        options = "--datadir %(datadir)s --bind-address %(host)s --port %(port)s --socket %(socket)s" % {"datadir": self.data_dir, "host": self.host, "port": self.port, "socket": self.socket}
        for setting, value in self.config["parameters"].iteritems():
            options += " --%s=%s" % (setting, value)
        return options

    def initdb_options(self):
        logger.debug("######## initdb_options")
        options = "--initialize-insecure %s" % self.server_options()
        if "initdb_parameters" in self.config:
            for param in self.config["initdb_parameters"]:
                options += " %s" % param

        return options

    def is_healthy(self):
        logger.debug("######## is_healthy")
        if not self.is_running():
            logger.warning("MySQL is not running.")
            return False

        if self.is_leader():
            return True

        return True

    def is_healthiest_node(self, state_store):
        logger.debug("######## is_healthiest_node")
        # this should only happen on initialization
        if state_store.last_leader_operation() is None:
            return True

        if (state_store.last_leader_operation() - self.last_operation()) > self.config["maximum_lag_on_failover"]:
            return False

        for member in state_store.members():
            if member["hostname"] == self.name:
                continue
            try:
                member_conn = psycopg2.connect(member["address"])
                member_conn.autocommit = True
                member_cursor = member_conn.cursor()
                member_cursor.execute("SELECT %s - (pg_last_xlog_replay_location() - '0/000000'::pg_lsn) AS bytes;" % self.last_operation())
                xlog_diff = member_cursor.fetchone()[0]
                if xlog_diff < 0:
                    member_cursor.close()
                    return False
                member_cursor.close()
            except psycopg2.OperationalError:
                continue
        return True

    def follow_the_leader(self, leader_hash):
        # logger.debug("######## follow_the_leader")
        # leader = urlparse(leader_hash["address"])
        # if subprocess.call("grep 'host=%(hostname)s port=%(port)s' %(data_dir)s/replication.cnf > /dev/null" % {"hostname": leader.hostname, "port": leader.port, "data_dir": self.data_dir}, shell=True) != 0:
            # self.write_replication_conf()
            # self.restart()
        return True

    def follow_no_leader(self):
        self.write_replication_conf()
        if self.is_ready():
            self.query("SET GLOBAL read_only = ON;")
            self.query("STOP SLAVE;")
        return True

    def promote(self):
        logger.debug("######## promote")
        self.clear_replication_conf()
        return self.restart()

    def demote(self, leader):
        logger.debug("######## demote")
        self.write_replication_conf()
        self.restart()

    def create_replication_user(self):
        logger.debug("######## create_replication_user")
        success = False

        while not success:
            try:
                self.query("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" % (self.replication["username"], self.replication["network"], self.replication["password"]))
                self.query("GRANT SELECT, PROCESS, FILE, SUPER, REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO '%s'@'%s';" % (self.replication["username"], self.replication["network"]))
                success = True
            except _mysql_exceptions.InternalError as e:
                if e[0] == 29:
                    logger.debug("MySQL is not ready yet.  Giving it 5 seconds.")
                    time.sleep(5)
                else:
                    raise e

    def run_post_initialization_commands(self):
        logger.debug("######## run_post_initialization_commands")
        for command in self.config["post_initialization"]:
            self.query(command)

    def write_replication_conf(self):
        f = open("%s/replication.cnf" % self.data_dir, "w")
        f.write("""
[mysqld]
read-only     = 1
""" % {"server_id": self.name})
        f.close()

    def clear_replication_conf(self):
        f = open("%s/replication.cnf" % self.data_dir, "w")
        f.write("""
[mysqld]
read-only     = 0
""" % {"server_id": self.name})
        f.close()

    def last_operation(self):
        logger.debug("######## last_operation")
        master_status = self.query("SHOW MASTER STATUS;").fetch_row()[0]
        position = None
        if master_status:
            position = master_status[0].split('.')[1] + '.{0:08d}'.format(int(master_status[1]))
        else:
            slave_status = self.query("SHOW SLAVE STATUS;").fetch_row()[0]
            position = slave_status[6].split('.')[1] + '.{0:08d}'.format(slave_status[7])
        logger.debug("last operation: %s" % position)
        return float(position)
