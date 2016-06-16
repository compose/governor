import os, re, time
import logging
import _mysql as msycosql
import _mysql_exceptions
import subprocess

from urlparse import urlparse


logger = logging.getLogger(__name__)

class MySQL:

    def __init__(self, config):
        logger.info("######## __init__")
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
        logger.info("######## conn")
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
        logger.info("######## disconnect")
        try:
            self.conn().close()
        except Exception as e:
            logger.error("Error disconnecting: %s" % e)

    def query(self, sql):
        logger.info("######## query")
        max_attempts = 0
        result = None
        while True:
            try:
                logger.info("###### Trying query: %s" % sql)
                self.conn().query(sql)
                result = self.conn().store_result()
                break
            except _mysql_exceptions.OperationalError as e:
                logger.info('###### rescued and will reconnect')
                if self.conn():
                    self.disconnect()
                self.conn_holder = None
                if max_attempts > 4:
                    raise e
                max_attempts += 1
                time.sleep(5)
        return result

    def data_directory_empty(self):
        logger.info("######## data_directory_empty")
        return not os.path.exists(self.data_dir) or os.listdir(self.data_dir) == []

    def initialize(self):
        logger.info("######## initializing: mysqld %s" % self.initdb_options())
        if subprocess.call("mysqld %s" % self.initdb_options(), shell=True) == 0:
            # start MySQL without options to setup replication user indepedent of other system settings
            logger.info("######## starting: mysqld %s" % self.initdb_options())
            self.start()
            self.create_replication_user()
            self.stop()

            return True

        return False

    def sync_from_leader(self, leader):
        logger.info("######## sync_from_leader")
        leader = urlparse(leader["address"])

        subprocess.call("mysqldump --host %(hostname)s --port %(port)s -u %(username)s --all-databases --master-data > /tmp/sync-from-leader.db" %
                {"hostname": leader.hostname, "port": leader.port, "username": leader.username, "password": leader.password}, shell=True)

        self.initialize()
        self.start()
        self.query("SELECT true;") # wait for mysql

        return subprocess.call("mysql -u root --socket %s < /tmp/sync-from-leader.db" % self.socket, shell=True) == 1

    def is_leader(self):
        logger.info("######## is_leader")
        return self.query("SHOW SESSION VARIABLES WHERE variable_name = 'read_only';").fetch_row()[0][1] == "OFF"

    def is_running(self):
        if self.mysql_process:
            logger.info("######## is_running: %s", self.mysql_process.poll())
            return self.mysql_process.poll() == None # if no status, then the process is running
        else:
            logger.info("######## is_running: never started")
            return False

    def start(self):
        logger.info("######## start")
        if self.is_running():
            logger.error("Cannot start MySQL because one is already running.")
            return False

        logger.info("######## starting mysql")
        self.mysql_process = subprocess.Popen("mysqld %s" % self.server_options(), shell=True)

        while not self.is_ready():
            time.sleep(3)
        return self.mysql_process.poll() == None

    def is_ready(self):
        return self.is_running() and subprocess.call("mysqladmin status -u root --socket %s" % self.socket, shell=True) == 0

    def stop(self):
        logger.info("######## stop")
        if subprocess.call("mysqladmin shutdown -u root --socket %s" % self.socket, shell=True) == 0:
            return self.mysql_process.wait() == 0

    def reload(self):
        logger.info("######## reload")
        return subprocess.call("mysqladmin reload -u root --socket %s" % self.socket, shell=True) == 0

    def restart(self):
        logger.info("######## restart")
        return self.stop() and self.start()

    def server_options(self):
        logger.info("######## server_options")
        options = "--datadir %s --bind-address %s --port %s --socket %s" % (self.data_dir, self.host, self.port, self.socket)
        for setting, value in self.config["parameters"].iteritems():
            options += " --%s=%s" % (setting, value)
        return options

    def initdb_options(self):
        logger.info("######## initdb_options")
        options = "--initialize-insecure %s" % self.server_options()
        if "initdb_parameters" in self.config:
            for param in self.config["initdb_parameters"]:
                options += " %s" % param

        return options

    def is_healthy(self):
        logger.info("######## is_healthy")
        if not self.is_running():
            logger.warning("MySQL is not running.")
            return False

        if self.is_leader():
            return True

        return True

    def is_healthiest_node(self, state_store):
        logger.info("######## is_healthiest_node")
        # this should only happen on initialization
        if state_store.last_leader_operation() is None:
            return True

        if (state_store.last_leader_operation() - self.xlog_position()) > self.config["maximum_lag_on_failover"]:
            return False

        for member in state_store.members():
            if member["hostname"] == self.name:
                continue
            try:
                member_conn = psycopg2.connect(member["address"])
                member_conn.autocommit = True
                member_cursor = member_conn.cursor()
                member_cursor.execute("SELECT %s - (pg_last_xlog_replay_location() - '0/000000'::pg_lsn) AS bytes;" % self.xlog_position())
                xlog_diff = member_cursor.fetchone()[0]
                logger.info([self.name, member["hostname"], xlog_diff])
                if xlog_diff < 0:
                    member_cursor.close()
                    return False
                member_cursor.close()
            except psycopg2.OperationalError:
                continue
        return True

    def follow_the_leader(self, leader_hash):
        logger.info("######## follow_the_leader")
        leader = urlparse(leader_hash["address"])
        if subprocess.call("grep 'host=%(hostname)s port=%(port)s' %(data_dir)s/recovery.conf > /dev/null" % {"hostname": leader.hostname, "port": leader.port, "data_dir": self.data_dir}, shell=True) != 0:
            self.write_recovery_conf(leader_hash)
            self.restart()
        return True

    def follow_no_leader(self):
        logger.info("######## follow_no_leader")
        if not os.path.exists("%s/recovery.conf" % self.data_dir) or subprocess.call("grep primary_conninfo %(data_dir)s/recovery.conf &> /dev/null" % {"data_dir": self.data_dir}, shell=True) == 0:
            self.write_recovery_conf(None)
            if self.is_running():
                self.restart()
        return True

    def promote(self):
        logger.info("######## promote")
        return subprocess.call("pg_ctl promote -w -D %s" % self.data_dir, shell=True) == 0

    def demote(self, leader):
        logger.info("######## demote")
        self.write_recovery_conf(leader)
        self.restart()

    def create_replication_user(self):
        logger.info("######## create_replication_user")
        success = False

        while not success:
            try:
                self.query("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" % (self.replication["username"], self.replication["network"], self.replication["password"]))
                self.query("GRANT SELECT, PROCESS, FILE, SUPER, REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO '%s'@'%s';" % (self.replication["username"], self.replication["network"]))
                success = True
            except _mysql_exceptions.InternalError as e:
                if e[0] == 29:
                    logger.info("MySQL is not ready yet.  Giving it 5 seconds.")
                    time.sleep(5)
                else:
                    raise e

    def xlog_position(self):
        logger.info("######## xlog_position")
        slave_status = self.query("SHOW SLAVE STATUS;").fetchone()
        return slave_status[6] + '{0:08d}'.format(slave_status[7])

    def last_operation(self):
        logger.info("######## last_operation")
        if self.is_leader():
            master_status = self.query("SHOW MASTER STATUS;").fetch_row()
            return master_status[0][0] + '{0:08d}'.format(int(master_status[0][1]))
        else:
            return self.xlog_position()
