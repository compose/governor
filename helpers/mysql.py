import os, re, time
import logging
import _mysql as msycosql
import _mysql_exceptions

from urlparse import urlparse


logger = logging.getLogger(__name__)

class MySQL:

    def __init__(self, config):
        print "######## __init__"
        self.name = config["name"]
        self.host, self.port = config["listen"].split(":")
        self.socket = config["socket"]
        self.data_dir = config["data_dir"]
        self.replication = config["replication"]

        self.config = config

        self.conn_holder = None
        self.connection_string = "postgres://%s:%s@%s:%s/postgres" % (self.replication["username"], self.replication["password"], self.host, self.port)

    def conn(self):
        print "######## conn"
        if not self.conn_holder:
            max_attempts = 0
            while True:
                try:
                    self.conn_holder = msycosql.connect(unix_socket=self.socket, user='root', db='mysql')
                    break
                except _mysql_exceptions.OperationalError as e:
                    self.conn_holder = None
                    if max_attempts > 4:
                        raise e
                    max_attempts += 1
                    time.sleep(5)
        return self.conn_holder

    def disconnect(self):
        print "######## disconnect"
        try:
            self.conn().close()
        except Exception as e:
            logger.error("Error disconnecting: %s" % e)

    def query(self, sql):
        print "######## query"
        max_attempts = 0
        result = None
        while True:
            try:
                print "###### Trying query: %s" % sql
                self.conn().query(sql)
                result = self.conn().store_result()
                break
            except _mysql_exceptions.OperationalError as e:
                print '###### rescued and will reconnect'
                if self.conn():
                    self.disconnect()
                self.conn_holder = None
                if max_attempts > 4:
                    raise e
                max_attempts += 1
                time.sleep(5)
        return result

    def data_directory_empty(self):
        print "######## data_directory_empty"
        return not os.path.exists(self.data_dir) or os.listdir(self.data_dir) == []

    def initialize(self):
        print "######## initialize"
        if os.system("mysqld %s" % self.initdb_options()) == 0:
            # start Postgres without options to setup replication user indepedent of other system settings
            os.system("mysqld %s &" % self.server_options())
            self.create_replication_user()
            os.system("mysqladmin shutdown -u root --socket %s" % self.socket)

            return True

        return False

    def sync_from_leader(self, leader):
        print "######## sync_from_leader"
        leader = urlparse(leader["address"])

        os.system("mysqldump --host %(hostname)s --port %(port)s -u %(username)s --all-databases --master-data > /tmp/sync-from-leader.db" %
                {"hostname": leader.hostname, "port": leader.port, "username": leader.username, "password": leader.password})

        self.initialize()
        self.start()
        self.query("SELECT true;") # wait for mysql

        return os.system("mysql -u root --socket %s < /tmp/sync-from-leader.db" % self.socket) == 1

    def is_leader(self):
        print "######## is_leader"
        return self.query("SHOW SESSION VARIABLES WHERE variable_name = 'read_only';").fetch_row()[0][1] == "OFF"

    def is_running(self):
        print "######## is_running"
        return os.system("mysqladmin status -u root --socket %s > /dev/null" % self.socket) == 0

    def start(self):
        print "######## start"
        if self.is_running():
            logger.error("Cannot start MySQL because one is already running.")
            return False

        print "######## starting mysql"
        return os.system("mysqld %s &" % self.server_options()) == 0

    def stop(self):
        print "######## stop"
        return os.system("mysqladmin shutdown -u root --socket %s" % self.socket) != 0

    def reload(self):
        print "######## reload"
        return os.system("mysqladmin reload -u root --socket %s" % self.socket) == 0

    def restart(self):
        print "######## restart"
        return self.stop() and self.start()

    def server_options(self):
        print "######## server_options"
        options = "--datadir %s --bind-address %s --port %s --socket %s" % (self.data_dir, self.host, self.port, self.socket)
        for setting, value in self.config["parameters"].iteritems():
            options += " --%s=%s" % (setting, value)
        return options

    def initdb_options(self):
        print "######## initdb_options"
        options = "--initialize-insecure %s" % self.server_options()
        if "initdb_parameters" in self.config:
            for param in self.config["initdb_parameters"]:
                options += " %s" % param

        return options

    def is_healthy(self):
        print "######## is_healthy"
        if not self.is_running():
            logger.warning("Postgresql is not running.")
            return False

        if self.is_leader():
            return True

        return True

    def is_healthiest_node(self, state_store):
        print "######## is_healthiest_node"
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
        print "######## follow_the_leader"
        leader = urlparse(leader_hash["address"])
        if os.system("grep 'host=%(hostname)s port=%(port)s' %(data_dir)s/recovery.conf > /dev/null" % {"hostname": leader.hostname, "port": leader.port, "data_dir": self.data_dir}) != 0:
            self.write_recovery_conf(leader_hash)
            self.restart()
        return True

    def follow_no_leader(self):
        print "######## follow_no_leader"
        if not os.path.exists("%s/recovery.conf" % self.data_dir) or os.system("grep primary_conninfo %(data_dir)s/recovery.conf &> /dev/null" % {"data_dir": self.data_dir}) == 0:
            self.write_recovery_conf(None)
            if self.is_running():
                self.restart()
        return True

    def promote(self):
        print "######## promote"
        return os.system("pg_ctl promote -w -D %s" % self.data_dir) == 0

    def demote(self, leader):
        print "######## demote"
        self.write_recovery_conf(leader)
        self.restart()

    def create_replication_user(self):
        print "######## create_replication_user"
        self.query("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" % (self.replication["username"], self.replication["network"], self.replication["password"]))
        self.query("GRANT SELECT, PROCESS, FILE, SUPER, REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO '%s'@'%s';" % (self.replication["username"], self.replication["network"]))

    def xlog_position(self):
        print "######## xlog_position"
        slave_status = self.query("SHOW SLAVE STATUS;").fetchone()
        return slave_status[6] + '{0:08d}'.format(slave_status[7])

    def last_operation(self):
        print "######## last_operation"
        if self.is_leader():
            master_status = self.query("SHOW MASTER STATUS;").fetch_row()
            return master_status[0] + '{0:08d}'.format(master_status[1])
        else:
            return self.xlog_position()
