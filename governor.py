#!/usr/bin/env python

import sys, os, yaml, time, urllib2, atexit, ssl
import logging

from helpers.etcdmanager import EtcdManager as Etcd
from helpers.postgresql import Postgresql
from helpers.ha import Ha

LOG_LEVEL = logging.DEBUG if os.getenv('DEBUG', None) else logging.INFO

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=LOG_LEVEL)


# stop postgresql on script exit
def stop_postgresql(postgresql):
    postgresql.stop()

# wait for etcd to be available
def wait_for_etcd(message, etcd, postgresql):
    etcd_ready = False
    while not etcd_ready:
        try:
            etcd.write_member(postgresql.name, postgresql.connection_string)
            etcd_ready = True
        except (urllib2.URLError, ssl.SSLError) as e:
            logging.info(e)
            logging.info("waiting on etcd: %s" % message)
            time.sleep(5)

def run(config):
    etcd = Etcd(config["etcd"])
    postgresql = Postgresql(config["postgresql"])
    ha = Ha(postgresql, etcd)

    atexit.register(stop_postgresql, postgresql)
    logging.info("Governor Starting up")
# is data directory empty?
    if postgresql.data_directory_empty():
        logging.info("Governor Starting up: Empty Data Dir")
        # racing to initialize
        wait_for_etcd("cannot initialize member without ETCD", etcd, postgresql)
        if etcd.race("/initialize", postgresql.name):
            logging.info("Governor Starting up: Initialisation Race ... WON!!!")
            logging.info("Governor Starting up: Initialise Postgres")
            postgresql.initialize()
            logging.info("Governor Starting up: Initialise Complete")
            etcd.take_leader(postgresql.name)
            logging.info("Governor Starting up: Starting Postgres")
            postgresql.start()
        else:
            logging.info("Governor Starting up: Initialisation Race ... LOST")
            logging.info("Governor Starting up: Sync Postgres from Leader")
            synced_from_leader = False
            while not synced_from_leader:
                leader = etcd.current_leader()
                if not leader:
                    time.sleep(5)
                    continue
                if postgresql.sync_from_leader(leader):
                    logging.info("Governor Starting up: Sync Completed")
                    postgresql.write_recovery_conf(leader)
                    logging.info("Governor Starting up: Starting Postgres")
                    postgresql.start()
                    synced_from_leader = True
                else:
                    time.sleep(5)
    else:
        logging.info("Governor Starting up: Existing Data Dir")
        postgresql.follow_no_leader()
        logging.info("Governor Starting up: Starting Postgres")
        postgresql.start()

    wait_for_etcd("running in readonly mode; cannot participate in cluster HA without etcd", etcd, postgresql)
    logging.info("Governor Running: Starting Running Loop")
    while True:
        try:
            logging.info("Governor Running: %s" % ha.run_cycle())

            # create replication slots
            if postgresql.is_leader():
                logging.info("Governor Running: I am the Leader")
                for hostname, address in etcd.members().iteritems():
                    if hostname != postgresql.name:
                        postgresql.query("DO LANGUAGE plpgsql $$DECLARE somevar VARCHAR; BEGIN SELECT slot_name INTO somevar FROM pg_replication_slots WHERE slot_name = '%(slot)s' LIMIT 1; IF NOT FOUND THEN PERFORM pg_create_physical_replication_slot('%(slot)s'); END IF; END$$;" % {"slot": hostname})
            etcd.write_member(postgresql.name, postgresql.connection_string)

            time.sleep(config["loop_wait"])
        except urllib2.URLError:
            logging.info("Lost connection to etcd, setting no leader and waiting on etcd")
            postgresql.follow_no_leader()
            wait_for_etcd("running in readonly mode; cannot participate in cluster HA without etcd", etcd, postgresql)

if __name__ == "__main__":
    f = open(sys.argv[1], "r")
    config = yaml.load(f.read())
    f.close()

    run(config)
