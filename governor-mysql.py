#!/usr/bin/env python

import sys, os, yaml, time, urllib2, atexit
import logging

from helpers.etcd import Etcd
from helpers.mysql import MySQL
from helpers.ha import Ha

LOG_LEVEL = logging.DEBUG if os.getenv('DEBUG', None) else logging.INFO

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=LOG_LEVEL)


# stop mysql on script exit
def stop_mysql(mysql):
    mysql.stop()

# wait for etcd to be available
def wait_for_etcd(message, etcd, mysql):
    etcd_ready = False
    while not etcd_ready:
        try:
            etcd.touch_member(mysql.name, mysql.connection_string)
            etcd_ready = True
        except urllib2.URLError:
            logging.info("waiting on etcd: %s" % message)
            time.sleep(5)

def run(config):
    etcd = Etcd(config["etcd"])
    mysql = MySQL(config["mysql"])
    ha = Ha(mysql, etcd)

    atexit.register(stop_mysql, mysql)
    logging.info("Governor Starting up")
# is data directory empty?
    if mysql.data_directory_empty():
        logging.info("Governor Starting up: Empty Data Dir")
        # racing to initialize
        wait_for_etcd("cannot initialize member without ETCD", etcd, mysql)
        if etcd.race("/initialize", mysql.name):
            logging.info("Governor Starting up: Initialisation Race ... WON!!!")
            logging.info("Governor Starting up: Initialise MySQL")
            mysql.initialize()
            logging.info("Governor Starting up: Initialise Complete")
            etcd.take_leader(mysql.name)
            logging.info("Governor Starting up: Starting MySQL")
            mysql.start()
        else:
            logging.info("Governor Starting up: Initialisation Race ... LOST")
            logging.info("Governor Starting up: Sync MySQL from Leader")
            synced_from_leader = False
            while not synced_from_leader:
                leader = etcd.current_leader()
                if not leader:
                    time.sleep(5)
                    continue
                if mysql.sync_from_leader(leader):
                    logging.info("Governor Starting up: Sync Completed")
                    mysql.write_replication_conf()
                    logging.info("Governor Starting up: Starting MySQL")
                    mysql.start()
                    synced_from_leader = True
                else:
                    time.sleep(5)
    else:
        logging.info("Governor Starting up: Existing Data Dir")
        mysql.follow_no_leader()
        logging.info("Governor Starting up: Starting MySQL")
        mysql.start()

    wait_for_etcd("running in readonly mode; cannot participate in cluster HA without etcd", etcd, mysql)
    logging.info("Governor Running: Starting Running Loop")
    while True:
        try:
            logging.info("Governor Running: %s" % ha.run_cycle())
            etcd.touch_member(mysql.name, mysql.connection_string)

            time.sleep(config["loop_wait"])
        except urllib2.URLError:
            logging.info("Lost connection to etcd, setting no leader and waiting on etcd")
            mysql.follow_no_leader()
            wait_for_etcd("running in readonly mode; cannot participate in cluster HA without etcd", etcd, mysql)

if __name__ == "__main__":
    f = open(sys.argv[1], "r")
    config = yaml.load(f.read())
    f.close()

    run(config)
