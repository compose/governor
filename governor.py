#!/usr/bin/env python

import sys, os, yaml, time, urllib2, atexit
import logging

from helpers.etcd import Etcd
from helpers.postgresql import Postgresql
from helpers.ha import Ha


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

f = open(sys.argv[1], "r")
config = yaml.load(f.read())
f.close()

etcd = Etcd(config["etcd"])
postgresql = Postgresql(config["postgresql"])
ha = Ha(postgresql, etcd)

# stop postgresql on script exit
def stop_postgresql():
    postgresql.stop()
atexit.register(stop_postgresql)

# wait for etcd to be available
def wait_for_etcd(message):
    etcd_ready = False
    while not etcd_ready:
        try:
            etcd.touch_member(postgresql.name, postgresql.connection_string)
            etcd_ready = True
        except urllib2.URLError:
            logging.info("waiting on etcd: %s" % message)
            time.sleep(5)

# is data directory empty?
if postgresql.data_directory_empty():
    # racing to initialize
    wait_for_etcd("cannot initialize member without ETCD")
    if etcd.race("/initialize", postgresql.name):
        postgresql.initialize()
        etcd.take_leader(postgresql.name)
        postgresql.start()
    else:
        synced_from_leader = False
        while not synced_from_leader:
            leader = etcd.current_leader()
            if not leader:
                time.sleep(5)
                continue
            if postgresql.sync_from_leader(leader):
                postgresql.write_recovery_conf(leader)
                postgresql.start()
                synced_from_leader = True
            else:
                time.sleep(5)
else:
    postgresql.follow_no_leader()
    postgresql.start()

wait_for_etcd("running in readonly mode; cannot participate in cluster HA without etcd")
while True:
    try:
        logging.info(ha.run_cycle())

        # create replication slots
        if postgresql.is_leader():
            for node in etcd.get_client_path("/members?recursive=true")["node"]["nodes"]:
                member = node["key"].split('/')[-1]
                if member != postgresql.name:
                    postgresql.query("DO LANGUAGE plpgsql $$DECLARE somevar VARCHAR; BEGIN SELECT slot_name INTO somevar FROM pg_replication_slots WHERE slot_name = '%(slot)s' LIMIT 1; IF NOT FOUND THEN PERFORM pg_create_physical_replication_slot('%(slot)s'); END IF; END$$;" % {"slot": member})

        etcd.touch_member(postgresql.name, postgresql.connection_string)

        time.sleep(config["loop_wait"])
    except urllib2.URLError:
        logging.info("Lost connection to etcd, setting no leader and waiting on etcd")
        postgresql.follow_no_leader()
        wait_for_etcd("running in readonly mode; cannot participate in cluster HA without etcd")
