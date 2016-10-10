import urllib3
import json
import os
import time
import base64
import ssl
import logging
from urllib import urlencode
import helpers.errors
import etcd
import ast
import urlparse

logger = logging.getLogger(__name__)


class EtcdManager:
    def __init__(self, config):
        url = urlparse.urlparse(config.get("endpoint"))
        self.client = etcd.Client(
            username=config.get("authentication", {}).get("username"),
            password=config.get("authentication", {}).get("password"),
            read_timeout=config.get("timeout", 5),
            host=url.hostname or "localhost",
            port=url.port or 4001,
            protocol=url.scheme or "http",
            version_prefix=config.get("version_prefix", "/v2")
        )
        self.scope = config["scope"]
        self.ttl = config["ttl"]

    def read(self, path, max_attempts=1, **kwargs):
        attempts = 0
        response = None
        while True:
            try:
                response = self.client.read("/" + self.scope + path, **kwargs)
                break
            except (etcd.EtcdException, urllib3.exceptions.TimeoutError) as e:
                attempts += 1
                if attempts < max_attempts:
                    logger.warning("Failed to return %s, trying again. (%s of %s)" % (path, attempts, max_attempts))
                    time.sleep(3)
                else:
                    raise e
        return response

    def _value_to_dict(self, etcdresponse):
        try:
            return ast.literal_eval(etcdresponse.value)
        except:
            return etcdresponse.value

    def refresh(self, path, ttl):
        self.client.refresh("/" + self.scope + path, ttl=ttl)

    def write(self, path, data, **kwargs):
        self.client.write("/" + self.scope + path, data, **kwargs)

    def current_leader(self):
        try:
            hostname = self._value_to_dict(self.read("/leader"))
            address = self._value_to_dict(self.read("/members/%s" % hostname))
            return {"hostname": hostname, "address": address}
        except etcd.EtcdException:
            raise helpers.errors.CurrentLeaderError("Etcd is not responding properly")

    def members(self):
        try:
            members = []

            r = self.read("/members", recursive=True)
            for node in r.leaves:
                members.append({"hostname": node.key.split('/')[-1], "address": node.value})
            return members
        except etcd.EtcdException:
            raise helpers.errors.CurrentLeaderError("Etcd is not responding properly")

    def write_member(self, member, connection_string):
        self.write("/members/{member}".format(member=member), connection_string, ttl=self.ttl)

    def touch_member(self, member):
        try:
            self.refresh("/members/{member}".format(member=member), self.ttl)
        except etcd.EtcdKeyNotFound:
            logger.error("Tried touching non-existent member")

    def take_leader(self, value):
        try:
            self.write("/leader", value, ttl=self.ttl)
            return True
        except etcd.EtcdException:
            logger.error("Error taking leader.")
            return False

    def attempt_to_acquire_leader(self, value):
        try:
            return self.write("/leader", value, ttl=self.ttl, prevExist=False)
        except etcd.EtcdAlreadyExist:
            return False

    def update_leader(self, state_handler):
        try:
            self.write("/leader", state_handler.name, ttl=self.ttl, prevValue=state_handler.name)
            self.write("/optime/leader", state_handler.last_operation())
        except (etcd.EtcdException) as e:
            logger.error("Error updating leader lock and optime on ETCD for primary.")
            logger.error(e)
            return False

    def last_leader_operation(self):
        try:
            return int(self._value_to_dict(self.read("/optime/leader")))
        except etcd.EtcdException as e:
            logger.error("Error updating TTL on ETCD for primary.")
            return None

    def leader_unlocked(self):
        try:
            self.read("/leader")
            return False
        except etcd.EtcdKeyNotFound as e:
            return True
        except etcd.EtcdException:
            return False

    def am_i_leader(self, value):
        try:
            response = self._value_to_dict(self.read("/leader"))
            logger.info("Lock owner: {owner}; I am {me}".format(owner=response, me=value))
            return response == value
        except etcd.EtcdException:
            logger.error("Couldn't reach etcd")
            return False

    def race(self, path, value):
        while True:
            try:
                self.write(path, value, prevExist=False)
                return True
            except etcd.EtcdAlreadyExist:
                return False
            except etcd.EtcdException:
                    logger.warning("etcd is not ready for connections")
                    time.sleep(10)
