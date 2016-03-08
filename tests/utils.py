# coding: utf-8
import subprocess


def _process(action):
    subprocess.call(('./start-solr-test-server.sh', action))


def prepare():
    _process("prepare")


def start_solr():
    _process("start")


def stop_solr():
    _process("stop")
