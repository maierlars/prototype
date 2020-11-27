#!/usr/bin/env bash

build/prototype cluster/4001 4001 localhost:4002 > cluster/4001.log &
build/prototype cluster/4002 4002 > cluster/4002.log &

