#!/bin/sh
set -e
shift
exec mix run --no-halt -- "$@"
