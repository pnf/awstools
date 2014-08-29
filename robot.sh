#!/bin/sh

set -uo pipefail

cd $HOME

date > $HOME/ROBODATE

(curl -f http://169.254.169.254/latest/user-data | sh -v) >& $HOME/ROBOSCRIPT

