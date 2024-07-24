#!/bin/bash

eval "$(conda shell.bash hook)"
conda activate py310
python ./get-latest-tickers.py
bqn -f ./portfolio-stats.bqn


