#!/usr/bin/env bash

set -x
set -e

export START_AT=$(cat ./.last_batch_started)
export END_AT=139
# benchmark (start_at=35)
# 1000,8: Took 0:03:08.529603
# 1000,12: Took 0:02:51.697720
# 100,12: Took 0:02:45.105367, 0:02:41.809742
# 100,32: Took 0:02:52.923711
export BATCH_SIZE=100
export WORKERS=12

. ./.provider_uri

for PREFIX in `seq $START_AT $END_AT`; do
  echo "Running for prefix: ${PREFIX}"
  echo -n "${PREFIX}" > .last_batch_started
  python3 ethereumetl.py export_blocks_extra -s ${PREFIX}00_000 -e ${PREFIX}99_999 \
    -b $BATCH_SIZE -w $WORKERS --blocks-output "${PREFIX}00_000__${PREFIX}99_999.csv" \
    --provider-uri $PROVIDER_URI \
    2>&1 | tee log_${PREFIX}00_000.txt || exit 1;
done
