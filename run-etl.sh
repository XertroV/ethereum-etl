#!/usr/bin/env bash

set -x
set -e

# set the contents of this file to be x where x*100_000 = the block number you want to start at; e.g.: `35` will start at block 3_500_000
export START_AT=$(cat ./.last_batch_started)
export END_AT=1501
# benchmark (start_at=35 w/ 100k chunk size)
# 1000,8: Took 0:03:08.529603
# 1000,12: Took 0:02:51.697720
# 100,12: Took 0:02:45.105367, 0:02:41.809742
# 100,32: Took 0:02:52.923711
# benchmark (start_at=1098 w/ 10k chunk size)
# 100,12: Took 0:06:04.250998
# 10,32: Took 0:06:02.047528.
export BATCH_SIZE=10
export WORKERS=12

. ./.provider_uri

for PREFIX in `seq $START_AT $END_AT`; do
  echo "Running for prefix: ${PREFIX}"
  echo -n "${PREFIX}" > .last_batch_started
  python3 ethereumetl.py export_blocks_extra -s ${PREFIX}0_000 -e ${PREFIX}9_999 \
    -b $BATCH_SIZE -w $WORKERS --blocks-output "${PREFIX}0_000__${PREFIX}9_999.csv" \
    --provider-uri $PROVIDER_URI \
    2>&1 | tee log_${PREFIX}0_000.txt || exit 1;
  test ${PIPESTATUS[0]} -eq 0 || exit 2;
done
