# Run Test until test failed once

MAXT=10

export GOPATH=$PWD
cd src/raft

for ((i=1;i<=${MAXT};i++))
do

  go test -run FailNoAgree

  if [ $? -ne 0 ]; then
    echo "Failed in iteration "${i}
    break
  fi

done

cd ..
