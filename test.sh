T=30

export GOPATH=$PWD
cd src/raft

for ((i=1;i<=${T};i++))
do
go test -run Election
done

cd ..
