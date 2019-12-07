T=20
N=0

export GOPATH=$PWD
cd src/raft

for ((i=1;i<=${T};i++))
do

# Lab 1 tests
go test -run Election && \

# Lab 2 part 1 tests
go test -run FailNoAgree && \
go test -run ConcurrentStarts && \
go test -run Rejoin && \
go test -run Backup && \

# Lab 2 part 2 tests
go test -run Persist1 && \
go test -run Persist2 && \
go test -run Persist3 && \

# More tests
go test -run Count && \
go test -run Figure8 && \
go test -run UnreliableAgree && \
go test -run Figure8Unreliable && \
go test -run ReliableChurn && \
go test -run UnreliableChurn && \

#echo "Passed "${i}
N=$[N+1]

done

echo "Passed "${N}"/"${T} 

cd ..
