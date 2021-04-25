count=10
    for i in $(seq $count); do
        go test -race
    done