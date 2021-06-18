count=10
    for i in $(seq $count); do
        go test -run 3A -race
    done