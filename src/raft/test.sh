count=20
    for i in $(seq $count); do
        go test -run 2A -race
    done