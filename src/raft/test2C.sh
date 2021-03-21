count=50
    for i in $(seq $count); do
        go test -run 2C -race
    done