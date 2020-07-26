numfiles:=25
minsize:=1024
maxsize:=$$((128*1024*1024))
concurrency:=4

all: bench

bench: m c
	/usr/bin/time -v ./m $(numfiles) $(minsize) $(maxsize) $(concurrency)
	/usr/bin/time -v ./c $(numfiles) $(minsize) $(maxsize) $(concurrency)

m: ./mattn/bench.go
	go build -o $@ $<

c: ./crawshaw/bench.go
	go build -o $@ $<
