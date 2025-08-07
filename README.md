# rabidfs

Proof-of-concept code following the presentation of "Parasitic Storage: Building RAID on Exposed S3 Buckets" at Summercon '25 in Brooklyn NY. See [abstract](https://www.summercon.org/presentations/#:~:text=PARASITIC%20STORAGE%3A%20BUILDING%20RAID%20ON%20EXPOSED%20S3%20BUCKETS) and [recording](https://www.youtube.com/live/TuKPA-CeDFA?si=Bd_3uRbvuQKWnefi&t=1114).

## Description

- RABID: Redundant Array of (Buckets of) Independent Data
- rabidfs: a filesystem implementing RABID via FUSE

## Getting started

### Prerequisites

Only tested on Ubuntu 24.04.2 LTS.

### Install

```
go install github.com/noperator/rabidfs/cmd/rabidfs@latest
```

### Usage

```
Usage of ./rabidfs:
  -b string
    	Path to the buckets file (default "buckets.txt")
  -data-shards int
    	Number of data shards for erasure coding (default 2)
  -m string
    	Path to the manifest file (default "manifest.json")
  -parity-shards int
    	Number of parity shards for erasure coding (default 1)
  -repair-interval duration
    	Interval between repair operations (default 5m0s)
```

## Back matter

### Legal disclaimer

Usage of this tool for attacking targets without prior mutual consent is illegal. It is the end user's responsibility to obey all applicable local, state, and federal laws. Developers assume no liability and are not responsible for any misuse or damage caused by this program.

### See also

- https://github.com/sa7mon/S3Scanner
- https://github.com/peak/s5cmd

### To-do

- [ ] make readme a bit more descriptive
- [ ] update readme with references to parasitic computing, other uses of parasitic storage
- [ ] upload test scripts
- [ ] trim down excessive logging

### License

This project is licensed under the [MIT License](LICENSE).
