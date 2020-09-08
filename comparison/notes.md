# Comparisons between DARS, Thredds and Hyrax

Topics to measure:

  * Errors
  * Maximum acceptable requests-per-second
  * Latency at that level
  * Load at that level

Cases:

  * Metadata
  * Small slice
  * Big slice

## Measurement

  * pidstat or similar to log cpu + memory
  * wrk2 --latency on case urls, find acceptable RPS for all, find acceptable for dars
  * record latency for both
  * all servers in docker containers
  * (it would be nice to do a test scenario with several consequent urls simulating a data fetch, and also measure cold-start time)

## URLs


