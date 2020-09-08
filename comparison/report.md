---
title: Benchmark comparison of DARS with Thredds and Hyrax
author: Gaute Hope
autoEqnLabels: true
template: eisvogel
link-citations: true
csl: https://www.zotero.org/styles/the-journal-of-the-acoustical-society-of-america
---

# Benchmark comparison of Dars with Thredds and Hyrax

The three servers are started in docker containers on the same machine (at the
same time). The tests are run on each server sequentially. Since each test does
the same request multiple times we are measuring a warmed-up system.

## Criteria

These are the criteria the servers are compared on:

  * Errors
  * Maximum acceptable level of requests per seconds (will depend on response size)
  * Compare latency histograms under constant load at this level of requests between servers.

The acceptable level of requests per seconds, or the number of requests per
seconds a server can deliver without exhausting resources or encountering
errors will be very different. It will therefore probably be necessary to
compare latency histograms under different loads.

## Cases

The cases that will be compared are:

  * Fetch metadata (`.das` and `.dds`) for a dataset.
  * Fetch a small slice
  * Fetch a large slice

The dataset variables are shuffled and compressed using zlib DEFLATE (gzip).

## Measuring

First the
The latency and maximum (acceptable) request-per-seconds will be measured using `wrk2`. The system resource usage will be monitored using `docker stats`.

# Experiment

## Before starting

Idle servers show:

|           | CPU      | Memory    | PIDs  |
| :-------- | -------: | --------: | ----: |
| Thredds   | 0.1%     | 1.47GB    | 45    |
| Hyrax     | 0.1%     | 345MB     | 50    |
| Dars      | 0.02%    | 62MB      | 16    |


## Request per seconds

Server load:

das

|         | CPU | Memory | PIDs |
| --      | --  | ---    | --   |
| Dars    | 150 | 80mb   | 16   |
| Thredds | 550 | 2.2gb  | 47   |
| Hyrax   | 250 | 750mb  |      |

dds

|         | CPU | Memory | PIDs |
| --      | --  | ---    | --   |
| Dars    | 380 | 53mb   | 16   |
| Thredds | 650 | 2.2gb  | 47   |
| Hyrax   | 250 | 650    | 69   |

dods small

|         | CPU | Memory | PIDs |
| --      | --  | ---    | --   |
| Dars    | 760 | 80mb   | 16   |
| Thredds | 730 | 3gb    | 48   |
| Hyrax   | 650 | 730mb  | 69   |

dods big

|         | CPU | Memory | PIDs |
| --      | --  | ---    | --   |
| Dars    | 690 | 1.1gb  | 16   |
| Thredds | 500 | 5.2gb  | 48   |
| Hyrax   | 792 | 8gb    | 62   |

