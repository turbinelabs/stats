
[//]: # ( Copyright 2018 Turbine Labs, Inc.                                   )
[//]: # ( you may not use this file except in compliance with the License.    )
[//]: # ( You may obtain a copy of the License at                             )
[//]: # (                                                                     )
[//]: # (     http://www.apache.org/licenses/LICENSE-2.0                      )
[//]: # (                                                                     )
[//]: # ( Unless required by applicable law or agreed to in writing, software )
[//]: # ( distributed under the License is distributed on an "AS IS" BASIS,   )
[//]: # ( WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or     )
[//]: # ( implied. See the License for the specific language governing        )
[//]: # ( permissions and limitations under the License.                      )

# turbinelabs/stats

**This project is no longer maintained by Turbine Labs, which has
[shut down](https://blog.turbinelabs.io/turbine-labs-is-shutting-down-and-our-team-is-joining-slack-2ad41554920c).**

[![Apache 2.0](https://img.shields.io/badge/license-apache%202.0-blue.svg)](LICENSE)
[![GoDoc](https://godoc.org/github.com/turbinelabs/stats?status.svg)](https://godoc.org/github.com/turbinelabs/stats)
[![CircleCI](https://circleci.com/gh/turbinelabs/stats.svg?style=shield)](https://circleci.com/gh/turbinelabs/stats)
[![Go Report Card](https://goreportcard.com/badge/github.com/turbinelabs/stats)](https://goreportcard.com/report/github.com/turbinelabs/stats)
[![codecov](https://codecov.io/gh/turbinelabs/stats/branch/master/graph/badge.svg)](https://codecov.io/gh/turbinelabs/stats)

The stats project provides a standard Stats interface to a variety of underlying
backends, along with a means to configure it from command line flags.

## Requirements

- Go 1.10.3 or later (previous versions may work, but we don't build or test against them)

## Dependencies

The stats project depends on these packages:

- [api](https://github.com/turbinelabs/api)
- [idgen](https://github.com/turbinelabs/idgen)
- [nonstdlib](https://github.com/turbinelabs/nonstdlib)

The tests depend on our [test package](https://github.com/turbinelabs/test),
and on [gomock](https://github.com/golang/mock). It should always be safe to use
HEAD of all master branches of Turbine Labs open source projects together, or to
vendor them with the same git tag.

[gomock](https://github.com/golang/mock)-based MockStats and MockFromFlags are provided.

<!-- remove if no vendored dependencies -->
Additionally, we vendor [github.com/rs/xstats](https://github.com/rs/xstats).
This should be considered an opaque implementation detail, see
[Vendoring](http://github.com/turbinelabs/developer/blob/master/README.md#vendoring)
for more discussion.

## Install

```
go get -u github.com/turbinelabs/stats/...
```

## Clone/Test

```
mkdir -p $GOPATH/src/turbinelabs
git clone https://github.com/turbinelabs/stats.git > $GOPATH/src/turbinelabs/stats
go test github.com/turbinelabs/stats/...
```

## Godoc

[`stats`](https://godoc.org/github.com/turbinelabs/stats)

## Versioning

Please see [Versioning of Turbine Labs Open Source Projects](http://github.com/turbinelabs/developer/blob/master/README.md#versioning).

## Pull Requests

Patches accepted! Please see [Contributing to Turbine Labs Open Source Projects](http://github.com/turbinelabs/developer/blob/master/README.md#contributing).

## Code of Conduct

All Turbine Labs open-sourced projects are released with a
[Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating in our
projects you agree to abide by its terms, which will be carefully enforced.
