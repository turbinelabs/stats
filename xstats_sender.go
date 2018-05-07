/*
Copyright 2018 Turbine Labs, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stats

import "github.com/rs/xstats"

// TODO currently this can't be executed from the open source project
//go:generate $TBN_HOME/scripts/mockgen_internal.sh -type xstatsSender -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE -aux_files xstats=vendor/github.com/rs/xstats/sender.go --write_package_comment=false

type xstatsSender interface {
	xstats.Sender
}
