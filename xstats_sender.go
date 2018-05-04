package stats

import "github.com/rs/xstats"

// TODO currently this can't be executed from the open source project
//go:generate $TBN_HOME/scripts/mockgen_internal.sh -type xstatsSender -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE -aux_files xstats=vendor/github.com/rs/xstats/sender.go

type xstatsSender interface {
	xstats.Sender
}
