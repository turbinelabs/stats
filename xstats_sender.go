package stats

import "github.com/rs/xstats"

//go:generate $TBN_HOME/scripts/mockgen_internal.sh -type xstatsSender -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE -aux_files xstats=$TBN_HOME/vendor/github.com/rs/xstats/sender.go

type xstatsSender interface {
	xstats.Sender
}
