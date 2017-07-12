package stats

//go:generate $TBN_HOME/scripts/mockgen_internal.sh -type statsFromFlags -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

type statsFromFlags interface {
	Validate() error
	Make(classifyStatusCodes bool) (Stats, error)
}
