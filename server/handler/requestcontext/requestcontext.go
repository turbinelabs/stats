package requestcontext

import (
	"github.com/turbinelabs/server/handler/requestcontext"
)

const (
	// Key                                      -- saves data as
	OrgKey requestcontext.RequestKey = "orgKey" // api.OrgKey
)

// Generate wrapper

//go:generate genny -in=$TBN_HOME/server/handler/requestcontext/requestcontext_wrapper.genny -out=gen_requestcontext.go -pkg=requestcontext gen "DUMMY=X"
//go:generate genny -in=$TBN_HOME/server/handler/requestcontext/requestcontext_wrapper_test.genny -out=gen_requestcontext_test.go -pkg=requestcontext gen "DUMMY=X"

// Generate methods for specific fields
// - OrgKey
//go:generate genny -in=$TBN_HOME/server/handler/requestcontext/requestcontext_field.genny -out=gen_orgkey.go -pkg=requestcontext gen "AttrType=api.OrgKey AttrName=OrgKey"
//go:generate genny -in=$TBN_HOME/server/handler/requestcontext/requestcontext_field_test.genny -out=gen_orgkey_test.go -pkg=requestcontext gen "AttrType=api.OrgKey AttrName=OrgKey"
