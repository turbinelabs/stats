package handler

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/golang/mock/gomock"

	apihttp "github.com/turbinelabs/api/http"
	"github.com/turbinelabs/test/assert"
)

func TestNewAPIAuthorizerFromFlags(t *testing.T) {
	flagset := flag.NewFlagSet("auth options", flag.PanicOnError)

	ff := NewAPIAuthorizerFromFlags(flagset)
	ffImpl := ff.(*apiAuthFromFlags)

	assert.NonNil(t, ffImpl.clientFromFlags)
}

func TestAPIAuthFromFlagsMake(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	httpClient := &http.Client{}
	endpoint := apihttp.Endpoint{}

	clientFromFlags := apihttp.NewMockFromFlags(ctrl)
	clientFromFlags.EXPECT().MakeClient().Return(httpClient)
	clientFromFlags.EXPECT().MakeEndpoint().Return(endpoint, nil)

	logger := log.New(os.Stderr, "", log.LstdFlags)

	ff := &apiAuthFromFlags{clientFromFlags: clientFromFlags}
	auth, err := ff.Make(logger)
	assert.Nil(t, err)
	assert.NonNil(t, auth)
}

func TestAPIAuthFromFlagsMakeError(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	httpClient := &http.Client{}
	endpoint := apihttp.Endpoint{}
	makeErr := errors.New("")

	clientFromFlags := apihttp.NewMockFromFlags(ctrl)
	clientFromFlags.EXPECT().MakeClient().Return(httpClient)
	clientFromFlags.EXPECT().MakeEndpoint().Return(endpoint, makeErr)

	logger := log.New(os.Stderr, "", log.LstdFlags)

	ff := &apiAuthFromFlags{clientFromFlags: clientFromFlags}
	auth, err := ff.Make(logger)
	assert.DeepEqual(t, err, makeErr)
	assert.Nil(t, auth)
}
