package lib

import (
	"net/http"
)

const (
	MaxRegionLength                = 16
	MaxCustomerNameLength          = 32
	MaxTierK8sNamespaceLength      = 32
	MaxAWSRoleLength               = 32
	MaxVPCIDLength                 = 32
	MaxEKSInstanceTypeLength       = 32
	MaxConfluentEnvironmentLength  = 32
	MaxConfluentClusterIDLength    = 32
	MaxConfluentClusterNameLength  = 32
	MaxClusterSecutityGroupLength  = 32
	MaxDBUsernameLength            = 32
	MaxDBPasswordLength            = 32
	MaxClusterIDLength             = 64
	MaxKafkaBootstrapServersLength = 128
	MaxKafkaAPIKeyLength           = 128
	MaxKafkaSecretKeyLength        = 128
	MaxHostnameLength              = 128
	MaxPulimiStackLength           = 128
	MaxTierAPIURLLength            = 256
)

type StandardEmailTemplate struct {
	MothershipEndpoint string
	Subject            string
	Title              string
	Year               int

	Desc    string // optional
	CTAText string // optional
	CTALink string // optional
}

type UserReadableError struct {
	Msg        string
	StatusCode int
}

func (err *UserReadableError) Error() string {
	return err.Msg
}

var ErrorWrongPassword = UserReadableError{
	Msg:        "Wrong password",
	StatusCode: http.StatusBadRequest,
}

var ErrorUserNotFound = UserReadableError{
	Msg:        "User not found",
	StatusCode: http.StatusBadRequest,
}

var ErrorNotConfirmed = UserReadableError{
	Msg:        "User not confirmed yet. Please confirm your email first.",
	StatusCode: http.StatusUnprocessableEntity,
}

var ErrorAlreadyConfirmed = UserReadableError{
	Msg:        "User email is already confirmed.",
	StatusCode: http.StatusUnprocessableEntity,
}

var ErrorUserAlreadySignedUp = UserReadableError{
	Msg:        "User already signed up",
	StatusCode: http.StatusUnprocessableEntity,
}

var ErrorBadEmail = UserReadableError{
	Msg:        "Bad email address",
	StatusCode: http.StatusBadRequest,
}
