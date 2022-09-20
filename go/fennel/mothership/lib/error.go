package lib

import "net/http"

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

var ErrorDomainNotWhitelisted = UserReadableError{
	Msg:        "Registration is limited. Please contact fennel first.",
	StatusCode: http.StatusForbidden,
}
