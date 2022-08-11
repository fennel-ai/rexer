package db

import (
	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
)

type logger struct {
	delegatee *zap.SugaredLogger
}

func NewLogger(l *zap.Logger) *logger {
	return &logger{
		delegatee: l.Sugar(),
	}
}

var _ badger.Logger = (*logger)(nil)

func (l *logger) Errorf(f string, args ...interface{}) {
	l.delegatee.Errorf(f, args...)
}

func (l *logger) Infof(f string, args ...interface{}) {
	l.delegatee.Infof(f, args...)
}

func (l *logger) Warningf(f string, args ...interface{}) {
	l.delegatee.Warnf(f, args...)
}

func (l *logger) Debugf(f string, args ...interface{}) {
	l.delegatee.Debugf(f, args...)
}
