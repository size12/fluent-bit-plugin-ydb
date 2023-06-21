package storage

import (
	"github.com/size12/fluent-bit-plugin-ydb/config"
	"github.com/size12/fluent-bit-plugin-ydb/model"
)

type Storager interface {
	Init() error
	Write(event []*model.Event) error
	Exit() error
}

func NewStorage(cfg config.Config) Storager {
	return &YDB{cfg: cfg}
}
