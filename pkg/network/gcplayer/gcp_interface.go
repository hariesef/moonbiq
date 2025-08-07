package gcplayer

//go:generate mockgen -source=gcp_interface.go -package=mocks -destination=../../../internal/mocks/gcp_mock.go

import "context"

type GcpLayer interface {
	Setup(ctx context.Context, bucketId string, projectId string, dataset string) error
	Close() error
	CheckTableExists(tablePath string, dataset string) (bool, string, error)
	CreateTable(tablePath string, dataset string, tableName string) error
	DeleteTable(dataset string, tableName string) error
	UploadToGcs(objectName string, file string) error
	LoadToBigQuery(gcsPath string, dataset string, tableName string) (int64, error)
	CopyTable(srcDataset, srcTable, dstDataset, dstTable string) error
	DeleteGcsFile(objectName string) error
}
