package bigquery

import (
	"errors"
	"testing"

	"moonbiq/internal/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestBigQueryStorage_Write(t *testing.T) {
	t.Run("error when checking table exists", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockGcp := mocks.NewMockGcpLayer(ctrl)
		bq := &BigQueryStorage{
			gcpLayer: mockGcp,
			config:   make(map[string]interface{}),
		}

		tablePath := "project.dataset.table"
		files := []string{"file1.csv"}

		mockGcp.EXPECT().CheckTableExists(tablePath, gomock.Any()).Return(false, "", errors.New("check failed"))

		err := bq.Write(tablePath, files, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "check failed")
	})

	t.Run("not continuing upload when table exists", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockGcp := mocks.NewMockGcpLayer(ctrl)
		bq := &BigQueryStorage{
			gcpLayer: mockGcp,
			config:   make(map[string]interface{}),
		}

		tablePath := "project.dataset.table"
		files := []string{"file1.csv"}

		mockGcp.EXPECT().CheckTableExists(tablePath, gomock.Any()).Return(true, "table-name", nil)

		err := bq.Write(tablePath, files, false)
		assert.NoError(t, err)
	})

	t.Run("successful upload E2E", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockGcp := mocks.NewMockGcpLayer(ctrl)
		config := make(map[string]interface{})
		config["projectId"] = "project-id"
		config["gcsBucket"] = "gcs-bucket"
		config["tempDir"] = "temp"
		config["useTempDataset"] = true
		config["tempDataSet"] = "temp-dataset"
		config["finalDataset"] = "final-dataset"
		bq := &BigQueryStorage{
			gcpLayer: mockGcp,
			config:   config,
		}

		tablePath := "config/table/clients.json"
		files := []string{"file1.jsonl.gz"}

		mockGcp.EXPECT().CheckTableExists(tablePath, gomock.Any()).Return(true, "table-name", nil)
		mockGcp.EXPECT().DeleteTable(gomock.Any(), gomock.Any()).Return(nil)
		mockGcp.EXPECT().CreateTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		mockGcp.EXPECT().UploadToGcs(gomock.Any(), gomock.Any()).Return(nil)
		mockGcp.EXPECT().LoadToBigQuery(gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil)
		mockGcp.EXPECT().DeleteGcsFile(gomock.Any()).Return(nil)

		err := bq.Write(tablePath, files, true)
		assert.NoError(t, err)
	})

}
