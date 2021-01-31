package server

import (
	"encoding/json"
	"errors"
	"github.com/rs/xid"
	"go.uber.org/zap"
	"io/ioutil"
	"mx/internal/storage/postgresql"
	"mx/internal/task"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
)

type productLister interface {
	List(context.Context, ...postgresql.ListOption) ([]postgresql.Product, error)
}

type handler struct {
	logger    *zap.Logger
	scheduler *task.Scheduler
	db        *postgresql.Storage
}

func (h *handler) handleUpload(w http.ResponseWriter, r *http.Request) {
	taskID := xid.New()
	logger := h.logger.With(zap.String("task_id", taskID.String()))
	logger.Info("Upload handler invocation")

	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		http.Error(w, "Request query can not be parsed", http.StatusBadRequest)
		return
	}

	merchantIDString := q.Get("merchant_id")
	if merchantIDString == "" {
		http.Error(w, "Query value for merchant_id parameter can not be blank", http.StatusBadRequest)
		return
	}

	merchantID, err := strconv.ParseInt(merchantIDString, 10, 64)
	if err != nil {
		http.Error(w, "Query value for merchant_id parameter must represent integer", http.StatusBadRequest)
		return
	}

	if merchantID <= 0 {
		http.Error(w, "Query value for merchant_id parameter must be positive integer greater than zero", http.StatusBadRequest)
		return
	}

	err = os.MkdirAll(merchantIDString, 0750)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	filePath := filepath.Join(merchantIDString, taskID.String()+".xlsx")
	file, err := os.Create(filePath)
	if err != nil {
		h.logger.Error("Creating file", zap.Error(err))
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	f, fh, err := r.FormFile("workbook")
	if err != nil {
		h.logger.Error("Retrieving multipart file", zap.Error(err))

		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer f.Close()

	logger.Info("File info: ", zap.String("name", fh.Filename), zap.Int64("size", fh.Size))

	data, err := ioutil.ReadAll(f)
	if err != nil {
		h.logger.Error("Reading file data", zap.Error(err))
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	_, err = file.Write(data)
	if err != nil {
		h.logger.Error("Writing file data on disk", zap.Error(err))
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	h.scheduler.NewTask(taskID, merchantID, filePath)

	w.Header().Set("Location", "http://172.20.224.1:8080/tasks?id="+taskID.String())
	w.WriteHeader(http.StatusOK)
	return
}

func (h *handler) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		http.Error(w, "Request query can not be parsed", http.StatusBadRequest)
		return
	}

	taskID := q.Get("id")
	if taskID == "" {
		http.Error(w, "Query value for id parameter can not be blank", http.StatusBadRequest)
		return
	}

	taskStatus, err := h.scheduler.ReadTaskStatus(taskID)
	if err != nil {
		switch {
		case errors.Is(err, task.ErrBadTaskID):
			http.Error(w, "Bad task id", http.StatusBadRequest)
			return
		default:
			h.logger.Error("Reading task status", zap.Error(err))
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	}

	_, err = w.Write([]byte(taskStatus))
	if err != nil {
		h.logger.Error("Writing response", zap.Error(err))
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
	return
}

func (h *handler) listProducts(w http.ResponseWriter, r *http.Request) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		http.Error(w, "Request query can not be parsed", http.StatusBadRequest)
		return
	}

	var listOpts []postgresql.ListOption

	merchantIDValues, ok := q["merchant_id"]
	if ok {
		merchantID, err := strconv.ParseInt(merchantIDValues[0], 10, 64)
		if err != nil {
			http.Error(w, "Query value for merchant_id parameter must represent integer", http.StatusBadRequest)
			return
		}

		if merchantID <= 0 {
			http.Error(w, "Query value for merchant_id parameter must be positive integer greater than zero", http.StatusBadRequest)
			return
		}

		listOpts = append(listOpts, postgresql.WithMerchantID(merchantID))
	}

	offerIDValues, ok := q["offer_id"]
	if ok {
		offerID, err := strconv.ParseInt(offerIDValues[0], 10, 64)
		if err != nil {
			http.Error(w, "Query value for merchant_id parameter must represent integer", http.StatusBadRequest)
			return
		}

		if offerID <= 0 {
			http.Error(w, "Query value for merchant_id parameter must be positive integer greater than zero", http.StatusBadRequest)
			return
		}

		listOpts = append(listOpts, postgresql.WithOfferID(offerID))
	}

	nameQueryValues, ok := q["name"]
	if ok {
		nameQuery := nameQueryValues[0]
		if nameQuery == "" {
			http.Error(w, "Query value for name parameter can not be blank", http.StatusBadRequest)
			return
		}

		listOpts = append(listOpts, postgresql.WithNameQuery(nameQuery))
	}

	products, err := h.db.List(r.Context(), listOpts...)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	payload, err := json.Marshal(products)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(payload)
	if err != nil {
		h.logger.Error("Writing response", zap.Error(err))
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
	return
}
