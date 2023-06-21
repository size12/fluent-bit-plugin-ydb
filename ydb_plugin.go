package main

import (
	"C"
	"log"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/size12/fluent-bit-plugin-ydb/config"
	"github.com/size12/fluent-bit-plugin-ydb/model"
	"github.com/size12/fluent-bit-plugin-ydb/storage"
)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	log.Println("[ydb] Exporting plugin.")
	return output.FLBPluginRegister(def, "ydb", "YDB storage")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	connURL := output.FLBPluginConfigKey(plugin, "ConnectionURL")
	tableName := output.FLBPluginConfigKey(plugin, "Table")

	if connURL == "" {
		log.Println("Please provide connection url for YDB.")
		return output.FLB_ERROR
	}

	if tableName == "" {
		log.Println("Please provide table name for YDB.")
		return output.FLB_ERROR
	}

	cfg := config.Config{
		ConnectionURL: connURL,
		TableName:     tableName,
	}

	s := storage.NewStorage(cfg)

	err := s.Init()

	if err != nil {
		log.Printf("Failed init YDB: %v\n", err)
	}

	log.Println("[ydb] Exported plugin.")
	output.FLBPluginSetContext(plugin, s)
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	log.Println("[ydb] Flush called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	s, ok := output.FLBPluginGetContext(ctx).(storage.Storager)
	if !ok {
		return output.FLB_ERROR
	}

	dec := output.NewDecoder(data, int(length))
	count := 0
	var events []*model.Event

	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		var timestamp time.Time
		switch t := ts.(type) {
		case output.FLBTime:
			timestamp = ts.(output.FLBTime).Time
		case uint64:
			timestamp = time.Unix(int64(t), 0)
		default:
			timestamp = time.Now()
		}

		message := make(map[string]interface{})

		for k, v := range record {
			key, ok := k.(string)
			if !ok {
				continue
			}

			message[key] = v
		}

		event := &model.Event{
			Timestamp: timestamp,
			Metadata:  C.GoString(tag),
			Message:   message,
		}

		events = append(events, event)
		count++
	}

	err := s.Write(events)

	if err != nil {
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Print("[ydb] Exit called for unknown instance.")
	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	s, ok := output.FLBPluginGetContext(ctx).(storage.Storager)
	if !ok {
		return output.FLB_ERROR
	}
	err := s.Exit()
	if err != nil {
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	log.Print("[ydb] Unregister called")
	output.FLBPluginUnregister(def)
}

func main() {
}
