package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/apache/pulsar/pulsar-function-go/pf"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// --- CONFIGURATION ---
const (
	BASYX_API_BASE_URL = "http://aas-env:8081"
	INFLUX_URL         = "http://influxdb:8086"
	INFLUX_TOKEN       = "my-super-secret-auth-token"
	INFLUX_ORG         = "pulsar"
	INFLUX_BUCKET      = "pulsar-bucket"
)

var httpClient = &http.Client{
	Timeout: 20 * time.Second,
}

var influxWriteAPI api.WriteAPI

type InputMessage struct {
	SubmodelID       string          `json:"submodelId"`
	ElementsToUpdate []ElementUpdate `json:"elementsToUpdate"`
}

type ElementUpdate struct {
	IDShortPath string      `json:"idShortPath"`
	Value       interface{} `json:"value"`
}

type InfluxLogEntry struct {
	Timestamp  time.Time
	SubmodelID string
	Property   string
	Value      interface{}
	Status     string
}

func BasyxPatchFunction(ctx context.Context, input []byte) error {
	// 1. Parse Input
	var msg InputMessage
	if err := json.Unmarshal(input, &msg); err != nil {
		fmt.Printf("[ERROR] Failed to unmarshal JSON: %v\n", err)
		return nil
	}

	// 2. Validate Input
	if msg.SubmodelID == "" || len(msg.ElementsToUpdate) == 0 {
		fmt.Printf("[WARN] Invalid input: SubmodelID missing\n")
		return nil
	}

	fmt.Printf("[INFO] Processing SubmodelID: %s (%d elements)\n", msg.SubmodelID, len(msg.ElementsToUpdate))

	hasError := false

	// 3. Process updates
	for _, element := range msg.ElementsToUpdate {
		if element.IDShortPath == "" {
			continue
		}

		err := updateBasyxElement(msg.SubmodelID, element)
		status := "SUCCESS"
		if err != nil {
			fmt.Printf("[ERROR] Update failed '%s': %v\n", element.IDShortPath, err)
			hasError = true
			status = "ERROR"
		} else {
			fmt.Printf("[INFO] Updated '%s'\n", element.IDShortPath)
		}

		// 4. Write to InfluxDB
		logEntry := InfluxLogEntry{
			Timestamp:  time.Now(),
			SubmodelID: msg.SubmodelID,
			Property:   element.IDShortPath,
			Value:      element.Value,
			Status:     status,
		}
		writeToInfluxDB(logEntry)
	}

	if hasError {
		return fmt.Errorf("updates failed")
	}
	return nil
}

func updateBasyxElement(submodelID string, element ElementUpdate) error {
	payload, err := json.Marshal(element.Value)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	url := fmt.Sprintf("%s/submodels/%s/submodel-elements/%s/$value", BASYX_API_BASE_URL, submodelID, element.IDShortPath)
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("req create failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("api call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func writeToInfluxDB(entry InfluxLogEntry) {
	valStr := fmt.Sprintf("%v", entry.Value)

	p := influxdb2.NewPointWithMeasurement("aas_property_history").
		AddTag("submodelId", entry.SubmodelID).
		AddTag("idShortPath", entry.Property).
		AddTag("status", entry.Status).
		AddField("value", valStr).
		SetTime(entry.Timestamp)

	influxWriteAPI.WritePoint(p)
	influxWriteAPI.Flush()
}

func main() {
	fmt.Println("[INIT] Connecting to InfluxDB...")
	client := influxdb2.NewClient(INFLUX_URL, INFLUX_TOKEN)
	influxWriteAPI = client.WriteAPI(INFLUX_ORG, INFLUX_BUCKET)

	defer client.Close()

	fmt.Println("[INFO] Starting BaSyx Updater Function with InfluxDB logging...")
	pf.Start(BasyxPatchFunction)
}
