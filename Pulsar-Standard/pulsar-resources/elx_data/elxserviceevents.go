package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/apache/pulsar/pulsar-function-go/pf"
)

const (
	BASYX_API_BASE_URL = "http://aas-env:8081"
	SUBMODEL_ID        = "https://electrolux.com/ids/washer/sm/service-data/1"
	SERIAL_FILTER      = "33600132"
	SEM_EVENT          = "https://electrolux.com/ids/washer/sm/ServiceData/1/0/ServiceHistory/ServiceEvent"
)

var httpClient = &http.Client{Timeout: 20 * time.Second}

var historyPath = fmt.Sprintf("/submodels/%s/submodel-elements/ServiceHistory", b64(SUBMODEL_ID))

type RowMessage struct {
	Headers []string `json:"headers"`
	Row     []string `json:"row"`
}

// looks a cell up by column name, cleaned.
func getter(msg RowMessage) func(string) string {
	norm := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	idx := make(map[string]int, len(msg.Headers))
	for i, h := range msg.Headers {
		idx[norm(h)] = i
	}
	return func(name string) string {
		i, ok := idx[norm(name)]
		if !ok {
			fmt.Printf("[WARN] No column %q in message\n", name)
		} else if i < len(msg.Row) {
			return strings.TrimSpace(html.UnescapeString(msg.Row[i]))
		}
		return ""
	}
}

func ElxServiceEventFunction(ctx context.Context, input []byte) error {
	var msg RowMessage
	if err := json.Unmarshal(input, &msg); err != nil {
		fmt.Printf("[ERROR] Bad JSON: %v\n", err)
		return nil
	}
	get := getter(msg)

	if get("Serial Number") != SERIAL_FILTER {
		return nil
	}
	orderNr := get("Order Nr")
	if orderNr == "" {
		fmt.Printf("[WARN] Row without Order Nr, dropped\n")
		return nil
	}

	status, history, err := basyx(http.MethodGet, historyPath+"/$value", nil)
	switch {
	case err != nil:
		return fmt.Errorf("BaSyx unreachable: %w", err)
	case status == http.StatusNotFound:
		fmt.Printf("[WARN] No AAS for serial %s, skipping\n", SERIAL_FILTER)
		return nil // provisioning is not this function job
	case status < 200 || status >= 300:
		return fmt.Errorf("history read failed: %d %s", status, history)
	case bytes.Contains(history, []byte(orderNr)):
		fmt.Printf("[INFO] Order %s already present, skipping\n", orderNr)
		return nil
	}

	event, _ := json.Marshal(buildEvent(get))
	status, body, err := basyx(http.MethodPost, historyPath, event)
	if err != nil {
		return fmt.Errorf("append failed: %w", err)
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("append rejected: %d %s", status, body)
	}

	fmt.Printf("[INFO] Appended order %s to %s\n", orderNr, SUBMODEL_ID)
	return nil
}

// buildEvent maps the Excel columns onto a ServiceEvent collection.
// The collection carries no idShort: it is a child of a SubmodelElementList.
// A column with no data yields a nil element, dropped rather than written as an empty value.
func buildEvent(get func(string) string) map[string]any {
	elements := []map[string]any{
		prop("OrderNumber", "xs:string", get("Order Nr")),
		prop("OrderDate", "xs:date", isoDate(get("Order Date"))),
		prop("ServiceDate", "xs:date", isoDate(get("Service Date"))),
		prop("Market", "xs:string", get("Market")),
		prop("ManufacturingPlant", "xs:string", get("Plant")),
		prop("FaultComponent", "xs:string", get("Component")),
		prop("FaultCode", "xs:string", get("Defect")),
		mlp("CustomerComplaint", get("Customer Complaint"), get("Customer Complaint Transl.")),
		mlp("TechnicianComment", get("Tech Comment"), get("Tech Comment Transl.")),
		prop("TimeToFailure", "xs:double", get("TTF Prod")),
		prop("TotalCost", "xs:double", get("Total Cost [EUR]")),
		prop("MaterialCost", "xs:double", get("Mtrl Cost [EUR]")),
	}

	value := make([]map[string]any, 0, len(elements))
	for _, e := range elements {
		if e != nil {
			value = append(value, e)
		}
	}
	return map[string]any{
		"modelType":  "SubmodelElementCollection",
		"semanticId": semRef(SEM_EVENT),
		"value":      value,
	}
}

func prop(idShort, valueType, value string) map[string]any {
	if value == "" {
		return nil
	}
	return map[string]any{
		"modelType":  "Property",
		"idShort":    idShort,
		"valueType":  valueType,
		"value":      value,
		"semanticId": semRef(SEM_EVENT + "/" + idShort),
	}
}

func mlp(idShort, fr, en string) map[string]any {
	var langs []map[string]string
	if fr != "" {
		langs = append(langs, map[string]string{"language": "fr", "text": fr})
	}
	if en != "" {
		langs = append(langs, map[string]string{"language": "en", "text": en})
	}
	if len(langs) == 0 {
		return nil // AASd-100: an empty MultiLanguageProperty must omit "value"
	}
	return map[string]any{
		"modelType":  "MultiLanguageProperty",
		"idShort":    idShort,
		"value":      langs,
		"semanticId": semRef(SEM_EVENT + "/" + idShort),
	}
}

func semRef(iri string) map[string]any {
	return map[string]any{
		"type": "ExternalReference",
		"keys": []map[string]string{{"type": "GlobalReference", "value": iri}},
	}
}

// isoDate turns "20240513" into xs:date "2024-05-13", "" if it is not a real date.
func isoDate(s string) string {
	t, err := time.Parse("20060102", s)
	if err != nil {
		return ""
	}
	return t.Format("2006-01-02")
}

// b64 encodes an AAS identifier for use in a BaSyx URL path.
func b64(id string) string {
	return base64.URLEncoding.EncodeToString([]byte(id))
}

func basyx(method, path string, body []byte) (int, []byte, error) {
	req, err := http.NewRequest(method, BASYX_API_BASE_URL+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	return resp.StatusCode, out, err
}

func main() {
	fmt.Println("[INFO] Starting Electrolux ServiceHistory Function...")
	pf.Start(ElxServiceEventFunction)
}
