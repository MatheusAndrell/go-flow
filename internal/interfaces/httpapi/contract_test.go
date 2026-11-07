package httpapi

import (
	"bufio"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// TestRoutesMatchOpenAPI keeps the router and api/openapi.yaml in sync.
func TestRoutesMatchOpenAPI(t *testing.T) {
	f, err := os.Open("../../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	pathLine := regexp.MustCompile(`^  (/\S*):\s*$`)
	methodLine := regexp.MustCompile(`^    (get|post|put|patch|delete):\s*$`)
	var documented []string
	current := ""
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if m := pathLine.FindStringSubmatch(sc.Text()); m != nil {
			current = m[1]
		} else if m := methodLine.FindStringSubmatch(sc.Text()); m != nil && current != "" {
			documented = append(documented, strings.ToUpper(m[1])+" "+current)
		}
		if strings.HasPrefix(sc.Text(), "components:") {
			break
		}
	}
	implemented := []string{"GET /health", "GET /ready", "GET /metrics"}
	for _, r := range (&Server{}).apiRoutes() {
		implemented = append(implemented, r.pattern)
	}
	slices.Sort(documented)
	slices.Sort(implemented)
	if !slices.Equal(documented, implemented) {
		t.Fatalf("router and OpenAPI differ\ndocumented:  %v\nimplemented: %v", documented, implemented)
	}
}
