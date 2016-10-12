package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type point struct {
	timestamp time.Time
	value     float64
	tags      map[string]string
}

type points []point

type data struct {
	name   string
	points points
}

func (d *data) add(s *MockStat) error {
	if s == nil {
		return fmt.Errorf("nil metric")
	}

	if s.Name != d.name {
		return fmt.Errorf("wrong metric: %s vs %s", s.Name, d.name)
	}

	pt := point{timestamp: s.Timestamp.UTC(), value: s.Value, tags: s.Tags}

	if len(d.points) == 0 {
		d.points = append(d.points, pt)
	} else {
		n := len(d.points)
		idx := sort.Search(n, func(i int) bool {
			return d.points[i].timestamp.After(pt.timestamp)
		})

		d.points = append(d.points, pt)
		if idx < n-1 {
			copy(d.points[idx+1:], d.points[idx:])
			d.points[idx] = pt
		}
	}

	return nil
}

type mockApi struct {
	port       int
	err        error
	metrics    map[string]*data
	lastUpdate time.Time
}

func startMockWavefrontApi(port int, statsChannel <-chan *MockStat) error {
	mockApi := &mockApi{
		port:       port,
		metrics:    make(map[string]*data, 10),
		lastUpdate: time.Now(),
	}

	go func() {
		for {
			select {
			case s, ok := <-statsChannel:
				if !ok {
					return
				}

				var d *data
				if d, ok = mockApi.metrics[s.Name]; !ok {
					d = &data{s.Name, make(points, 0, 10)}
					mockApi.metrics[s.Name] = d
				}

				if err := d.add(s); err != nil {
					fmt.Printf("incoming stat error: %+v\n", err)
				}

				mockApi.lastUpdate = time.Now()
			}
		}
	}()

	go func() {
		lastCheck := time.Now()
		for {
			if len(mockApi.metrics) > 0 && mockApi.lastUpdate.After(lastCheck) {
				fmt.Println("--")
				oldest := int64(math.MaxInt64)
				newest := int64(0)
				for k, v := range mockApi.metrics {
					n := len(v.points)
					if n == 1 {
						t := v.points[0].timestamp.Unix()
						fmt.Printf("%-30s %d points (%d)\n", k+":", n, t)
						oldest = minInt(oldest, t)
						newest = maxInt(newest, t)
					} else {
						first := v.points[0].timestamp.Unix()
						last := v.points[n-1].timestamp.Unix()
						fmt.Printf(
							"%-30s %d points (%d to %d)\n",
							k+":",
							n,
							first,
							last,
						)
						oldest = minInt(oldest, first)
						newest = maxInt(newest, last)
					}
				}
				fmt.Printf("Time range %d to %d\n", oldest, newest)
			}
			lastCheck = time.Now()
			time.Sleep(5 * time.Second)
		}
	}()

	return mockApi.Listen()
}

func unparen(s string) (string, bool) {
	if s == "" {
		return s, false
	}

	n := len(s) - 1
	if s[0] == '(' && s[n-1] == ')' {
		return s[1 : n-1], true
	}
	return s, false
}

func unquote(s string) string {
	n := len(s)
	if s[0] == '"' && s[n-1] == '"' {
		return s[1 : n-1]
	}
	return s
}

func splitkv(s string) (string, string, error) {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("bad tag: '%s'", s)
	}
	k := parts[0]
	v := unquote(parts[1])
	return k, v, nil
}

type expr struct {
	token   string
	metrics []string
	tags    map[string][]string
	data    [][]*data
}

func (e *expr) eval(ts int64) (float64, error) {
	result := 0.0

	for _, data := range e.data {
		for _, d := range data {
			pts := d.points
			idx := sort.Search(len(pts), func(i int) bool {
				return pts[i].timestamp.Unix() >= ts
			})

			if idx < 0 || idx >= len(pts) {
				continue
			}

			pt := pts[idx]
			if pt.timestamp.Unix() != ts {
				continue
			}

			if len(e.tags) > 0 {
				if len(pt.tags) == 0 {
					continue
				}

				foundAllTags := true
				for name, values := range e.tags {
					foundTag := false
					for _, value := range values {
						if pt.tags[name] == value {
							foundTag = true
							break
						}
					}

					if !foundTag {
						foundAllTags = false
						break
					}
				}

				if foundAllTags {
					result += pt.value
				}
			} else {
				result += pt.value
			}
		}
	}

	return result, nil
}

type exprMap struct {
	query            string
	expr             ast.Expr
	wavefrontExprMap map[string]*expr
}

func (em *exprMap) add(e *expr) {
	if em.wavefrontExprMap == nil {
		em.wavefrontExprMap = make(map[string]*expr, 4)
	}

	em.wavefrontExprMap[e.token] = e
}

func (em *exprMap) len() int {
	return len(em.wavefrontExprMap)
}

func (em *exprMap) metrics() []string {
	s := make([]string, 0, len(em.wavefrontExprMap))
	for k := range em.wavefrontExprMap {
		s = append(s, k)
	}
	return s
}

func (em *exprMap) parse(reducedQuery string) error {
	expr, err := parser.ParseExpr(strings.ToUpper(reducedQuery))
	if err != nil {
		return err
	}
	em.query = reducedQuery
	em.expr = expr
	return nil
}

func (em *exprMap) eval(ts int64) (float64, error) {
	return em.evalExpr(ts, em.expr)
}

func (em exprMap) evalExpr(ts int64, expr ast.Expr) (float64, error) {
	switch e := expr.(type) {
	case *ast.Ident:
		expr := em.wavefrontExprMap[e.Name]
		if expr == nil {
			return math.NaN(), fmt.Errorf("unknown identifier: '%s'", e.Name)
		}
		return expr.eval(ts)

	case *ast.ParenExpr:
		return em.evalExpr(ts, e.X)

	case *ast.BinaryExpr:
		left, err := em.evalExpr(ts, e.X)
		if err != nil {
			return math.NaN(), err
		}
		right, err := em.evalExpr(ts, e.Y)
		if err != nil {
			return math.NaN(), err
		}
		switch e.Op {
		case token.ADD:
			return left + right, nil
		case token.SUB:
			return left - right, nil
		case token.MUL:
			return left * right, nil
		case token.QUO:
			if right == 0 {
				return 0, nil
			}
			return left / right, nil
		default:
			return math.NaN(), fmt.Errorf("unknown operator: %s", e.Op.String())
		}

	case *ast.CallExpr:
		nameIdent, ok := e.Fun.(*ast.Ident)
		if !ok {
			return math.NaN(), fmt.Errorf("unexpected function node '%+v'", e.Fun)
		}
		if nameIdent.Name == "DEFAULT" {
			if len(e.Args) != 2 {
				return math.NaN(), fmt.Errorf(
					"wrong number of arguments to default: '%+v'",
					e.Args,
				)
			}

			return em.evalExpr(ts, e.Args[1])
		} else if nameIdent.Name == "RAWSUM" {
			if len(e.Args) != 1 {
				return math.NaN(), fmt.Errorf(
					"wrong number of arguments to rawsum: '%+v'",
					e.Args,
				)
			}

			return em.evalExpr(ts, e.Args[0])
		} else {
			return math.NaN(), fmt.Errorf("unknown function '%s'", nameIdent.Name)
		}
	}

	if expr.Pos().IsValid() && expr.End().IsValid() {
		return math.NaN(), fmt.Errorf(
			"unknown expression type '%s' in '%s'",
			em.query[expr.Pos():expr.End()],
			em.query,
		)
	} else {
		return math.NaN(), fmt.Errorf("unknown expression in '%s'", em.query)
	}
}

func parseTagExpr(tagExpr string) (map[string][]string, error) {
	tags := map[string][]string{}

	ands := strings.Split(strings.TrimSpace(tagExpr), " and ")
	for _, and := range ands {
		and = strings.TrimSpace(and)
		if stripped, ok := unparen(and); ok {
			ors := strings.Split(stripped, " or ")
			for _, or := range ors {
				k, v, err := splitkv(or)
				if err != nil {
					return nil, err
				}

				if prev, ok := tags[k]; ok {
					tags[k] = append(prev, v)
				} else {
					tags[k] = []string{v}
				}
			}
		} else if and != "" {
			k, v, err := splitkv(and)
			if err != nil {
				return nil, err
			}

			tags[k] = []string{v}
		}
	}

	return tags, nil
}

var queryRegex = regexp.MustCompile(
	`ts\( *("[A-Za-z0-9._*-]+"(?: +or +"[A-Za-z0-9._*-]+")*) *(?:, *([^)]+))?\)`,
)

func (a *mockApi) parseQuery(q string) (*exprMap, error, bool) {
	exprMap := &exprMap{}

	tokenN := 1
	reducedQuery := ""
	remainder := q
	numMetricsFound := 0
	for remainder != "" {
		match := queryRegex.FindStringSubmatchIndex(remainder)
		if len(match) == 0 {
			reducedQuery += remainder
			remainder = ""
			break
		}

		subquery := remainder[match[0]:match[1]]
		metric := remainder[match[2]:match[3]]

		var tags map[string][]string
		if match[4] >= 0 {
			var err error
			tagExpr := remainder[match[4]:match[5]]
			if tags, err = parseTagExpr(tagExpr); err != nil {
				return nil, fmt.Errorf(
					"subquery '%s' of query '%s': %s",
					subquery,
					q,
					err.Error(),
				), false
			}
		}

		token := fmt.Sprintf("_%d_", tokenN)
		tokenN++

		reducedQuery += remainder[0:match[0]] + token
		remainder = remainder[match[1]:]

		metrics, err := a.splitMetrics(metric)
		if err != nil {
			return nil, err, false
		}

		dataSets := make([][]*data, len(metrics))
		for i, metric := range metrics {
			d := a.findMetrics(metric)
			if len(d) > 0 {
				numMetricsFound++
			}
			dataSets[i] = d
		}
		exprMap.add(&expr{token: token, metrics: metrics, tags: tags, data: dataSets})
	}

	if exprMap.len() == 0 {
		return nil, fmt.Errorf("query '%s': cannot find metric and tags", q), false
	}

	if numMetricsFound == 0 {
		return nil, fmt.Errorf("No metrics matching [%s]", exprMap.metrics()), true
	}

	// We make use of the fact that the tokens in the reduced
	// query look like go identifiers and use the go parser to
	// parse the query.
	if err := exprMap.parse(reducedQuery); err != nil {
		return nil, fmt.Errorf(
			"query '%s', reduced to '%s': %s",
			q,
			reducedQuery,
			err.Error(),
		), false
	}

	return exprMap, nil, false
}

var metricRegex = regexp.MustCompile(`^"([A-Za-z0-9._*-]+)"((?: +or +"[A-Za-z0-9._*-]+")*)$`)

func (a *mockApi) splitMetrics(metric string) ([]string, error) {
	metrics := []string{}
	original := metric
	for metric != "" {
		match := metricRegex.FindStringSubmatchIndex(metric)
		if len(match) == 0 {
			return nil, fmt.Errorf("could not split metric '%s'", original)
		}

		metrics = append(metrics, metric[match[2]:match[3]])
		if match[4] >= 0 {
			metric = strings.TrimLeft(metric[match[4]:], " or")
		} else {
			metric = ""
		}
	}

	return metrics, nil
}

type apiString string

func (s apiString) ReplaceAll(old, new string) apiString {
	return apiString(strings.Replace(string(s), old, new, -1))
}

func (s apiString) String() string {
	return string(s)
}

func (a *mockApi) findMetrics(metric string) []*data {
	if d, ok := a.metrics[metric]; ok {
		return []*data{d}
	}

	metricRegex, err := regexp.Compile(
		"^" + apiString(metric).ReplaceAll(`.`, `\.`).ReplaceAll(`*`, `.+`).String() + "$",
	)

	if err != nil {
		fmt.Println("regexp error", err)
		return nil
	}

	result := []*data{}
	for k, d := range a.metrics {
		if metricRegex.MatchString(k) {
			result = append(result, d)
		}
	}

	return result
}

func write(w http.ResponseWriter, i interface{}) {
	b, err := json.Marshal(i)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	w.Write(b)
}

func parseInt(s string, def int64) (int64, error) {
	if s == "" {
		return def, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func (a *mockApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()

	query := params.Get("q")

	fmt.Println("Query:", query)

	exprMap, err, isApiErr := a.parseQuery(query)
	if err != nil {
		if isApiErr {
			result := map[string]string{
				"query":    query,
				"name":     query,
				"warnings": err.Error(),
			}
			write(w, result)
		} else {
			w.WriteHeader(400)
			w.Write([]byte(err.Error() + "\n"))
		}
		return
	}

	granularity := params.Get("g")
	start := params.Get("s")
	end := params.Get("e")

	delta := int64(1)
	switch granularity {
	case "m", "M":
		delta = 60
	case "h", "H":
		delta = 3600
	case "d", "D":
		delta = 86400
	}

	startTs, err := parseInt(start, 0)
	if err != nil {
		err = fmt.Errorf("bad s=%s: %s", start, err.Error())
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}
	endTs, err := parseInt(end, startTs+3600)
	if err != nil {
		err = fmt.Errorf("bad e=%s: %s", end, err.Error())
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	points := make([][]float64, 0, (endTs-startTs)/delta)
	for ts := startTs; ts < endTs; ts += delta {
		v, err := exprMap.eval(ts)
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte("data error: " + err.Error() + "\n"))
			return

		}
		points = append(points, []float64{float64(ts), v})
	}

	result := map[string]interface{}{
		"query": query,
		"name":  query,
		"timeseries": []map[string]interface{}{
			{
				"label": query,
				"data":  points,
			},
		},
		"granularity": delta,
	}
	write(w, result)
}

func (a *mockApi) Listen() error {
	http.Handle("/chart/api", a)

	go func() {
		a.err = http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", a.port), nil)
	}()

	time.Sleep(1 * time.Second)
	return a.err
}

func minInt(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
