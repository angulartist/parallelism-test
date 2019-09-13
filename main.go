package main

import (
	"bramp.net/morebeam"
	"bramp.net/morebeam/csvio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"reflect"
)

var (
	input  = flag.String("file", "./csv/sample.csv", "input file")
	output = flag.String("output", "./outputs/reporting.txt", "output results")
)

type Rating struct {
	UserId    int     `csv:"userId" json:"userId"`
	MovieId   int     `csv:"movieId" json:"movieId"`
	Rating    float64 `csv:"rating" json:"rating"`
	Timestamp int     `csv:"timestamp" json:"timestamp"`
}

type UserRatings struct {
	UserId  int `json:"userId"`
	Ratings int `json:"ratings"`
}

func dumpAndClearMetrics() {
	metrics.DumpToOut()
	metrics.Clear()
}

func init() {
	beam.RegisterType(reflect.TypeOf(Rating{}))
	beam.RegisterType(reflect.TypeOf(UserRatings{}))
}

func main() {
	flag.Parse()

	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	ctx := context.Background()

	log.Infof(ctx, "Started pipeline on scope: %s", s)

	/* [TEST PIPELINE START ]*/

	countMetric := beam.NewCounter("example.parallelism", "Count K/V Pairs")

	// Read .csv file and convert each row to a Rating struct.
	sr := csvio.Read(s, *input, reflect.TypeOf(Rating{}))

	// Reshuffle the CSV output to improve parallelism.
	// This helps Dataflow to distribute ops across multiple workers.
	rs := morebeam.Reshuffle(s.Scope("Reshuffle"), sr)

	// PCollection<Rating> -> KV<int, int>
	pwo := beam.ParDo(s.Scope("Pair Key With One"),
		func(x Rating, emit func(int, int)) {
			countMetric.Inc(ctx, 1)
			emit(x.UserId, 1)
		}, rs)

	// KV<int, int> -> KV<int, int>
	spk := stats.SumPerKey(s, pwo)

	// KV<int, int> -> PCollection<UserRatings>
	mp := beam.ParDo(s.Scope("Map KV To Struct"),
		func(k int, v int, emit func(UserRatings)) {
			emit(UserRatings{
				UserId:  k,
				Ratings: v,
			})
		}, spk)

	// PCollection<UserRatings> -> PCollection<[]UserRatings>
	t := top.Largest(s, mp, 1000, func(x, y UserRatings) bool { return x.Ratings < y.Ratings })

	// PCollection<[]UserRatings> -> PCollection<string>
	o := beam.ParDo(s.Scope("Format Output"),
		func(x []UserRatings) string {
			if data, err := json.MarshalIndent(x, "", ""); err != nil {
				return fmt.Sprintf("[Err]: %v", err)
			} else {
				return fmt.Sprintf("Output: %s", data)
			}
		}, t)

	// PCollection<string> -> none
	textio.Write(s, *output, o)

	dumpAndClearMetrics()

	/* [TEST PIPELINE END ]*/

	if err := beamx.Run(ctx, p); err != nil {
		fmt.Println(err)
		log.Exitf(ctx, "Failed to execute job: on ctx=%v:")
	}
}
