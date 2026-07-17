// Minimal DuckDB-Airport-compatible Arrow Flight server (REQ-1097).
//
// zaychik (this repo's other Flight fixture) speaks Flight SQL, a different application
// protocol from DuckDB's `airport` community extension (custom DoAction/catalog RPCs) — so
// `ATTACH ... (TYPE AIRPORT)` cannot use zaychik. This shim implements the airport protocol
// itself, via github.com/hugr-lab/airport-go, and serves one static table so the integration
// test can drive DuckDBAirportConnector.details() against a REAL airport-speaking server.
//
// Catalog: schema "test", table "widgets" (id INTEGER, name VARCHAR), 3 static rows.
package main

import (
	"context"
	"log"
	"net"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	airport "github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

func main() {
	widgetsSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	scanWidgets := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, widgetsSchema)
		defer builder.Release()

		builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues(
			[]string{"Widget A", "Widget B", "Widget C"}, nil,
		)

		record := builder.NewRecord()
		defer record.Release()

		return array.NewRecordReader(widgetsSchema, []arrow.Record{record})
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("test").
		SimpleTable(airport.SimpleTableDef{
			Name:     "widgets",
			Comment:  "static test fixture (REQ-1097)",
			Schema:   widgetsSchema,
			ScanFunc: scanWidgets,
		}).
		Build()
	if err != nil {
		log.Fatalf("failed to build catalog: %v", err)
	}

	grpcServer := grpc.NewServer()
	if err := airport.NewServer(grpcServer, airport.ServerConfig{Catalog: cat}); err != nil {
		log.Fatalf("failed to register airport server: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Println("airport shim listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}
