package server

import (
	"fmt"
	"testing"

	"jingoal.com/dfs/metadata"
)

func updateSegment(segments []*metadata.Segment, segment *metadata.Segment) []*metadata.Segment {
	pos, result := findPerfectSegment(segments, segment.Domain)

	if result.Domain != segment.Domain { // Not found
		// Insert
		pos++
		rear := append([]*metadata.Segment{}, segments[pos:]...)
		segments = append(segments[:pos], segment)
		segments = append(segments, rear...)

		return segments
	} else { // Found
		// Equal, remove it according to flag.
		if *segmentDeletion &&
			result.NormalServer == segment.NormalServer &&
			result.MigrateServer == segment.MigrateServer {
			segs := append(segments[:pos], segments[pos+1:]...)
			segments = segs
			return segments
		}
		// Not equal, update it.
		segments[pos] = segment
		return segments
	}
}

func printSegments(segments []*metadata.Segment) {
	fmt.Printf("===START===\n")
	for i, segment := range segments {
		fmt.Printf("sequence: %d,\t\tDomain: %v\n", i, segment)
	}
	fmt.Printf("===END===\n\n")
}

func TestUpdateSegment(t *testing.T) {
	segments := []*metadata.Segment{
		{
			Domain: 1,
		},
	}
	printSegments(segments)

	segments = updateSegment(segments,
		&metadata.Segment{
			Domain: 2,
		},
	)
	printSegments(segments)

	segments = updateSegment(segments,
		&metadata.Segment{
			Domain: 7,
		},
	)
	printSegments(segments)

	segments = updateSegment(segments,
		&metadata.Segment{
			Domain: 5,
		},
	)
	printSegments(segments)

	segments = updateSegment(segments,
		&metadata.Segment{
			Domain: 100,
		},
	)
	printSegments(segments)

	segments = updateSegment(segments,
		&metadata.Segment{
			Domain: 22,
		},
	)
	printSegments(segments)

	segments = updateSegment(segments,
		&metadata.Segment{
			Domain: 9,
		},
	)
	printSegments(segments)

	segments = updateSegment(segments,
		&metadata.Segment{
			Domain:       5,
			NormalServer: "A1",
		},
	)
	printSegments(segments)

	segments = updateSegment(segments,
		&metadata.Segment{
			Domain:        5,
			NormalServer:  "A1",
			MigrateServer: "B1",
		},
	)
	printSegments(segments)

	segments = updateSegment(segments,
		&metadata.Segment{
			Domain: 13,
		},
	)
	printSegments(segments)

	segments = updateSegment(segments,
		&metadata.Segment{
			Domain:        5,
			NormalServer:  "A1",
			MigrateServer: "B1",
		},
	)
	printSegments(segments)

	for domain := 1; domain <= 100; domain++ {
		_, p := findPerfectSegment(segments, int64(domain))
		fmt.Printf("domain: %d, segment: %d\n", domain, p.Domain)
	}
}
