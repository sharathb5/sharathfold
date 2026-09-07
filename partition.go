package fold

import (
	"fmt"

	"github.com/sharathb5/sharathfold/internal/hash"
)

// ownership maps partitions to worker owner IDs and the reverse.
type ownership struct {
	partitions int
	byOwner    map[string][]int
	byPart     []string // partition -> owner (exclusive); empty string if overlap
	overlap    bool
}

func buildOwnership(workers, partitions int, overlap bool) (ownership, error) {
	if workers <= 0 {
		return ownership{}, fmt.Errorf("fold: Workers must be >= 1")
	}
	if partitions <= 0 {
		partitions = hash.DefaultPartitions
	}
	o := ownership{
		partitions: partitions,
		byOwner:    make(map[string][]int, workers),
		byPart:     make([]string, partitions),
		overlap:    overlap,
	}
	for w := 0; w < workers; w++ {
		id := ownerID(w)
		parts := make([]int, 0, (partitions/workers)+1)
		if overlap {
			for p := 0; p < partitions; p++ {
				parts = append(parts, p)
			}
		} else {
			for p := 0; p < partitions; p++ {
				if p%workers == w {
					parts = append(parts, p)
					o.byPart[p] = id
				}
			}
		}
		o.byOwner[id] = parts
	}
	return o, nil
}

func ownerID(i int) string {
	return fmt.Sprintf("worker-%d", i)
}

func (o ownership) partitionsFor(owner string) []int {
	return o.byOwner[owner]
}

func (o ownership) ownerOf(partition int) (string, bool) {
	if o.overlap || partition < 0 || partition >= len(o.byPart) {
		return "", false
	}
	id := o.byPart[partition]
	return id, id != ""
}

func (o ownership) owners() []string {
	out := make([]string, 0, len(o.byOwner))
	for id := range o.byOwner {
		out = append(out, id)
	}
	return out
}
