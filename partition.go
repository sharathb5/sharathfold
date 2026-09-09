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
	// narrowed is true when this process claims a subset of the hash space
	// (PartitionsClaim). Resize is unsupported while narrowed.
	narrowed bool
}

func buildOwnership(workers, partitions int, overlap bool, claim []int) (ownership, error) {
	if workers <= 0 {
		return ownership{}, fmt.Errorf("fold: Workers must be >= 1")
	}
	if partitions <= 0 {
		partitions = hash.DefaultPartitions
	}

	narrowed := claim != nil
	universe := claim
	if universe == nil {
		universe = make([]int, partitions)
		for p := 0; p < partitions; p++ {
			universe[p] = p
		}
	} else {
		seen := make(map[int]struct{}, len(universe))
		for _, p := range universe {
			if p < 0 || p >= partitions {
				return ownership{}, fmt.Errorf("fold: PartitionsClaim value %d out of [0, %d)", p, partitions)
			}
			if _, ok := seen[p]; ok {
				return ownership{}, fmt.Errorf("fold: PartitionsClaim duplicate partition %d", p)
			}
			seen[p] = struct{}{}
		}
	}

	o := ownership{
		partitions: partitions,
		byOwner:    make(map[string][]int, workers),
		byPart:     make([]string, partitions),
		overlap:    overlap,
		narrowed:   narrowed,
	}
	for w := 0; w < workers; w++ {
		id := ownerID(w)
		parts := make([]int, 0, (len(universe)/workers)+1)
		if overlap {
			parts = append(parts, universe...)
		} else {
			for i, p := range universe {
				if i%workers == w {
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

func parseOwnerIndex(owner string) (int, bool) {
	var i int
	if _, err := fmt.Sscanf(owner, "worker-%d", &i); err != nil {
		return 0, false
	}
	return i, true
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
