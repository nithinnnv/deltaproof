// Bulk helpers around minisketch for the PinSketch baseline (deltaproof/pinsketch.py).
#include <cstddef>
#include <cstdint>
#include "../include/minisketch.h"

extern "C" {

// Add n elements to one sketch.
void ms_add_many(minisketch* sketch, const uint64_t* elements, size_t n) {
    for (size_t i = 0; i < n; ++i) minisketch_add_uint64(sketch, elements[i]);
}

// Add n elements across an array of sketches: element i goes to sketches[part[i]].
void ms_add_partitioned(minisketch** sketches, const uint64_t* elements, const int64_t* part, size_t n) {
    for (size_t i = 0; i < n; ++i) minisketch_add_uint64(sketches[part[i]], elements[i]);
}

}
