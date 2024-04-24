import Int "mo:base/Int";

import MemoryRegion "mo:memory-region/MemoryRegion";

module {
    type MemoryRegion = MemoryRegion.MemoryRegion;

    public func shift(region : MemoryRegion, start : Nat, end : Nat, offset : Int) {
        let size = (end - start : Nat);

        let blob = MemoryRegion.loadBlob(region, start, size);

        let new_start = Int.abs(start + offset);

        MemoryRegion.storeBlob(region, new_start, blob);
    };

}