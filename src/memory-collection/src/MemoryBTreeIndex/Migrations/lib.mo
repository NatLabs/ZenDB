import V0 "V0";

module Migrations {

    public type MemoryBTreeIndex = V0.MemoryBTreeIndex;
    public type Leaf = V0.Leaf;
    public type Node = V0.Node;
    public type Branch = V0.Branch;

    public type VersionedMemoryBTreeIndex = {
        #v0 : V0.MemoryBTreeIndex;
    };

    public func upgrade(versions: VersionedMemoryBTreeIndex) : VersionedMemoryBTreeIndex {
        switch(versions) {
            case (#v0(v0)) versions;
        }
    };

    public func getCurrentVersion(versions: VersionedMemoryBTreeIndex) : MemoryBTreeIndex {
        switch(versions) {
            case (#v0(v0)) v0;
            // case (_) Debug.trap("Unsupported version. Please upgrade the memory buffer to the latest version.");
        }
    };
}