import V0 "V0";

module Migrations {

    public type MemoryBTree = V0.MemoryBTree;
    public type Leaf = V0.Leaf;
    public type Node = V0.Node;
    public type Branch = V0.Branch;

    public type VersionedMemoryBTree = {
        #v0 : V0.MemoryBTree;
    };

    public func upgrade(versions: VersionedMemoryBTree) : VersionedMemoryBTree {
        switch(versions) {
            case (#v0(v0)) versions;
        }
    };

    public func getCurrentVersion(versions: VersionedMemoryBTree) : MemoryBTree {
        switch(versions) {
            case (#v0(v0)) v0;
            // case (_) Debug.trap("Unsupported version. Please upgrade the memory buffer to the latest version.");
        }
    };
}