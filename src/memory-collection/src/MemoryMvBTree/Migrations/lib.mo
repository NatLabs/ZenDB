import V0 "V0";

module Migrations {

    public type MemoryMvBTree = V0.MemoryMvBTree;
    public type Leaf = V0.Leaf;
    public type Node = V0.Node;
    public type Branch = V0.Branch;

    public type VersionedMemoryMvBTree = {
        #v0 : V0.MemoryMvBTree;
    };

    public func upgrade(versions: VersionedMemoryMvBTree) : VersionedMemoryMvBTree {
        switch(versions) {
            case (#v0(v0)) versions;
        }
    };

    public func getCurrentVersion(versions: VersionedMemoryMvBTree) : MemoryMvBTree {
        switch(versions) {
            case (#v0(v0)) v0;
            // case (_) Debug.trap("Unsupported version. Please upgrade the memory buffer to the latest version.");
        }
    };
}