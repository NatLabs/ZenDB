import V0 "V0";

module Migrations {

    public type MemoryIdBTree = V0.MemoryIdBTree;
    public type Leaf = V0.Leaf;
    public type Node = V0.Node;
    public type Branch = V0.Branch;

    public type VersionedMemoryIdBTree = {
        #v0 : V0.MemoryIdBTree;
    };

    public func upgrade(versions: VersionedMemoryIdBTree) : VersionedMemoryIdBTree {
        switch(versions) {
            case (#v0(v0)) versions;
        }
    };

    public func getCurrentVersion(versions: VersionedMemoryIdBTree) : MemoryIdBTree {
        switch(versions) {
            case (#v0(v0)) v0;
            // case (_) Debug.trap("Unsupported version. Please upgrade the memory buffer to the latest version.");
        }
    };
}