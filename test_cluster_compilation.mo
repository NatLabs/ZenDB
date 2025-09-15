// Test compilation of updated cluster functions
import CanisterDB "../src/Cluster/CanisterDB";
import ClusterTypes "../src/Cluster/Types";
import ZT "../src/Types";

actor TestCluster {
    public func test_compilation() : async Bool {
        // This is just a compilation test
        let doc_id : ZT.DocumentId = "\00\01\02";
        true;
    };
};
