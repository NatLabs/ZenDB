import EmbeddedInstance "EmbeddedInstance";
import RemoteInstance "RemoteInstance";

module {

    public let {
        Types;
        Constants;
        Schema;
        Tuple;
        fromTuple;
        Triple;
        fromTriple;
        Quadruple;
        fromQuadruple;
        Quintuple;
        fromQuintuple;
        DefaultMemoryType;
        defaultSettings;
        newStableStore;
        upgrade;
        launchDefaultDB;
        createDB;
        getDB;
        setIsRunLocally;
        setLogLevel;
        QueryBuilder;
        stats;
    } = EmbeddedInstance;

    public type Collection<T> = EmbeddedInstance.Collection<T>;
    public type Database = EmbeddedInstance.Database;
    public type Schema = EmbeddedInstance.Schema;
    public type Candify<T> = EmbeddedInstance.Candify<T>;
    public type Tuple<A, B> = EmbeddedInstance.Tuple<A, B>;
    public type Triple<A, B, C> = EmbeddedInstance.Triple<A, B, C>;
    public type Quadruple<A, B, C, D> = EmbeddedInstance.Quadruple<A, B, C, D>;
    public type Quintuple<A, B, C, D, E> = EmbeddedInstance.Quintuple<A, B, C, D, E>;
    public type Settings = EmbeddedInstance.Settings;
    public type QueryBuilder = EmbeddedInstance.QueryBuilder;

    public let {
        Roles;
        Client;
        CanisterDB;
    } = RemoteInstance;

    public type Client = RemoteInstance.Client;
    public type CanisterDB = RemoteInstance.CanisterDB;

};
