module {
    public func new() : [var Nat] { [var 0] };

    public func next(ids : [var Nat]) : Nat {
        let id = ids[0];
        ids[0] += 1;
        id;
    };
};
