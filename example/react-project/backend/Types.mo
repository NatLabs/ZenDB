module {

    public type Tx = {
        amt : ?Nat;
        from : ?Blob;
        to : ?Blob;
        spender : ?Blob;
        memo : ?Blob;
        expires_at : ?Nat;
        expected_allowance : ?Nat;
    };

    public type Block = {
        btype : Text;
        phash : ?Blob;
        ts : Nat;
        fee : ?Nat;
        tx : Tx;
        tx_index : Nat;
    };
};
