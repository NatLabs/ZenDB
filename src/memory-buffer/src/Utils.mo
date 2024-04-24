import Blob "mo:base/Blob";
import Buffer "mo:base/Buffer";
import Array "mo:base/Array";
import Nat8 "mo:base/Nat8";
import Prelude "mo:base/Prelude";
import Iter "mo:base/Iter";
import Debug "mo:base/Debug";
import Result "mo:base/Result";

import Fuzz "mo:fuzz";

module {

    type Buffer<A> = Buffer.Buffer<A>;
    type Iter<A> = Iter.Iter<A>;
    type Result<A, B> = Result.Result<A, B>;
    type Fuzzer = Fuzz.Fuzzer;    

    // public func blob_pointer_at_index<A>(self : MemoryBuffer<A>, index : Nat) : Blob {
    //     let address = index * 12;
    //     MemoryRegion.loadBlob(self.pointers, address, 12);
    // };

    // public func address_at_index<A>(self : MemoryBuffer<A>, index : Nat) : Nat {
    //     let pointer_address = Nat64.fromNat(index * 12);
    //     let address = Region.loadNat64(self.pointers.region, pointer_address);
    //     Nat64.toNat(address);
    // };

    // public func size_at_index<A>(self : MemoryBuffer<A>, index : Nat) : Nat {
    //     let pointer_address = Nat64.fromNat(index * 12);
    //     let value_size = Region.loadNat32(self.pointers.region, pointer_address + 8);

    //     Nat32.toNat(value_size);
    // };

    // public func update_pointer_at_index<A>(self : MemoryBuffer<A>, index : Nat, address : Nat, size : Nat) {
    //     if (index == 398) Debug.print("prev (i, address, size): " # debug_show (index, address, size));

    //     let pointer_address = Nat64.fromNat(index * 12);

    //     let value_address = Nat64.fromNat(address);
    //     let value_size = Nat32.fromNat(size);

    //     Region.storeNat64(self.pointers.region, pointer_address, value_address);
    //     Region.storeNat32(self.pointers.region, pointer_address + 8, value_size);

    //     if (index == 398) Debug.print("new (i, address, size): " # debug_show (index, value_address, value_size));
    // };

    public func sized_iter_to_array<A>(iter: Iter<A>, size: Nat): [A] {
        Array.tabulate(size, func(_i: Nat): A {
            switch (iter.next()) {
                case (null) Debug.trap("sized_iter_to_array: found null before end of iter");
                case (?(a)) return a;
            };
        });
    };

    public func unwrap<T>(optional: ?T, trap_msg: Text) : T {
        switch(optional) {
            case (?v) return v;
            case (_) return Debug.trap(trap_msg);
        };
    };

    public func shuffle_buffer<A>(fuzz : Fuzz.Fuzzer, buffer : Buffer.Buffer<A>) {
        for (i in Iter.range(0, buffer.size() - 3)) {
            let j = fuzz.nat.randomRange(i + 1, buffer.size() - 1);
            let tmp = buffer.get(i);
            buffer.put(i, buffer.get(j));
            buffer.put(j, tmp);
        };
    };

    public func send_error<OldOk, NewOk, Error>(res: Result<OldOk, Error>): Result<NewOk, Error>{
        switch (res) {
            case (#ok(_)) Prelude.unreachable();
            case (#err(errorMsg)) #err(errorMsg);
        };
    };
    
    public func nat_to_blob(num : Nat, nbytes : Nat) : Blob {
        var n = num;

        let bytes = Array.reverse(
            Array.tabulate(
                nbytes,
                func(_ : Nat) : Nat8 {
                    if (n == 0) {
                        return 0;
                    };

                    let byte = Nat8.fromNat(n % 256);
                    n /= 256;
                    byte;
                },
            )
        );

        return Blob.fromArray(bytes);
    };

    public func blob_to_nat(blob: Blob): Nat {
        var n = 0;

        for (byte in blob.vals()){
            n *= 256;
            n += Nat8.toNat(byte);
        };

        return n;
    };

    public func byte_iter_to_nat(iter: Iter<Nat8>): Nat {
        var n = 0;

        for (byte in iter){
            n *= 256;
            n += Nat8.toNat(byte);
        };

        return n;
    };


};
