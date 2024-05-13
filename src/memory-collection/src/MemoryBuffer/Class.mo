import Iter "mo:base/Iter";
import Order "mo:base/Order";
import Nat "mo:base/Nat";

import RevIter "mo:itertools/RevIter";

import Blobify "../Blobify";
import MemoryBuffer "Base";
import VersionedMemoryBuffer "Versioned";
import Migrations "Migrations";
import MemoryCmp "../MemoryCmp";

module {

    /// ```motoko
    ///     let mbuffer = MemoryBufferClass.new();
    /// ```

    type Iter<A> = Iter.Iter<A>;
    type RevIter<A> = RevIter.RevIter<A>;
    type Order = Order.Order;
    type Blobify<A> = Blobify.Blobify<A>;

    public type MemoryBuffer<A> = Migrations.MemoryBuffer<A>;
    public type VersionedMemoryBuffer<A> = Migrations.VersionedMemoryBuffer<A>;

    /// Creates a new stable store for the memory buffer.
    public func new<A>() : VersionedMemoryBuffer<A> = VersionedMemoryBuffer.new();

    /// Creates a new stable store for the memory buffer.
    public func newStableStore<A>() : VersionedMemoryBuffer<A> = VersionedMemoryBuffer.new();

    public func upgrade<A>(versions : VersionedMemoryBuffer<A>) : VersionedMemoryBuffer<A> {
        Migrations.upgrade<A>(versions);
    };

    public class MemoryBufferClass<A>(versions : VersionedMemoryBuffer<A>, blobify : Blobify<A>) {
        let internal = Migrations.getCurrentVersion(versions);

        public func add(elem: A) = MemoryBuffer.add<A>(internal, blobify, elem);
        public func get(i: Nat) : A = MemoryBuffer.get<A>(internal, blobify, i);
        public func size() : Nat = MemoryBuffer.size<A>(internal);
        public func bytes() : Nat = MemoryBuffer.bytes<A>(internal);
        public func metadataBytes() : Nat = MemoryBuffer.metadataBytes<A>(internal);
        public func capacity() : Nat = MemoryBuffer.capacity<A>(internal);
        public func metadataCapacity() : Nat = MemoryBuffer.metadataCapacity<A>(internal);
        public func put(i: Nat, elem: A) = MemoryBuffer.put<A>(internal, blobify, i, elem);
        public func getOpt(i: Nat) : ?A = MemoryBuffer.getOpt<A>(internal, blobify, i);
        public func vals() : RevIter<A> = MemoryBuffer.vals<A>(internal, blobify);
        public func items() : RevIter<(Nat, A)> = MemoryBuffer.items<A>(internal, blobify);
        public func blobs() : RevIter<Blob> = MemoryBuffer.blobs<A>(internal);
        public func swap(i: Nat, j: Nat) = MemoryBuffer.swap<A>(internal, i, j);
        public func swapRemove(i: Nat) : A = MemoryBuffer.swapRemove<A>(internal, blobify, i);

        public func remove(i: Nat) : A = MemoryBuffer.remove<A>(internal, blobify, i);
        public func removeLast() : ?A = MemoryBuffer.removeLast<A>(internal, blobify);
        public func insert(i: Nat, elem: A) = MemoryBuffer.insert<A>(internal, blobify, i, elem);
        public func sortUnstable(cmp: MemoryCmp.MemoryCmp<A> ) = MemoryBuffer.sortUnstable<A>(internal, blobify, cmp);

        public func clear() = MemoryBuffer.clear<A>(internal);
        public func toArray() : [A] = MemoryBuffer.toArray<A>(internal, blobify);

        public func _getInternalRegion() : MemoryBuffer<A> = internal;
        public func _getBlobifyFn() : Blobify<A> = blobify;
    };

    public func init<A>(internal: VersionedMemoryBuffer<A>, blobify: Blobify<A>, size: Nat, val: A) : MemoryBufferClass<A> {
        let mbuffer = MemoryBufferClass(internal, blobify);

        for (_ in Iter.range(0, size - 1)){
            mbuffer.add(val);
        };

        return mbuffer;
    };

    // public func tabulate<A>(internal: VersionedMemoryBuffer<A>, blobify: Blobify<A>, size: Nat, f: (Nat) -> A) : MemoryBufferClass<A> {
    //     MemoryBuffer.tabulate(internal, blobify, size, f);
    //     return MemoryBufferClass(internal, blobify);
    // };

    // public func fromArray<A>(internal: VersionedMemoryBuffer<A>, blobify: Blobify<A>, arr: [A]) : MemoryBufferClass<A> {
    //     MemoryBuffer.fromArray(internal, blobify, arr);
    //     return MemoryBufferClass(internal, blobify);
    // };

    public func toArray<A>(mbuffer: MemoryBufferClass<A>) : [A] {
        return MemoryBuffer.toArray(mbuffer._getInternalRegion(), mbuffer._getBlobifyFn());
    };

    // public func fromIter<A>(internal: VersionedMemoryBuffer<A>, blobify: Blobify<A>, iter: Iter<A>) : MemoryBufferClass<A> {
    //     MemoryBuffer.fromIter(internal, blobify, iter);
    //     return MemoryBufferClass(internal, blobify);
    // };

    public func append<A>(mbuffer: MemoryBufferClass<A>, b: MemoryBufferClass<A>) {
        MemoryBuffer.append(mbuffer._getInternalRegion(), mbuffer._getBlobifyFn(), b._getInternalRegion());
    };

    public func appendArray<A>(mbuffer: MemoryBufferClass<A>, arr: [A]) {
        MemoryBuffer.appendArray<A>(mbuffer._getInternalRegion(), mbuffer._getBlobifyFn(), arr);
    };

    public func appendBuffer<A>(mbuffer: MemoryBufferClass<A>, other : { vals : () -> Iter<A> }) {
        MemoryBuffer.appendBuffer<A>(mbuffer._getInternalRegion(), mbuffer._getBlobifyFn(), other);
    };

    public func blocks<A>(mbuffer: MemoryBufferClass<A>) : RevIter<(Nat, Nat)> = MemoryBuffer.blocks<A>(mbuffer._getInternalRegion());
    
    public func reverse<A>(mbuffer: MemoryBufferClass<A>) = MemoryBuffer.reverse<A>(mbuffer._getInternalRegion());

}