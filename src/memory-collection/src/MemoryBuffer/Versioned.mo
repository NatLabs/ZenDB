/// A memory buffer is a data structure that stores a sequence of values in memory.

import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Result "mo:base/Result";
import Order "mo:base/Order";

import MemoryRegion "mo:memory-region/MemoryRegion";
import RevIter "mo:itertools/RevIter";

import Blobify "../Blobify";
import MemoryBuffer "Base";
import Migrations "Migrations";
import MemoryCmp "../MemoryCmp";

module VersionedMemoryBuffer {
    type Iter<A> = Iter.Iter<A>;
    type RevIter<A> = RevIter.RevIter<A>;
    type Result<A, B> = Result.Result<A, B>;
    type MemoryRegion = MemoryRegion.MemoryRegion;

    type MemoryRegionV0 = MemoryRegion.MemoryRegionV0;
    type MemoryRegionV1 = MemoryRegion.MemoryRegionV1;

    type Pointer = MemoryRegion.Pointer;
    type Order = Order.Order;

    public type Blobify<A> = Blobify.Blobify<A>;
    public type MemoryBuffer<A> = Migrations.MemoryBuffer<A>;
    public type VersionedMemoryBuffer<A> = Migrations.VersionedMemoryBuffer<A>;

    public func new<A>() : VersionedMemoryBuffer<A> {
        return MemoryBuffer.toVersioned(MemoryBuffer.new());
    };

    public func upgrade<A>(self : VersionedMemoryBuffer<A>) : VersionedMemoryBuffer<A> {
        Migrations.upgrade(self);
    };

    public func verify<A>(self : VersionedMemoryBuffer<A>) : Result<(), Text> {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.verify(state);
    };

    public func init<A>(self : Blobify<A>, size : Nat, val : A) : VersionedMemoryBuffer<A> {
        return MemoryBuffer.toVersioned(MemoryBuffer.init(self, size, val));
    };

    public func tabulate<A>(blobify : Blobify<A>, size : Nat, fn : (i : Nat) -> A) : VersionedMemoryBuffer<A> {
        return MemoryBuffer.toVersioned(MemoryBuffer.tabulate(blobify, size, fn));
    };

    public func fromArray<A>(blobify : Blobify<A>, arr : [A]) : VersionedMemoryBuffer<A> {
        return MemoryBuffer.toVersioned(MemoryBuffer.fromArray(blobify, arr));
    };

    public func fromIter<A>(blobify : Blobify<A>, iter : Iter<A>) : VersionedMemoryBuffer<A> {
        return MemoryBuffer.toVersioned(MemoryBuffer.fromIter(blobify, iter));
    };

    public func size<A>(self : VersionedMemoryBuffer<A>) : Nat {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.size(state);
    };

    public func bytes<A>(self : VersionedMemoryBuffer<A>) : Nat {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.bytes(state);
    };

    public func metadataBytes<A>(self : VersionedMemoryBuffer<A>) : Nat {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.metadataBytes(state);
    };

    public func totalBytes<A>(self : VersionedMemoryBuffer<A>) : Nat {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.totalBytes(state);
    };

    public func capacity<A>(self : VersionedMemoryBuffer<A>) : Nat {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.capacity(state);
    };

    public func put<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>, index : Nat, value : A) {
        let state = Migrations.getCurrentVersion(self);
        MemoryBuffer.put(state, blobify, index, value);
    };

    public func getOpt<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>, index : Nat) : ?A {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.getOpt(state, blobify, index);
    };

    public func get<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>, index : Nat) : A {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.get(state, blobify, index);
    };

    public func _get_pointer<A>(self : VersionedMemoryBuffer<A>, index : Nat) : Nat {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer._get_pointer(state, index);
    };

    public func _get_memory_block<A>(self : VersionedMemoryBuffer<A>, index : Nat) : (Nat, Nat) {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer._get_memory_block(state, index);
    };

    public func _get_blob<A>(self : VersionedMemoryBuffer<A>, index : Nat) : Blob {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer._get_blob<A>(state, index);
    };

    public func add<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>, value : A) {
        let state = Migrations.getCurrentVersion(self);
        MemoryBuffer.add(state, blobify, value);
    };

    public func append<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>, other : VersionedMemoryBuffer<A>) {
        let curr_state = Migrations.getCurrentVersion(self);
        let other_state = Migrations.getCurrentVersion(other);
        MemoryBuffer.append(curr_state, blobify, other_state);
    };

    public func vals<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>) : RevIter<A> {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.vals(state, blobify);
    };

    public func items<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>) : RevIter<(index : Nat, value : A)> {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.items(state, blobify);
    };

    public func blobs<A>(self : VersionedMemoryBuffer<A>) : RevIter<Blob> {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.blobs(state);
    };

    public func pointers<A>(self : VersionedMemoryBuffer<A>) : RevIter<Nat> {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.pointers(state);
    };

    public func blocks<A>(self: VersionedMemoryBuffer<A>) : RevIter<(Nat, Nat)> {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.blocks(state);
    };

    public func remove<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>, index : Nat) : A {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.remove(state, blobify, index);
    };

    public func removeLast<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>) : ?A {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.removeLast(state, blobify);
    };

    public func swap<A>(self : VersionedMemoryBuffer<A>, index_a : Nat, index_b : Nat) {
        let state = Migrations.getCurrentVersion(self);
        MemoryBuffer.swap(state, index_a, index_b);
    };

    public func swapRemove<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>, index : Nat) : A {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.swapRemove(state, blobify, index);
    };

    public func reverse<A>(self : VersionedMemoryBuffer<A>) {
        let state = Migrations.getCurrentVersion(self);
        MemoryBuffer.reverse(state);
    };

    public func clear<A>(self : VersionedMemoryBuffer<A>) {
        let state = Migrations.getCurrentVersion(self);
        MemoryBuffer.clear(state);
    };

    public func clone<A>(self : VersionedMemoryBuffer<A>) : VersionedMemoryBuffer<A> {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.toVersioned(MemoryBuffer.clone(state));
    };

    public func insert<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>, index : Nat, value : A) {
        let state = Migrations.getCurrentVersion(self);
        MemoryBuffer.insert(state, blobify, index, value);
    };

    public func sortUnstable<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>, cmp : MemoryCmp.MemoryCmp<A> ) {
        let state = Migrations.getCurrentVersion(self);
        MemoryBuffer.sortUnstable(state, blobify, cmp);
    };

    public func toArray<A>(self : VersionedMemoryBuffer<A>, blobify : Blobify<A>) : [A] {
        let state = Migrations.getCurrentVersion(self);
        return MemoryBuffer.toArray(state, blobify);
    };

};
