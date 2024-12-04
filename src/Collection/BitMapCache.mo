import BitMap "mo:bit-map";
import Map "mo:map/Map";

module {
    let { thash } = Map;

    public class BitMapCache() {

        let cache = Map.new<Text, BitMap.BitMap>();

        // public func get(key : Text) : BitMap.BitMap {
        //     switch (Map.get(cache, key)) {
        //         case (?value) {
        //             return value;
        //         };
        //         case (null) {
        //             let value = BitMap.new();
        //             Map.set(cache, key, value);
        //             return value;
        //         };
        //     };
        // };

    };
};
