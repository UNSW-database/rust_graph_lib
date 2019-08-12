@0x923748e1a087dd8d;

interface Graph {
    neighbors @0 (x :UInt32) -> (y :List(UInt32));
}