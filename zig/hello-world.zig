const std = @import("std");

pub fn main() void {
    const x: u128 = 2<<125;
    var y: i128 = -@as(i128, x);
    const a = [_]i128{@as(i128, x),y};
    std.debug.print("x={}, y={}, a={}\n", .{y, x, a});
}
