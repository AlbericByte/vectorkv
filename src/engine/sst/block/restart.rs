use crate::engine::sst::format::decode_fixed32;

/// 解析 block 末尾的 restart array
///
/// 返回：
/// - Vec<u32>，每个元素是 restart entry 的 offset
pub fn parse_restarts(block: &[u8]) -> Vec<u32> {
    // block 至少要能放下 num_restarts
    if block.len() < 4 {
        return Vec::new();
    }

    let block_len = block.len();

    // 1️⃣ 读取 num_restarts（最后 4 字节）
    let num_restarts =
        decode_fixed32(&block[block_len - 4..]) as usize;

    // 防御：num_restarts 为 0 是合法的（空 block）
    if num_restarts == 0 {
        return Vec::new();
    }

    // 2️⃣ restart array 起始位置
    let restarts_bytes = num_restarts * 4;

    // 防御：block 长度不够
    if block_len < 4 + restarts_bytes {
        return Vec::new();
    }

    let restarts_offset = block_len - 4 - restarts_bytes;

    // 3️⃣ 逐个读取 restart offset
    let mut restarts = Vec::with_capacity(num_restarts);

    for i in 0..num_restarts {
        let pos = restarts_offset + i * 4;
        let off = decode_fixed32(&block[pos..pos + 4]);
        restarts.push(off);
    }

    restarts
}
