# 07 - WebSocket Transport

## Analysis

WebSocket client frames must be masked, and the original implementation XORed every payload byte in Python. Partial sends also replaced `_sendbuffer` with a full slice after every socket write. These costs affect only WebSocket users but grow rapidly with payload size.

## Preparation

Frame creation is measured at 2, 16, 128, 1,024, and 65,536 bytes. Tests decode the generated mask, exercise extended lengths, partial sends, ping/pong, binary frames, and continuation frames. The HTTP handshake is excluded because it is connection-time work.

## Expected Gain

Priority: P2. Target at least +15% for 128-byte frame creation with no small-frame regression and bounded temporary memory.

## Acceptance Criteria

- At least +15% at 128 bytes.
- No regression for 2/16-byte frames.
- Temporary masking work is bounded to 64-KiB chunks.
- Partial sends emit one correct frame without copying the remaining buffer.
- Existing WebSocket unit and integration tests pass.

## Before Measurement

| Payload | Before frames/s |
| ---: | ---: |
| 16 B | 237,684 |
| 128 B | 55,218 |
| 1,024 B | 8,342 |
| 65,536 B | 130 |

## Implementation

- Keep the short Python XOR loop below 64 bytes, where native-integer setup is more expensive.
- Mask larger payloads using `int.from_bytes()`/XOR/`to_bytes()` in bounded 64-KiB chunks.
- Build frame headers with cached `Struct` instances.
- Avoid generating a mask key for unmasked server control replies.
- Track `_sendbuffer_head` and send a memoryview instead of slicing the remaining buffer after partial writes.
- Leave handshake parsing unchanged.

## After Measurements

| Payload | After frames/s | Delta |
| ---: | ---: | ---: |
| 16 B | 276,686 | **+16.4%** |
| 128 B | 140,558 | **+154.5%** |
| 1,024 B | 75,130 | **+800.6%** |
| 65,536 B | 2,496 | **+1,820%** |

The permanent harness subsequently reports roughly 260k, 170k, and 101k frames/s for 16, 128, and 1,024 bytes; run-to-run CPU scaling affects absolute values but not the verdict.

## Results Analysis

A hybrid is necessary: allocating a 64-KiB mask block for tiny frames regressed them, while the original short loop is already efficient at that scale. Native big-integer operations dominate positively from 128 bytes upward. Chunking prevents temporary integers from scaling with an arbitrarily large frame.

## Verdict

**GO.** The 128-byte threshold is exceeded by a wide margin, small frames improve, and all 32 focused WebSocket tests pass.
