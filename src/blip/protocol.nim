# protocol.nim
#
# Specification:
#    https://github.com/couchbaselabs/BLIP-Cpp/blob/master/docs/BLIP%20Protocol.md

const BLIPWebSocketProtocol* = "BLIP_3"

type MessageType* = enum
    kRequestType     = (0'u8, "REQ")  # A message initiated by a peer
    kResponseType    = (1'u8, "RES")  # A response to a Request
    kErrorType       = (2'u8, "ERR")  # A response indicating failure
    kAckRequestType  = (4'u8, "ARQ")  # Acknowledgement of data received from a Request (internal)
    kAckResponseType = (5'u8, "ARS")  # Acknowledgement of data received from a Response (internal)

const kTypeMask*        = 0x07'u8 # These 3 bits hold a MessageType
const kCompressed*      = 0x08'u8 # Message payload is gzip-deflated
const kUrgent*          = 0x10'u8 # Message is given priority delivery
const kNoReply*         = 0x20'u8 # Request only: no response desired
const kMoreComing*      = 0x40'u8 # Used only in frames, not in messages

type MessageNo* = distinct uint64

const ProfileProperty*      = "Profile"
const ErrorDomainProperty*  = "Error-Domain"
const ErrorCodeProperty*    = "Error-Code"

const BLIPErrorDomain*      = "BLIP"
const HTTPErrorDomain*      = "HTTP"

type BlipException* = object of CatchableError

proc `+`*(n: MessageNo, i: int): MessageNo = MessageNo(int64(n) + i)
proc `++`*(n: var MessageNo): MessageNo {.discardable.} =
    n = n + 1
    return n
proc `==`*(n: MessageNo, m: MessageNo): bool = uint64(n) == uint64(m)
proc `<`* (n: MessageNo, m: MessageNo): bool = uint64(n) <  uint64(m)
proc `<=`*(n: MessageNo, m: MessageNo): bool = uint64(n) <= uint64(m)

proc flagsToString*(flags: byte): string =
    result = "----"
    if (flags and kMoreComing) != 0: result[0] = '+'
    if (flags and kNoReply) != 0:    result[1] = 'x'
    if (flags and kUrgent) != 0:     result[2] = '!'
    if (flags and kCompressed) != 0: result[3] = 'z'
