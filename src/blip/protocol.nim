# protocol.nim

## Constants and types defined by the BLIP protocol.
##
## Specification: https://github.com/couchbaselabs/BLIP-Cpp/blob/master/docs/BLIP%20Protocol.md

const BLIPWebSocketProtocol* = "BLIP_3" ## WebSocket subprotocol name for BLIP

type MessageType* = enum
    ## Message type, encoded in the 3 low bits of flags
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

const kIncomingAckThreshold* =  50000   # Send an ack at this incoming unacked-byte count
const kOutgoingAckThreshold* = 100000   # Stop sending a message at this unacked-byte count

type MessageNo* = distinct uint64

const ProfileProperty*      = "Profile"
const ErrorDomainProperty*  = "Error-Domain"
const ErrorCodeProperty*    = "Error-Code"

const BLIPErrorDomain*      = "BLIP"
const HTTPErrorDomain*      = "HTTP"

type BlipException* = object of CatchableError  ## Fatal exceptions (protocol errors)

proc `+`*(n: MessageNo, i: int): MessageNo = MessageNo(int64(n) + i)
proc `++`*(n: var MessageNo): MessageNo {.discardable.} =
    n = n + 1
    return n
proc `==`*(n: MessageNo, m: MessageNo): bool = uint64(n) == uint64(m)
proc `<`* (n: MessageNo, m: MessageNo): bool = uint64(n) <  uint64(m)
proc `<=`*(n: MessageNo, m: MessageNo): bool = uint64(n) <= uint64(m)
