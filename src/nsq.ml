open Base
open Eio.Std

let client_version = "0.6.0"
let default_nsqd_port = 4150
let default_lookupd_port = 4161
let network_buffer_size = 16 * 1024
let lookupd_error_threshold = 5

module Seconds = struct
  type t = float [@@deriving yojson]

  let of_float f = f
  let value s = s
end

let recalculate_rdy_interval = Seconds.of_float 60.0
let default_backoff = Seconds.of_float 5.0
let max_backoff = Seconds.of_float 600.0

module Milliseconds = struct
  type t = int64 [@@deriving yojson]

  let of_int64 i = i
  let value i = i
  let of_seconds s = Seconds.value s *. 1000.0 |> Int64.of_float |> of_int64
end

let default_requeue_delay = Milliseconds.of_int64 5000L

module MessageID = struct
  type t = MessageID of bytes

  let of_bytes b = MessageID b
  let to_string = function MessageID id -> Bytes.to_string id
end

let ephemeral s = s ^ "#ephemeral"

module Topic = struct
  type t = Topic of string | TopicEphemeral of string

  let to_string = function Topic s -> s | TopicEphemeral s -> ephemeral s

  let%expect_test "to_string" =
    List.iter
      ~f:(fun h -> to_string h |> Stdlib.print_endline)
      [ Topic "topic"; TopicEphemeral "topic" ];
    [%expect {|
    topic
    topic#ephemeral |}]
end

module Channel = struct
  type t = Channel of string | ChannelEphemeral of string

  let to_string = function Channel s -> s | ChannelEphemeral s -> ephemeral s

  let%expect_test "to_string" =
    List.iter
      ~f:(fun h -> to_string h |> Stdlib.print_endline)
      [ Channel "chan"; ChannelEphemeral "chan" ];
    [%expect {|
    chan
    chan#ephemeral |}]
end

module FrameType = struct
  type t = FrameResponse | FrameError | FrameMessage | FrameUnknown of int32

  let of_int32 = function
    | 0l -> FrameResponse
    | 1l -> FrameError
    | 2l -> FrameMessage
    | i -> FrameUnknown i
end

type raw_frame = { frame_type : int32; data : Bytes.t }

type raw_message = {
  timestamp : int64;
  attempts : int;
  id : MessageID.t;
  body : Bytes.t;
}

module Address = struct
  type t = Host of string | HostPort of string * int
  [@@deriving sexp_of, compare, hash, equal]

  let host s = Host s
  let host_port a p = HostPort (a, p)

  let to_string a =
    match a with Host a -> a | HostPort (a, p) -> Printf.sprintf "%s:%d" a p

  let%expect_test "to_string" =
    List.iter
      ~f:(fun h -> to_string h |> Stdlib.print_endline)
      [ Host "example.com"; HostPort ("example.com", 123) ];
    [%expect {|
      example.com
      example.com:123 |}]
end

module IdentifyConfig = struct
  (* This will be encoded to JSON and sent with the IDENTIFY command *)
  type t = {
    heartbeat_interval : Milliseconds.t;
    client_id : string;
    hostname : string;
    user_agent : string;
    output_buffer_size : int;
    (* buffer size on bytes the server should use  *)
    output_buffer_timeout : Milliseconds.t;
    sample_rate : int;
  }
  [@@deriving yojson { strict = false }]
end

type command =
  | IDENTIFY of IdentifyConfig.t
  | MAGIC
  | SUB of Topic.t * Channel.t
  | PUB of Topic.t * Bytes.t
  | MPUB of Topic.t * Bytes.t list
  | REQ of MessageID.t * Milliseconds.t
  | FIN of MessageID.t
  | RDY of int
  | NOP

let try_with_string f =
  match Result.try_with f with
  | Ok _ as x -> x
  | Error e -> Error (Exn.to_string e)

module ServerMessage = struct
  type t =
    | ResponseOk
    | Heartbeat
    | ErrorInvalid of string
    | ErrorBadTopic of string
    | ErrorBadChannel of string
    | ErrorFINFailed of string
    | ErrorREQFailed of string
    | ErrorTOUCHFailed of string
    | Message of raw_message

  let to_string = function
    | ResponseOk -> "OK"
    | Heartbeat -> "Heartbeat"
    | ErrorInvalid s -> Printf.sprintf "ErrorInvalid: %s" s
    | ErrorBadTopic s -> Printf.sprintf "ErrorBadTopic: %s" s
    | ErrorBadChannel s -> Printf.sprintf "ErrorBadChannel: %s" s
    | ErrorFINFailed s -> Printf.sprintf "ErrorFINFailed: %s" s
    | ErrorREQFailed s -> Printf.sprintf "ErrorREQFailed: %s" s
    | ErrorTOUCHFailed s -> Printf.sprintf "ErrTouchFailed: %s" s
    | Message m ->
        Printf.sprintf "Message with ID %s" (MessageID.to_string m.id)

  let parse_response_body body =
    let body = Bytes.to_string body in
    match body with
    | "OK" -> Ok ResponseOk
    | "_heartbeat_" -> Ok Heartbeat
    | _ -> Error (Printf.sprintf "Unknown response: %s" body)

  let parse_message_body_exn body =
    let open Stdlib in
    let timestamp = Bytes.get_int64_be body 0 in
    let attempts = Bytes.get_uint16_be body 8 in
    let length = Bytes.length body in
    let id = MessageID.of_bytes (Bytes.sub body 10 16) in
    let body = Bytes.sub body 26 (length - 26) in
    Message { timestamp; attempts; id; body }

  let parse_message_body body =
    try_with_string (fun () -> parse_message_body_exn body)

  let parse_error_body body =
    let open Result in
    match String.split ~on:' ' (Bytes.to_string body) with
    | [ code; detail ] -> (
        match code with
        | "E_INVALID" -> return (ErrorInvalid detail)
        | "E_BAD_TOPIC" -> return (ErrorBadTopic detail)
        | "E_BAD_CHANNEL" -> return (ErrorBadChannel detail)
        | "E_FIN_FAILED" -> return (ErrorFINFailed detail)
        | "E_REQ_FAILED" -> return (ErrorREQFailed detail)
        | "E_TOUCH_FAILED" -> return (ErrorTOUCHFailed detail)
        | _ -> fail (Printf.sprintf "Unknown error code: %s. %s" code detail))
    | _ ->
        fail (Printf.sprintf "Malformed error code: %s" (Bytes.to_string body))

  let of_raw_frame raw =
    match FrameType.of_int32 raw.frame_type with
    | FrameResponse -> parse_response_body raw.data
    | FrameMessage -> parse_message_body raw.data
    | FrameError -> parse_error_body raw.data
    | FrameUnknown i ->
        Result.Error (Printf.sprintf "Unknown frame type: %li" i)

  let%expect_test "parse_response_body" =
    let cases = [ "OK"; "_heartbeat_"; "wat" ] in
    List.iter cases ~f:(fun s ->
        let r = parse_response_body (Bytes.of_string s) in
        let str = match r with Ok m -> to_string m | Error e -> "ERR: " ^ e in
        Stdlib.print_endline (Printf.sprintf "%s -> %s" s str));
    [%expect
      {|
      OK -> OK
      _heartbeat_ -> Heartbeat
      wat -> ERR: Unknown response: wat
      |}]

  let%expect_test "parse_error_body" =
    let cases =
      [
        "E_INVALID detail";
        "E_BAD_TOPIC detail";
        "E_BAD_CHANNEL detail";
        "E_FIN_FAILED detail";
        "E_REQ_FAILED detail";
        "E_TOUCH_FAILED detail";
        "E_UNKNOWN detail";
        "malformed";
      ]
    in
    List.iter cases ~f:(fun s ->
        let r = parse_error_body (Bytes.of_string s) in
        let str = match r with Ok m -> to_string m | Error e -> "ERR: " ^ e in
        Stdlib.print_endline (Printf.sprintf "%s -> %s" s str));
    [%expect
      {|
      E_INVALID detail -> ErrorInvalid: detail
      E_BAD_TOPIC detail -> ErrorBadTopic: detail
      E_BAD_CHANNEL detail -> ErrorBadChannel: detail
      E_FIN_FAILED detail -> ErrorFINFailed: detail
      E_REQ_FAILED detail -> ErrorREQFailed: detail
      E_TOUCH_FAILED detail -> ErrTouchFailed: detail
      E_UNKNOWN detail -> ERR: Unknown error code: E_UNKNOWN. detail
      malformed -> ERR: Malformed error code: malformed
      |}]

  let%expect_test "parse_message_body round-trip" =
    let body = Bytes.create 26 in
    Stdlib.Bytes.set_int64_be body 0 1234567890L;
    Stdlib.Bytes.set_uint16_be body 8 7;
    Stdlib.Bytes.blit_string "0123456789abcdef" 0 body 10 16;
    let body = Stdlib.Bytes.cat body (Bytes.of_string "hello") in
    let r = parse_message_body body in
    (match r with
    | Ok (Message m) ->
        Stdlib.print_endline
          (Printf.sprintf "ts=%Ld attempts=%d id=%s body=%s" m.timestamp
             m.attempts (MessageID.to_string m.id) (Bytes.to_string m.body))
    | Ok _ -> Stdlib.print_endline "unexpected"
    | Error e -> Stdlib.print_endline ("ERR: " ^ e));
    [%expect {| ts=1234567890 attempts=7 id=0123456789abcdef body=hello |}]

  let%expect_test "of_raw_frame" =
    let frames =
      [
        { frame_type = 0l; data = Bytes.of_string "OK" };
        { frame_type = 1l; data = Bytes.of_string "E_INVALID nope" };
        { frame_type = 99l; data = Bytes.create 0 };
      ]
    in
    List.iter frames ~f:(fun f ->
        let r = of_raw_frame f in
        let str = match r with Ok m -> to_string m | Error e -> "ERR: " ^ e in
        Stdlib.print_endline (Printf.sprintf "type=%li -> %s" f.frame_type str));
    [%expect
      {|
      type=0 -> OK
      type=1 -> ErrorInvalid: nope
      type=99 -> ERR: Unknown frame type: 99
      |}]
end

module Lookup = struct
  type producer = {
    remote_address : string;
    hostname : string;
    broadcast_address : string;
    tcp_port : int;
    http_port : int;
    version : string;
  }
  [@@deriving yojson { strict = false }]

  type response = { channels : string list; producers : producer list }
  [@@deriving yojson { strict = false }]

  let response_of_string s =
    let open Result in
    try_with_string (fun () -> Yojson.Safe.from_string s) >>= response_of_yojson

  let producer_addresses lr =
    List.map
      ~f:(fun p -> Address.host_port p.broadcast_address p.tcp_port)
      lr.producers

  let%expect_test "response_of_string" =
    let json =
      {|{"channels":["c1","c2"],"producers":[{"remote_address":"r","hostname":"h","broadcast_address":"127.0.0.1","tcp_port":4150,"http_port":4151,"version":"1.0"}]}|}
    in
    (match response_of_string json with
    | Ok r ->
        Stdlib.print_endline
          (Printf.sprintf "channels=%d producers=%d" (List.length r.channels)
             (List.length r.producers));
        List.iter
          ~f:(fun a -> Stdlib.print_endline (Address.to_string a))
          (producer_addresses r)
    | Error e -> Stdlib.print_endline ("ERR: " ^ e));
    (match response_of_string "not-json" with
    | Ok _ -> Stdlib.print_endline "unexpected ok"
    | Error _ -> Stdlib.print_endline "malformed: error");
    (match response_of_string "{}" with
    | Ok _ -> Stdlib.print_endline "unexpected ok"
    | Error _ -> Stdlib.print_endline "missing fields: error");
    [%expect
      {|
      channels=2 producers=1
      127.0.0.1:4150
      malformed: error
      missing fields: error
      |}]
end

let log_and_return prefix r =
  match r with
  | Ok _ as ok -> ok
  | Error s as error ->
      Logs.err (fun l -> l "%s: %s" prefix s);
      error

let query_nsqlookupd ~http_client ~sw ~topic a =
  let host, port =
    match a with
    | Address.Host h -> (h, default_lookupd_port)
    | Address.HostPort (h, p) -> (h, p)
  in
  let topic_string = Topic.to_string topic in
  let uri =
    Uri.make ~scheme:"http" ~host ~port ~path:"lookup"
      ~query:[ ("topic", [ topic_string ]) ]
      ()
  in
  try
    let resp, body = Cohttp_eio.Client.get http_client ~sw uri in
    match Http.Response.status resp with
    | `OK ->
        let body_str =
          Eio.Buf_read.(parse_exn take_all) body ~max_size:Int.max_value
        in
        log_and_return "Error parsing lookup response"
          (Lookup.response_of_string body_str)
    | status ->
        Error
          (Printf.sprintf "Expected %s, got %s"
             (Http.Status.to_string `OK)
             (Http.Status.to_string status))
  with e ->
    let s = Exn.to_string e in
    log_and_return "Querying lookupd" (Error s)

(*
     PUB <topic_name>\n
     [ 4-byte size in bytes ][ N-byte binary data ]

     <topic_name> - a valid string (optionally having #ephemeral suffix)
*)
let bytes_of_pub topic data =
  let topic = Topic.to_string topic in
  let buf = Buffer.create (4 + String.length topic + 1 + 4 + Bytes.length data) in
  let len_buf = Bytes.create 4 in
  Stdlib.Bytes.set_int32_be len_buf 0 (Int32.of_int_exn (Bytes.length data));
  Buffer.add_string buf "PUB ";
  Buffer.add_string buf topic;
  Buffer.add_string buf "\n";
  Buffer.add_bytes buf len_buf;
  Buffer.add_bytes buf data;
  Buffer.contents_bytes buf

let%expect_test "bytes_of_pub" =
  let topic = Topic.Topic "TestTopic" in
  let data = Bytes.of_string "Hello World" in
  bytes_of_pub topic data |> Hex.of_bytes |> Hex.hexdump ~print_chars:false;
  (* Run it twice with different inputs to ensure independence between calls *)
  let topic = Topic.Topic "AnotherTopic" in
  let data = Bytes.of_string "Another message" in
  bytes_of_pub topic data |> Hex.of_bytes |> Hex.hexdump ~print_chars:false;
  [%expect
    {|
        00000000: 5055 4220 5465 7374 546f 7069 630a 0000
        00000001: 000b 4865 6c6c 6f20 576f 726c 64
        00000000: 5055 4220 416e 6f74 6865 7254 6f70 6963
        00000001: 0a00 0000 0f41 6e6f 7468 6572 206d 6573
        00000002: 7361 6765
      |}]

(*
    MPUB <topic_name>\n
    [ 4-byte body size ]
    [ 4-byte num messages ]
    [ 4-byte message #1 size ][ N-byte binary data ]
       ... (repeated <num_messages> times)

    <topic_name> - a valid string (optionally having #ephemeral suffix)
*)
let bytes_of_mpub topic bodies =
  let topic = Topic.to_string topic in
  let body_size =
    List.fold_left ~f:(fun a b -> a + Bytes.length b) ~init:0 bodies
  in
  let num_messages = List.length bodies in
  let buf =
    Buffer.create
      (5 + String.length topic + 1 + 8 + (4 * num_messages) + body_size)
  in
  let header = Bytes.create 8 in
  let size_buf = Bytes.create 4 in
  Buffer.add_string buf "MPUB ";
  Buffer.add_string buf topic;
  Buffer.add_string buf "\n";
  Stdlib.Bytes.set_int32_be header 0 (Int32.of_int_exn body_size);
  Stdlib.Bytes.set_int32_be header 4 (Int32.of_int_exn num_messages);
  Buffer.add_bytes buf header;
  List.iter
    ~f:(fun data ->
      Stdlib.Bytes.set_int32_be size_buf 0
        (Int32.of_int_exn (Bytes.length data));
      Buffer.add_bytes buf size_buf;
      Buffer.add_bytes buf data)
    bodies;
  Buffer.contents_bytes buf

let%expect_test "bytes_of_mpub" =
  let topic = Topic.Topic "TestTopic" in
  let bodies =
    [ Bytes.of_string "Hello world"; Bytes.of_string "Hello again" ]
  in
  bytes_of_mpub topic bodies |> Hex.of_bytes |> Hex.hexdump ~print_chars:false;
  [%expect
    {|
        00000000: 4d50 5542 2054 6573 7454 6f70 6963 0a00
        00000001: 0000 1600 0000 0200 0000 0b48 656c 6c6f
        00000002: 2077 6f72 6c64 0000 000b 4865 6c6c 6f20
        00000003: 6167 6169 6e
      |}]

(*
   IDENTIFY\n
   [ 4-byte size in bytes ][ N-byte JSON data ]
*)
let bytes_of_identify c =
  let data = IdentifyConfig.to_yojson c |> Yojson.Safe.to_string in
  let length = String.length data in
  let len_buf = Bytes.create 4 in
  Stdlib.Bytes.set_int32_be len_buf 0 (Int32.of_int_exn length);
  let buf = Buffer.create (9 + 4 + length) in
  Buffer.add_string buf "IDENTIFY\n";
  Buffer.add_bytes buf len_buf;
  Buffer.add_string buf data;
  Buffer.contents_bytes buf

let%expect_test "bytes_of_identify" =
  let open IdentifyConfig in
  let id =
    {
      heartbeat_interval = 10L;
      client_id = "test";
      hostname = "test";
      user_agent = "test";
      output_buffer_size = 100;
      output_buffer_timeout = 60L;
      sample_rate = 50;
    }
  in
  bytes_of_identify id |> Hex.of_bytes |> Hex.hexdump ~print_chars:false;
  [%expect
    {|
        00000000: 4944 454e 5449 4659 0a00 0000 977b 2268
        00000001: 6561 7274 6265 6174 5f69 6e74 6572 7661
        00000002: 6c22 3a31 302c 2263 6c69 656e 745f 6964
        00000003: 223a 2274 6573 7422 2c22 686f 7374 6e61
        00000004: 6d65 223a 2274 6573 7422 2c22 7573 6572
        00000005: 5f61 6765 6e74 223a 2274 6573 7422 2c22
        00000006: 6f75 7470 7574 5f62 7566 6665 725f 7369
        00000007: 7a65 223a 3130 302c 226f 7574 7075 745f
        00000008: 6275 6666 6572 5f74 696d 656f 7574 223a
        00000009: 3630 2c22 7361 6d70 6c65 5f72 6174 6522
        00000010: 3a35 307d
      |}]

let bytes_of_command = function
  | MAGIC -> Bytes.of_string "  V2"
  | IDENTIFY c -> bytes_of_identify c
  | NOP -> Bytes.of_string "NOP\n"
  | RDY i -> Printf.sprintf "RDY %i\n" i |> Bytes.of_string
  | FIN id ->
      Printf.sprintf "FIN %s\n" (MessageID.to_string id) |> Bytes.of_string
  | SUB (t, c) ->
      Printf.sprintf "SUB %s %s\n" (Topic.to_string t) (Channel.to_string c)
      |> Bytes.of_string
  | REQ (id, delay) ->
      Printf.sprintf "REQ %s %Li\n" (MessageID.to_string id)
        (Milliseconds.value delay)
      |> Bytes.of_string
  | PUB (t, data) -> bytes_of_pub t data
  | MPUB (t, data) -> bytes_of_mpub t data

let%expect_test "bytes_of_command" =
  let commands =
    [
      MAGIC;
      IDENTIFY
        {
          heartbeat_interval = Milliseconds.of_int64 5000L;
          client_id = "client";
          hostname = "hostname";
          user_agent = "user_agent";
          output_buffer_size = 1024;
          output_buffer_timeout = Milliseconds.of_int64 1000L;
          sample_rate = 50;
        };
      NOP;
      RDY 10;
      SUB (Topic "Test", Channel "Test");
      REQ (MessageID.of_bytes (Bytes.of_string "ABC"), 100L);
      FIN (MessageID.of_bytes (Bytes.of_string "ABC"));
      PUB (Topic "Test", Bytes.of_string "Hello");
      MPUB (Topic "Test", [ Bytes.of_string "A"; Bytes.of_string "BB" ]);
    ]
  in
  List.iter
    ~f:(fun c ->
      bytes_of_command c |> Hex.of_bytes |> Hex.hexdump ~print_chars:false)
    commands;
  [%expect
    {|
    00000000: 2020 5632
    00000000: 4944 454e 5449 4659 0a00 0000 a87b 2268
    00000001: 6561 7274 6265 6174 5f69 6e74 6572 7661
    00000002: 6c22 3a35 3030 302c 2263 6c69 656e 745f
    00000003: 6964 223a 2263 6c69 656e 7422 2c22 686f
    00000004: 7374 6e61 6d65 223a 2268 6f73 746e 616d
    00000005: 6522 2c22 7573 6572 5f61 6765 6e74 223a
    00000006: 2275 7365 725f 6167 656e 7422 2c22 6f75
    00000007: 7470 7574 5f62 7566 6665 725f 7369 7a65
    00000008: 223a 3130 3234 2c22 6f75 7470 7574 5f62
    00000009: 7566 6665 725f 7469 6d65 6f75 7422 3a31
    00000010: 3030 302c 2273 616d 706c 655f 7261 7465
    00000011: 223a 3530 7d
    00000000: 4e4f 500a
    00000000: 5244 5920 3130 0a
    00000000: 5355 4220 5465 7374 2054 6573 740a
    00000000: 5245 5120 4142 4320 3130 300a
    00000000: 4649 4e20 4142 430a
    00000000: 5055 4220 5465 7374 0a00 0000 0548 656c
    00000001: 6c6f
    00000000: 4d50 5542 2054 6573 740a 0000 0003 0000
    00000001: 0002 0000 0001 4100 0000 0242 42
    |}]

(* A connection holds the underlying flow plus a buffered reader *)
type conn = {
  flow : [ `Generic ] Eio.Net.stream_socket_ty Eio.Resource.t;
  reader : Eio.Buf_read.t;
}

(* Timeout will only apply if > 0.0 *)
let maybe_timeout ~clock ~timeout f =
  let timeout = Seconds.value timeout in
  if Float.(timeout <= 0.0) then f ()
  else Eio.Time.with_timeout_exn clock timeout f

let send ~clock ~timeout ~conn command =
  let data = bytes_of_command command in
  maybe_timeout ~clock ~timeout (fun () ->
      Eio.Flow.copy_string (Bytes.to_string data) conn.flow)

let catch_result f = try Ok (f ()) with e -> Error (Exn.to_string e)

let connect ~sw ~net ~clock ~rng host timeout =
  let host, port =
    match host with
    | Address.Host h -> (h, default_nsqd_port)
    | Address.HostPort (h, p) -> (h, p)
  in
  let addrs =
    Eio.Net.getaddrinfo_stream net host ~service:(Int.to_string port)
  in
  let addr =
    match addrs with
    | [] -> failwith (Printf.sprintf "No addresses for %s" host)
    | xs -> List.random_element_exn ~random_state:rng xs
  in
  maybe_timeout ~clock ~timeout (fun () ->
      let flow = Eio.Net.connect ~sw net addr in
      let reader =
        Eio.Buf_read.of_flow ~initial_size:network_buffer_size
          ~max_size:Int.max_value flow
      in
      { flow; reader })

let frame_from_bytes bytes =
  let frame_type = Stdlib.Bytes.get_int32_be bytes 0 in
  let to_read = Bytes.length bytes - 4 in
  let data = Bytes.sub ~pos:4 ~len:to_read bytes in
  { frame_type; data }

let read_raw_frame ~clock ~timeout conn =
  let size = Eio.Buf_read.BE.uint32 conn.reader |> Int32.to_int_exn in
  let bytes = Bytes.create size in
  maybe_timeout ~clock ~timeout (fun () ->
      let s = Eio.Buf_read.take size conn.reader in
      Stdlib.Bytes.blit_string s 0 bytes 0 size);
  frame_from_bytes bytes

let send_expect_ok ~clock ~read_timeout ~write_timeout ~conn cmd =
  send ~clock ~timeout:write_timeout ~conn cmd;
  let raw = read_raw_frame ~clock ~timeout:read_timeout conn in
  match ServerMessage.of_raw_frame raw with
  | Ok ResponseOk -> ()
  | Ok sm ->
      failwith
        (Printf.sprintf "Expected OK, got %s" (ServerMessage.to_string sm))
  | Error e -> failwith (Printf.sprintf "Expected OK, got %s" e)

let subscribe ~clock ~read_timeout ~write_timeout ~conn topic channel =
  send_expect_ok ~clock ~read_timeout ~write_timeout ~conn
    (SUB (topic, channel))

let identify ~clock ~read_timeout ~write_timeout ~conn ic =
  send_expect_ok ~clock ~read_timeout ~write_timeout ~conn (IDENTIFY ic)

module Consumer = struct
  module Config = struct
    type t = {
      (* The total number of messages allowed in flight for all connections of this consumer *)
      max_in_flight : int;
      max_attempts : int;
      backoff_multiplier : float;
      error_threshold : int;
      (* After how many errors do we send RDY 0 and back off *)
      (* network timeouts in seconds *)
      dial_timeout : Seconds.t;
      read_timeout : Seconds.t;
      write_timeout : Seconds.t;
      lookupd_poll_interval : Seconds.t;
      lookupd_poll_jitter : float;
      max_requeue_delay : Seconds.t;
      default_requeue_delay : Seconds.t;
      (* The fields below are used in IdentifyConfig.t *)
      heartbeat_interval : Seconds.t;
      client_id : string;
      hostname : string;
      user_agent : string;
      output_buffer_size : int;
      (* buffer size on bytes the server should use  *)
      output_buffer_timeout : Seconds.t;
      sample_rate : int; (* Between 0 and 99 *)
    }
    [@@deriving fields]

    let validate t =
      let module V = Core.Validate in
      let module Maybe_bound = Core.Maybe_bound in
      let w check = V.field_folder check t in
      let bound_int ~min ~max =
        Core.Int.validate_bound ~min:(Maybe_bound.Incl min)
          ~max:(Maybe_bound.Incl max)
      in
      let bound_float ~min ~max =
        Core.Float.validate_bound ~min:(Maybe_bound.Incl min)
          ~max:(Maybe_bound.Incl max)
      in
      let string_not_blank s = String.is_empty s |> not in
      V.of_list
        (Fields.fold ~init:[]
           ~max_in_flight:(w Core.Int.validate_positive)
           ~max_attempts:(w (bound_int ~min:0 ~max:65535))
           ~dial_timeout:(w (bound_float ~min:0.1 ~max:300.0))
           ~read_timeout:(w (bound_float ~min:0.1 ~max:300.0))
           ~write_timeout:(w (bound_float ~min:0.1 ~max:300.0))
           ~lookupd_poll_interval:(w (bound_float ~min:0.1 ~max:300.0))
           ~default_requeue_delay:(w (bound_float ~min:0.0 ~max:3600.0))
           ~max_requeue_delay:(w (bound_float ~min:0.0 ~max:3600.0))
           ~lookupd_poll_jitter:(w (bound_float ~min:0.0 ~max:1.0))
           ~output_buffer_timeout:(w (bound_float ~min:0.01 ~max:300.0))
           ~output_buffer_size:(w (bound_int ~min:64 ~max:(5 * 1025 * 1000)))
           ~sample_rate:(w (bound_int ~min:0 ~max:99))
           ~heartbeat_interval:(w (bound_float ~min:1.0 ~max:300.0))
           ~client_id:(w (V.booltest string_not_blank ~if_false:"blank"))
           ~hostname:(w (V.booltest string_not_blank ~if_false:"blank"))
           ~user_agent:(w (V.booltest string_not_blank ~if_false:"blank"))
           ~error_threshold:(w (bound_int ~min:1 ~max:10000))
           ~backoff_multiplier:(w Core.Float.validate_positive))

    let create ?(max_in_flight = 1) ?(max_attempts = 5)
        ?(backoff_multiplier = 0.5) ?(error_threshold = 1)
        ?(dial_timeout = Seconds.of_float 1.0)
        ?(read_timeout = Seconds.of_float 60.0)
        ?(write_timeout = Seconds.of_float 1.0)
        ?(lookupd_poll_interval = Seconds.of_float 60.0)
        ?(lookupd_poll_jitter = 0.3)
        ?(heartbeat_interval = Seconds.of_float 60.0)
        ?(max_requeue_delay = Seconds.of_float (15.0 *. 60.0))
        ?(default_requeue_delay = Seconds.of_float 90.0)
        ?(client_id = Unix.gethostname ()) ?(hostname = Unix.gethostname ())
        ?(user_agent = Printf.sprintf "nsq-ocaml/%s" client_version)
        ?(output_buffer_size = 16 * 1024)
        ?(output_buffer_timeout = Seconds.of_float 0.25) ?(sample_rate = 0) () =
      let t =
        {
          max_in_flight;
          max_attempts;
          backoff_multiplier;
          error_threshold;
          dial_timeout;
          read_timeout;
          write_timeout;
          lookupd_poll_interval;
          lookupd_poll_jitter;
          max_requeue_delay;
          default_requeue_delay;
          heartbeat_interval;
          client_id;
          hostname;
          user_agent;
          output_buffer_size;
          output_buffer_timeout;
          sample_rate;
        }
      in
      Core.Validate.valid_or_error validate t
      |> Result.map_error ~f:Error.to_string_hum

    let to_identity_config c =
      {
        IdentifyConfig.heartbeat_interval =
          Milliseconds.of_seconds c.heartbeat_interval;
        IdentifyConfig.client_id = c.client_id;
        IdentifyConfig.hostname = c.hostname;
        IdentifyConfig.user_agent = c.user_agent;
        IdentifyConfig.output_buffer_size = c.output_buffer_size;
        IdentifyConfig.output_buffer_timeout =
          Milliseconds.of_seconds c.output_buffer_timeout;
        IdentifyConfig.sample_rate = c.sample_rate;
      }

    let%expect_test "create validation" =
      let show name r =
        match r with
        | Ok _ -> Stdlib.print_endline (name ^ " -> ok")
        | Error _ -> Stdlib.print_endline (name ^ " -> error")
      in
      show "defaults" (create ());
      show "max_in_flight=0" (create ~max_in_flight:0 ());
      show "sample_rate=100" (create ~sample_rate:100 ());
      show "sample_rate=99" (create ~sample_rate:99 ());
      show "lookupd_poll_jitter=1.5" (create ~lookupd_poll_jitter:1.5 ());
      show "client_id blank" (create ~client_id:"" ());
      show "max_attempts=-1" (create ~max_attempts:(-1) ());
      [%expect
        {|
        defaults -> ok
        max_in_flight=0 -> error
        sample_rate=100 -> error
        sample_rate=99 -> ok
        lookupd_poll_jitter=1.5 -> error
        client_id blank -> error
        max_attempts=-1 -> error
        |}]
  end

  type net = [ `Generic ] Eio.Net.ty Eio.Resource.t
  type clock = float Eio.Time.clock_ty Eio.Resource.t

  type t = {
    net : net;
    clock : clock;
    addresses : Address.t list;
    (* The number of open NSQD connections *)
    open_connections : Address.t Hash_set.t;
    topic : Topic.t;
    channel : Channel.t;
    handler : bytes -> [ `Ok | `Requeue ];
    config : Config.t;
    mode : [ `Nsqd | `Lookupd ];
    log_prefix : string;
    rng : Random.State.t;
  }

  type breaker_position = Closed | HalfOpen | Open
  type breaker_state = { position : breaker_position; error_count : int }

  let backoff_duration ~multiplier ~error_count =
    let bo = multiplier *. Float.of_int error_count in
    Float.min bo max_backoff |> Seconds.of_float

  let%expect_test "backoff_duration" =
    let test_cases = [ (1.0, 1); (1.0, 2); (2.0, 4000) ] in
    List.iteri
      ~f:(fun i (multiplier, error_count) ->
        let r = backoff_duration ~multiplier ~error_count in
        Stdlib.print_endline
          (Printf.sprintf "Case %d: Backoff = %f" i (Seconds.value r)))
      test_cases;
    [%expect
      {|
      Case 0: Backoff = 1.000000
      Case 1: Backoff = 2.000000
      Case 2: Backoff = 600.000000 |}]

  let create ~net ~clock ?(mode = `Nsqd)
      ?(config = Config.create () |> Result.ok_or_failwith) addresses topic
      channel handler =
    let open_connections = Hash_set.create (module Address) in
    {
      net :> net;
      clock :> clock;
      addresses;
      open_connections;
      topic;
      channel;
      handler;
      config;
      mode;
      log_prefix =
        Printf.sprintf "%s/%s" (Topic.to_string topic)
          (Channel.to_string channel);
      rng = Random.State.make_self_init ();
    }

  let calc_rdy ~connection_count ~max_in_flight =
    if connection_count = 0 || max_in_flight < connection_count then 1
    else max_in_flight / connection_count

  let%expect_test "calc_rdy" =
    let cases =
      [ (0, 10); (1, 1); (2, 1); (2, 10); (3, 10); (4, 4); (10, 100) ]
    in
    List.iter cases ~f:(fun (connection_count, max_in_flight) ->
        let r = calc_rdy ~connection_count ~max_in_flight in
        Stdlib.print_endline
          (Printf.sprintf "conns=%d max=%d -> %d" connection_count max_in_flight
             r));
    [%expect
      {|
      conns=0 max=10 -> 1
      conns=1 max=1 -> 1
      conns=2 max=1 -> 1
      conns=2 max=10 -> 5
      conns=3 max=10 -> 3
      conns=4 max=4 -> 1
      conns=10 max=100 -> 10
      |}]

  let rdy_per_connection c =
    calc_rdy
      ~connection_count:(Hash_set.length c.open_connections)
      ~max_in_flight:c.config.max_in_flight

  let do_after_async ~sw ~clock ~duration f =
    Fiber.fork ~sw (fun () ->
        Logs.debug (fun l ->
            l "Sleeping for %f seconds" (Seconds.value duration));
        Eio.Time.sleep clock (Seconds.value duration);
        f ())

  let handle_message handler msg max_attempts =
    let requeue_delay attempts =
      let d = Milliseconds.value default_requeue_delay in
      let attempts = Int64.of_int_exn attempts in
      Milliseconds.of_int64 Int64.(d * attempts)
    in
    let handler_result =
      match catch_result (fun () -> handler msg.body) with
      | Ok r -> r
      | Error s ->
          Logs.err (fun l -> l "Handler error: %s" s);
          `Requeue
    in
    match handler_result with
    | `Ok -> FIN msg.id
    | `Requeue ->
        if msg.attempts >= max_attempts then begin
          Logs.warn (fun l ->
              l "Discarding message %s as reached max attempts, %d"
                (MessageID.to_string msg.id)
                msg.attempts);
          FIN msg.id
        end
        else
          let delay = requeue_delay msg.attempts in
          REQ (msg.id, delay)

  let%expect_test "handle_message" =
    let mk_msg attempts =
      {
        timestamp = 0L;
        attempts;
        id = MessageID.of_bytes (Bytes.of_string "0123456789abcdef");
        body = Bytes.of_string "data";
      }
    in
    let show_cmd = function
      | FIN _ -> "FIN"
      | REQ (_, delay) ->
          Printf.sprintf "REQ delay=%Ld" (Milliseconds.value delay)
      | _ -> "OTHER"
    in
    let cases =
      [
        ("ok", (fun _ -> `Ok), mk_msg 1, 5);
        ("requeue, attempts=1, max=5", (fun _ -> `Requeue), mk_msg 1, 5);
        ("requeue, attempts=3, max=5", (fun _ -> `Requeue), mk_msg 3, 5);
        ("requeue at max", (fun _ -> `Requeue), mk_msg 5, 5);
        ("requeue past max", (fun _ -> `Requeue), mk_msg 10, 5);
        ("handler raises", (fun _ -> failwith "boom"), mk_msg 1, 5);
        ("handler raises at max", (fun _ -> failwith "boom"), mk_msg 5, 5);
      ]
    in
    List.iter cases ~f:(fun (name, h, msg, max_attempts) ->
        let r = handle_message h msg max_attempts in
        Stdlib.print_endline (Printf.sprintf "%s -> %s" name (show_cmd r)));
    [%expect
      {|
      ok -> FIN
      requeue, attempts=1, max=5 -> REQ delay=5000
      requeue, attempts=3, max=5 -> REQ delay=15000
      requeue at max -> FIN
      requeue past max -> FIN
      handler raises -> REQ delay=5000
      handler raises at max -> FIN
      |}]

  let handle_server_message server_message handler max_attempts =
    let warn_return_none name msg =
      Logs.warn (fun l -> l "%s: %s" name msg);
      None
    in
    let open ServerMessage in
    match server_message with
    | ResponseOk -> None
    | Heartbeat ->
        Logs.debug (fun l -> l "Received heartbeat");
        Some NOP
    | ErrorInvalid s -> warn_return_none "ErrorInvalid" s
    | ErrorBadTopic s -> warn_return_none "ErrorBadTopic" s
    | ErrorBadChannel s -> warn_return_none "ErrorBadChannel" s
    | ErrorFINFailed s -> warn_return_none "ErrorFINFailed" s
    | ErrorREQFailed s -> warn_return_none "ErrorREQFailed" s
    | ErrorTOUCHFailed s -> warn_return_none "ErrorTOUCHFailed" s
    | Message msg -> Some (handle_message handler msg max_attempts)

  type loop_message =
    | RawFrame of raw_frame
    | Command of command
    | TrialBreaker
    | ConnectionError of string
    | RecalcRDY

  let read_loop ~clock ~timeout conn mbox =
    let rec loop () =
      match catch_result (fun () -> read_raw_frame ~clock ~timeout conn) with
      | Ok raw ->
          Eio.Stream.add mbox (RawFrame raw);
          loop ()
      | Error s -> Eio.Stream.add mbox (ConnectionError s)
    in
    loop ()

  type breaker_action =
    | NoBreakerAction
    | OpenBreaker of Seconds.t
        (** send RDY 0 and schedule a trial after this delay *)
    | CloseBreaker of int  (** trial passed, send this RDY *)

  (** Pure decision function for the breaker state machine. Returns the next
      state and an action describing any side effect the caller must perform. *)
  let next_breaker_state ~error_threshold ~backoff_multiplier ~rdy_when_closed
      cmd bs =
    match cmd with
    | NOP -> (bs, NoBreakerAction)
    | _ -> (
        let error_state = match cmd with REQ _ -> `Error | _ -> `Ok in
        match (error_state, bs.position) with
        | `Error, (Closed | HalfOpen) ->
            let bs = { bs with error_count = bs.error_count + 1 } in
            if bs.error_count < error_threshold then (bs, NoBreakerAction)
            else
              let duration =
                backoff_duration ~multiplier:backoff_multiplier
                  ~error_count:bs.error_count
              in
              ({ bs with position = Open }, OpenBreaker duration)
        | `Error, Open -> (bs, NoBreakerAction)
        | `Ok, Closed -> (bs, NoBreakerAction)
        | `Ok, Open -> (bs, NoBreakerAction)
        | `Ok, HalfOpen ->
            ( { position = Closed; error_count = 0 },
              CloseBreaker rdy_when_closed ))

  let%expect_test "next_breaker_state" =
    let next =
      next_breaker_state ~error_threshold:3 ~backoff_multiplier:1.0
        ~rdy_when_closed:5
    in
    let dummy_id = MessageID.of_bytes (Bytes.of_string "0123456789abcdef") in
    let req = REQ (dummy_id, 0L) in
    let fin = FIN dummy_id in
    let show_pos = function
      | Closed -> "Closed"
      | HalfOpen -> "HalfOpen"
      | Open -> "Open"
    in
    let show_action = function
      | NoBreakerAction -> "NoAction"
      | OpenBreaker d -> Printf.sprintf "OpenBreaker(%f)" (Seconds.value d)
      | CloseBreaker n -> Printf.sprintf "CloseBreaker(%d)" n
    in
    let cases =
      [
        ("NOP keeps state", NOP, { position = Open; error_count = 99 });
        ("Ok+Closed", fin, { position = Closed; error_count = 0 });
        ("Ok+Open", fin, { position = Open; error_count = 5 });
        ("Ok+HalfOpen closes", fin, { position = HalfOpen; error_count = 5 });
        ("Err+Open", req, { position = Open; error_count = 5 });
        ( "Err+Closed below threshold",
          req,
          { position = Closed; error_count = 0 } );
        ( "Err+Closed at threshold opens",
          req,
          { position = Closed; error_count = 2 } );
        ("Err+HalfOpen reopens", req, { position = HalfOpen; error_count = 2 });
      ]
    in
    List.iter cases ~f:(fun (name, cmd, bs) ->
        let bs', act = next cmd bs in
        Stdlib.print_endline
          (Printf.sprintf "%s -> {%s, %d} %s" name (show_pos bs'.position)
             bs'.error_count (show_action act)));
    [%expect
      {|
      NOP keeps state -> {Open, 99} NoAction
      Ok+Closed -> {Closed, 0} NoAction
      Ok+Open -> {Open, 5} NoAction
      Ok+HalfOpen closes -> {Closed, 0} CloseBreaker(5)
      Err+Open -> {Open, 5} NoAction
      Err+Closed below threshold -> {Closed, 1} NoAction
      Err+Closed at threshold opens -> {Open, 3} OpenBreaker(3.000000)
      Err+HalfOpen reopens -> {Open, 3} OpenBreaker(3.000000)
      |}]

  let update_breaker_state ~sw c conn mbox cmd bs =
    let rdy = rdy_per_connection c in
    let new_bs, action =
      next_breaker_state ~error_threshold:c.config.error_threshold
        ~backoff_multiplier:c.config.backoff_multiplier ~rdy_when_closed:rdy cmd
        bs
    in
    (match action with
    | NoBreakerAction -> ()
    | OpenBreaker duration ->
        Logs.warn (fun l ->
            l "Error threshold exceeded (%d of %d)" new_bs.error_count
              c.config.error_threshold);
        Logs.debug (fun l -> l "Breaker open, sending RDY 0");
        send ~clock:c.clock ~timeout:c.config.write_timeout ~conn (RDY 0);
        do_after_async ~sw ~clock:c.clock ~duration (fun () ->
            Eio.Stream.add mbox TrialBreaker)
    | CloseBreaker rdy ->
        Logs.debug (fun l ->
            l "%s Trial passed, sending RDY %d" c.log_prefix rdy);
        send ~clock:c.clock ~timeout:c.config.write_timeout ~conn (RDY rdy));
    new_bs

  let consume ~sw c conn mbox =
    let update_breaker_state = update_breaker_state ~sw c conn mbox in
    let send = send ~clock:c.clock ~timeout:c.config.write_timeout ~conn in
    let rec mbox_loop bs =
      let taken = Eio.Stream.take mbox in
      match taken with
      | RawFrame raw -> (
          match ServerMessage.of_raw_frame raw with
          | Ok server_message ->
              Fiber.fork ~sw (fun () ->
                  let resp =
                    handle_server_message server_message c.handler
                      c.config.max_attempts
                  in
                  match resp with
                  | Some c -> Eio.Stream.add mbox (Command c)
                  | None -> ());
              mbox_loop bs
          | Error s ->
              Logs.err (fun l ->
                  l "%s Error parsing response: %s" c.log_prefix s);
              mbox_loop bs)
      | Command cmd ->
          send cmd;
          let new_state = update_breaker_state cmd bs in
          mbox_loop new_state
      | TrialBreaker ->
          Logs.debug (fun l ->
              l "%s Breaker trial, sending RDY 1 (Error count: %i)" c.log_prefix
                bs.error_count);
          let bs = { bs with position = HalfOpen } in
          send (RDY 1);
          mbox_loop bs
      | ConnectionError s -> failwith s
      | RecalcRDY -> (
          (* Only recalc and send if breaker is closed  *)
          match bs.position with
          | Open -> mbox_loop bs
          | HalfOpen -> mbox_loop bs
          | Closed ->
              let rdy = rdy_per_connection c in
              Logs.debug (fun l ->
                  l "%s Sending recalculated RDY %d" c.log_prefix rdy);
              send (RDY rdy);
              mbox_loop bs)
    in
    send MAGIC;
    let ic = Config.to_identity_config c.config in
    identify ~clock:c.clock ~read_timeout:c.config.read_timeout
      ~write_timeout:c.config.write_timeout ~conn ic;
    subscribe ~clock:c.clock ~read_timeout:c.config.read_timeout
      ~write_timeout:c.config.write_timeout ~conn c.topic c.channel;
    (* Start cautiously by sending RDY 1 *)
    Logs.debug (fun l -> l "%s Sending initial RDY 1" c.log_prefix);
    send (RDY 1);
    (* Start background reader *)
    Fiber.fork ~sw (fun () ->
        read_loop ~clock:c.clock ~timeout:c.config.read_timeout conn mbox);
    let initial_state = { position = HalfOpen; error_count = 0 } in
    mbox_loop initial_state

  let connection_loop c address mbox =
    let rec loop error_count =
      Switch.run @@ fun sw ->
      let conn =
        catch_result (fun () ->
            connect ~sw ~net:c.net ~clock:c.clock ~rng:c.rng address
              c.config.dial_timeout)
      in
      match conn with
      | Ok conn ->
          Hash_set.add c.open_connections address;
          Logs.debug (fun l ->
              l "%s %d connections" c.log_prefix
                (Hash_set.length c.open_connections));
          (try consume ~sw c conn mbox
           with e ->
             Logs.err (fun l -> l "Consumer error: %s" (Exn.to_string e));
             Eio.Time.sleep c.clock (Seconds.value default_backoff));
          (* Error consuming. If we get here it means that something failed and we need to reconnect *)
          Hash_set.remove c.open_connections address;
          Logs.debug (fun l ->
              l "%s %d connections" c.log_prefix
                (Hash_set.length c.open_connections));
          let error_count = 1 in
          loop error_count
      | Error e ->
          (* Error connecting *)
          Logs.err (fun l ->
              l "%s Connecting to consumer '%s': %s" c.log_prefix
                (Address.to_string address)
                e);
          let error_count = error_count + 1 in
          let stop_connecting =
            match c.mode with
            | `Nsqd -> false
            | `Lookupd -> error_count > lookupd_error_threshold
          in
          if stop_connecting then
            (* Log and stop recursing *)
            Logs.err (fun l ->
                l "%s Exceeded reconnection threshold (%d), not reconnecting"
                  c.log_prefix error_count)
          else
            let duration =
              backoff_duration ~multiplier:default_backoff ~error_count
            in
            Logs.debug (fun l ->
                l "%s Sleeping for %f seconds" c.log_prefix
                  (Seconds.value duration));
            Eio.Time.sleep c.clock (Seconds.value duration);
            loop error_count
    in
    loop 0

  (** Start a fiber to update RDY count occasionally. The number of open
      connections can change as we add new consumers due to lookupd discovering
      producers or we have connection failures. Each time a new connection is
      opened or closed the consumer.nsqd_connections field is updated. We
      therefore need to occasionally update our RDY count as this may have
      changed so that it is spread evenly across connections. *)
  let start_ready_calculator ~sw c mbox =
    let jitter = Random.State.float c.rng (recalculate_rdy_interval /. 10.0) in
    let interval = recalculate_rdy_interval +. jitter in
    let rec loop () =
      Eio.Time.sleep c.clock interval;
      Logs.debug (fun l -> l "%s recalculating RDY" c.log_prefix);
      Eio.Stream.add mbox RecalcRDY;
      loop ()
    in
    Fiber.fork ~sw loop

  let start_nsqd_consumer ~sw c address =
    let mbox = Eio.Stream.create 16 in
    Switch.run (fun inner_sw ->
        start_ready_calculator ~sw:inner_sw c mbox;
        connection_loop c address mbox);
    ignore sw

  let start_polling_lookupd ~sw c =
    let http_client = Cohttp_eio.Client.make ~https:None c.net in
    let poll_interval =
      Float.(
        (1.0 + Random.State.float c.rng c.config.lookupd_poll_jitter)
        * Seconds.value c.config.lookupd_poll_interval)
    in
    let rec check_for_producers () =
      try
        Logs.debug (fun l ->
            l "Querying %d lookupd hosts" (List.length c.addresses));
        let results =
          Fiber.List.map
            (fun a ->
              Switch.run (fun sw ->
                  query_nsqlookupd ~http_client ~sw ~topic:c.topic a))
            c.addresses
        in
        let discovered_producers =
          List.filter_map ~f:Result.ok results
          |> List.map ~f:Lookup.producer_addresses
          |> List.join
          |> Hash_set.of_list (module Address)
        in
        let running = c.open_connections in
        let new_producers = Hash_set.diff discovered_producers running in
        Logs.debug (fun l ->
            l "Found %d new producers" (Hash_set.length new_producers));
        Hash_set.iter
          ~f:(fun a ->
            Fiber.fork ~sw (fun () ->
                Logs.debug (fun l ->
                    l "Starting consumer for: %s" (Address.to_string a));
                start_nsqd_consumer ~sw c a))
          new_producers;
        Eio.Time.sleep c.clock poll_interval;
        check_for_producers ()
      with e ->
        Logs.err (fun l -> l "Error polling lookupd: %s" (Exn.to_string e));
        Eio.Time.sleep c.clock (Seconds.value default_backoff);
        check_for_producers ()
    in
    check_for_producers ()

  let run c =
    Switch.run @@ fun sw ->
    match c.mode with
    | `Lookupd ->
        Logs.debug (fun l -> l "Starting lookupd poller");
        start_polling_lookupd ~sw c
    | `Nsqd ->
        Fiber.List.iter (fun a -> start_nsqd_consumer ~sw c a) c.addresses
end

module Producer = struct
  type net = [ `Generic ] Eio.Net.ty Eio.Resource.t
  type clock = float Eio.Time.clock_ty Eio.Resource.t
  type connection = { conn : conn; mutable last_write : float }

  type t = {
    net : net;
    clock : clock;
    address : Address.t;
    pool : connection Eio.Pool.t;
    rng : Random.State.t;
  }

  let default_pool_size = 5
  let default_dial_timeout = Seconds.of_float 15.0
  let default_write_timeout = Seconds.of_float 15.0
  let default_read_timeout = Seconds.of_float 15.0

  (** Throw away connections that are idle for this long Note that NSQ expects
      hearbeats to be answered every 30 seconds and if two are missed it closes
      the connection. *)
  let ttl_seconds = 50.0

  let create_pool ~sw ~net ~clock ~rng address size =
    let validate c =
      let now = Eio.Time.now clock in
      let diff = now -. c.last_write in
      Float.(diff < ttl_seconds)
    in
    let dispose c =
      Logs.warn (fun l -> l "Closing pooled producer connection");
      try Eio.Resource.close c.conn.flow with _ -> ()
    in
    Eio.Pool.create size ~validate ~dispose (fun () ->
        let conn = connect ~sw ~net ~clock ~rng address default_dial_timeout in
        send ~clock ~timeout:default_write_timeout ~conn MAGIC;
        let last_write = Eio.Time.now clock in
        { conn; last_write })

  let create ~sw ~net ~clock ?(pool_size = default_pool_size) address =
    if pool_size < 1 then Error "Pool size must be >= 1"
    else
      let net = (net :> net) in
      let clock = (clock :> clock) in
      let rng = Random.State.make_self_init () in
      Ok
        {
          net;
          clock;
          address;
          pool = create_pool ~sw ~net ~clock ~rng address pool_size;
          rng;
        }

  let publish_cmd t cmd =
    let with_conn c =
      let rec read_until_ok () =
        let frame =
          read_raw_frame ~clock:t.clock ~timeout:default_read_timeout c.conn
        in
        match ServerMessage.of_raw_frame frame with
        | Ok ResponseOk -> Ok ()
        | Ok Heartbeat ->
            send ~clock:t.clock ~timeout:default_write_timeout ~conn:c.conn NOP;
            c.last_write <- Eio.Time.now t.clock;
            read_until_ok ()
        | Ok _ -> Error "Expected OK or Heartbeat, got another message"
        | Error e -> Error (Printf.sprintf "Received error: %s" e)
      in
      send ~clock:t.clock ~timeout:default_write_timeout ~conn:c.conn cmd;
      c.last_write <- Eio.Time.now t.clock;
      read_until_ok ()
    in
    try Eio.Pool.use t.pool with_conn
    with e ->
      let message =
        Printf.sprintf "Publishing to `%s`: %s"
          (Address.to_string t.address)
          (Exn.to_string e)
      in
      Error message

  let publish t topic message =
    let cmd = PUB (topic, message) in
    publish_cmd t cmd

  let publish_multi t topic messages =
    let cmd = MPUB (topic, messages) in
    publish_cmd t cmd
end
